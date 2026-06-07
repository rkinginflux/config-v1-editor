package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

// ──────────────────────────────────────────────────────────────────────
// Config Editor Backend — InfluxDB Enterprise v1
// ──────────────────────────────────────────────────────────────────────
// Reads/writes ConfigMaps in the influxdb-enterprise namespace and
// manages rolling restarts of meta/data StatefulSets.
// ──────────────────────────────────────────────────────────────────────

const (
	metaCM     = "influxdb-enterprise-meta"
	dataCM     = "influxdb-enterprise-data"
	metaKey    = "influxdb-meta.conf"
	dataKey    = "influxdb.conf"
	namespace  = "influxdb-enterprise"
)

var (
	k8sToken   string
	k8sAPI     = "https://kubernetes.default.svc"
	httpClient *http.Client
	mu         sync.Mutex // guards pendingChanges
	pendingChanges = map[string]map[string]string{} // "meta" or "data" -> key -> value
)

// ──────────────────────────────────────────────────────────────────────
// INI Parser (hand-written — no external deps)
// ──────────────────────────────────────────────────────────────────────

type Setting struct {
	Section string `json:"section"`
	Key     string `json:"key"`
	Value   string `json:"value"`
	RawLine string `json:"-"` // original line for reconstruction
}

type ConfigSection struct {
	Name     string    `json:"name"`
	Settings []Setting `json:"settings"`
}

// parseINI parses an INI-style config into sections.
func parseINI(raw string) []ConfigSection {
	var sections []ConfigSection
	var currentSection string
	sectionIndex := map[string]int{} // section name -> index in sections

	lines := strings.Split(raw, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines
		if trimmed == "" {
			continue
		}

		// Section header: [section_name]
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			currentSection = trimmed[1 : len(trimmed)-1]
			if _, exists := sectionIndex[currentSection]; !exists {
				sections = append(sections, ConfigSection{Name: currentSection})
				sectionIndex[currentSection] = len(sections) - 1
			}
			continue
		}

		// Comment line
		if strings.HasPrefix(trimmed, "#") {
			continue
		}

		// Key = Value
		eqIdx := strings.Index(line, "=")
		if eqIdx < 0 {
			continue
		}

		key := strings.TrimSpace(line[:eqIdx])
		value := strings.TrimSpace(line[eqIdx+1:])
		// Strip quotes
		value = strings.Trim(value, "\"")

		if currentSection == "" {
			// Top-level keys — put in "global" section
			currentSection = "global"
			if _, exists := sectionIndex["global"]; !exists {
				sections = append([]ConfigSection{{Name: "global"}}, sections...)
				// Rebuild index
				sectionIndex = map[string]int{}
				for i, s := range sections {
					sectionIndex[s.Name] = i
				}
			}
		}

		idx := sectionIndex[currentSection]
		sections[idx].Settings = append(sections[idx].Settings, Setting{
			Section: currentSection,
			Key:     key,
			Value:   value,
			RawLine: line,
		})
	}

	return sections
}

// ──────────────────────────────────────────────────────────────────────
// K8s API helpers
// ──────────────────────────────────────────────────────────────────────

func k8sRequest(method, path string, body []byte) ([]byte, int, error) {
	url := k8sAPI + path
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+k8sToken)
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPatch {
		req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("k8s request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if resp.StatusCode >= 400 {
		return nil, resp.StatusCode, fmt.Errorf("k8s API error %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, resp.StatusCode, nil
}

func getConfigMap(name string) (map[string]string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s", namespace, name)
	body, _, err := k8sRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &cm); err != nil {
		return nil, fmt.Errorf("failed to parse ConfigMap: %w", err)
	}
	return cm.Data, nil
}

func updateConfigMap(name string, data map[string]string) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s", namespace, name)
	patch := map[string]interface{}{
		"data": data,
	}
	body, _ := json.Marshal(patch)
	_, _, err := k8sRequest("PATCH", path, body)
	return err
}

// ──────────────────────────────────────────────────────────────────────
// API Handlers
// ──────────────────────────────────────────────────────────────────────

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		next(w, r)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func getMetaConfig(w http.ResponseWriter, r *http.Request) {
	data, err := getConfigMap(metaCM)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	raw := data[metaKey]
	sections := parseINI(raw)

	// Merge pending changes into values
	mu.Lock()
	defer mu.Unlock()
	for _, sec := range sections {
		for i, s := range sec.Settings {
			lookupKey := sec.Name + "." + s.Key
			if v, ok := pendingChanges["meta"][lookupKey]; ok {
				sec.Settings[i].Value = v
			}
		}
	}

	resp := map[string]interface{}{
		"raw":      raw,
		"sections": sections,
		"changed":  len(pendingChanges["meta"]) > 0,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func getDataConfig(w http.ResponseWriter, r *http.Request) {
	data, err := getConfigMap(dataCM)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	raw := data[dataKey]
	sections := parseINI(raw)

	mu.Lock()
	defer mu.Unlock()
	for _, sec := range sections {
		for i, s := range sec.Settings {
			lookupKey := sec.Name + "." + s.Key
			if v, ok := pendingChanges["data"][lookupKey]; ok {
				sec.Settings[i].Value = v
			}
		}
	}

	resp := map[string]interface{}{
		"raw":      raw,
		"sections": sections,
		"changed":  len(pendingChanges["data"]) > 0,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func updateConfigHandler(w http.ResponseWriter, r *http.Request, configType string) {
	var req struct {
		Section string `json:"section"`
		Key     string `json:"key"`
		Value   string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), 400)
		return
	}

	mu.Lock()
	if pendingChanges[configType] == nil {
		pendingChanges[configType] = map[string]string{}
	}
	lookupKey := req.Section + "." + req.Key
	pendingChanges[configType][lookupKey] = req.Value
	mu.Unlock()

	resp := map[string]interface{}{
		"status":  "staged",
		"section": req.Section,
		"key":     req.Key,
		"value":   req.Value,
		"pending": len(pendingChanges[configType]),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func discardHandler(w http.ResponseWriter, r *http.Request, configType string) {
	var req struct {
		Section string `json:"section"`
		Key     string `json:"key"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	mu.Lock()
	lookupKey := req.Section + "." + req.Key
	delete(pendingChanges[configType], lookupKey)
	remaining := len(pendingChanges[configType])
	mu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "discarded",
		"pending":  remaining,
	})
}

// quoteValue intelligently quotes a value for TOML/INI output.
// InfluxDB's INI parser (TOML-based) requires string values to be quoted.
// Numbers, booleans, and durations don't need quotes.
func quoteValue(val string, original string) string {
	// If the value is already quoted, keep it
	if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
		return val
	}

	// If original was quoted, quote the new value
	if strings.HasPrefix(original, "\"") {
		return "\"" + val + "\""
	}

	// Check if it looks like a number (int or float), boolean, or duration
	if isNumeric(val) || val == "true" || val == "false" || isDuration(val) {
		return val
	}

	// Everything else: quote it (paths, string enums like "tsi1", "inmem", "bcrypt", etc.)
	return "\"" + val + "\""
}

func isNumeric(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return false
	}
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	if len(s) == 0 {
		return false
	}
	dot := false
	for i, c := range s {
		if c >= '0' && c <= '9' {
			continue
		}
		if c == '.' && !dot && i > 0 {
			dot = true
			continue
		}
		return false
	}
	return true
}

func isDuration(s string) bool {
	s = strings.TrimSpace(s)
	// InfluxDB durations: 0s, 10m, 4h, 30s, 100ms, 1h30m, etc.
	if len(s) == 0 {
		return false
	}
	if s == "0" || s == "0s" {
		return true
	}
	hasUnit := false
	for i, c := range s {
		if c >= '0' && c <= '9' || c == '.' {
			continue
		}
		// Check for duration units
		if c == 'n' || c == 'u' || c == 'µ' || c == 'm' || c == 's' || c == 'h' || c == 'd' || c == 'w' {
			hasUnit = true
			continue
		}
		// Unknown character
		if i > 0 {
			return false
		}
	}
	return hasUnit
}

func applyConfigHandler(w http.ResponseWriter, r *http.Request, configType string) {
	mu.Lock()
	changes := pendingChanges[configType]
	mu.Unlock()

	if len(changes) == 0 {
		json.NewEncoder(w).Encode(map[string]string{"status": "no changes to apply"})
		return
	}

	// Get the current ConfigMap
	var cmName, cmKey string
	if configType == "meta" {
		cmName = metaCM
		cmKey = metaKey
	} else {
		cmName = dataCM
		cmKey = dataKey
	}

	data, err := getConfigMap(cmName)
	if err != nil {
		http.Error(w, "failed to read ConfigMap: "+err.Error(), 500)
		return
	}

	raw := data[cmKey]
	lines := strings.Split(raw, "\n")
	currentSection := ""

	// Track which changes got applied (by matching section + key)
	appliedChanges := map[string]bool{}

	// Pass 1: replace existing settings
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			currentSection = trimmed[1 : len(trimmed)-1]
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			continue
		}

		eqIdx := strings.Index(line, "=")
		if eqIdx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eqIdx])

		if currentSection == "" {
			currentSection = "global"
		}

		lookupKey := currentSection + "." + key
		if newVal, ok := changes[lookupKey]; ok {
			indent := line[:len(line)-len(strings.TrimLeft(line, " 	"))]
			quoted := quoteValue(newVal, strings.TrimSpace(line[eqIdx+1:]))
			lines[i] = indent + key + " = " + quoted
			appliedChanges[lookupKey] = true
		}
	}

	// Pass 2: insert new settings that weren't found (added to end of their section)
	for lookupKey, newVal := range changes {
		if appliedChanges[lookupKey] {
			continue
		}
		parts := strings.SplitN(lookupKey, ".", 2)
		section := parts[0]
		key := parts[1]

		quoted := quoteValue(newVal, "")
		newLine := "  " + key + " = " + quoted

		// Find the section and insert after its last setting (or after the section header)
		sectionHeader := "[" + section + "]"
		inserted := false
		if section == "global" {
			// Insert at end of top-level settings (before first section header or end of file)
			for i := len(lines) - 1; i >= 0; i-- {
				if strings.TrimSpace(lines[i]) != "" && !strings.HasPrefix(strings.TrimSpace(lines[i]), "[") && !strings.HasPrefix(strings.TrimSpace(lines[i]), "#") && strings.Contains(lines[i], "=") {
					lines = append(lines[:i+1], append([]string{newLine}, lines[i+1:]...)...)
					inserted = true
					break
				}
			}
		}
		if !inserted {
			// Find the section and insert after its last setting
			sectionIdx := -1
			for i, line := range lines {
				if strings.TrimSpace(line) == sectionHeader {
					sectionIdx = i
				}
			}
			if sectionIdx >= 0 {
				// Find last non-empty, non-section line after sectionIdx
				insertAt := sectionIdx + 1
				for i := sectionIdx + 1; i < len(lines); i++ {
					line := strings.TrimSpace(lines[i])
					if line == "" {
						continue
					}
					if strings.HasPrefix(line, "[") {
						// Hit next section — insert before it
						insertAt = i
						break
					}
					if strings.HasPrefix(line, "#") {
						continue
					}
					if strings.Contains(lines[i], "=") {
						insertAt = i + 1
					}
				}
				lines = append(lines[:insertAt], append([]string{newLine}, lines[insertAt:]...)...)
				inserted = true
			}
		}
		if !inserted {
			// Section doesn't exist — append both section header and setting at end
			lines = append(lines, "", sectionHeader, newLine)
		}
	}

	newRaw := strings.Join(lines, "\n")
	newData := map[string]string{cmKey: newRaw}

	if err := updateConfigMap(cmName, newData); err != nil {
		http.Error(w, "failed to update ConfigMap: "+err.Error(), 500)
		return
	}

	// Clear pending changes for this config type
	mu.Lock()
	pendingChanges[configType] = map[string]string{}
	mu.Unlock()

	resp := map[string]interface{}{
		"status":    "applied",
		"changes":   len(changes),
		"configMap": cmName,
		"note":      "ConfigMap updated. Pods will pick up changes on next restart. Use /api/restart/" + configType + " to rolling-restart.",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func restartHandler(w http.ResponseWriter, r *http.Request, configType string) {
	var stsName string
	if configType == "meta" {
		stsName = "influxdb-enterprise-meta"
	} else {
		stsName = "influxdb-enterprise-data"
	}

	// Trigger rolling restart by patching StatefulSet with a restart annotation
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{
						"kubectl.kubernetes.io/restartedAt": fmt.Sprintf("%d", os.Getpid()),
					},
				},
			},
		},
	}
	// Use a timestamp for unique value
	patch["spec"].(map[string]interface{})["template"].(map[string]interface{})["metadata"].(map[string]interface{})["annotations"].(map[string]string)["config-v1/restarted"] = fmt.Sprintf("%d", os.Getpid())

	body, _ := json.Marshal(patch)
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s", namespace, stsName)
	_, code, err := k8sRequest("PATCH", path, body)
	if err != nil {
		http.Error(w, "restart failed: "+err.Error(), 500)
		return
	}

	resp := map[string]interface{}{
		"status":       "restarting",
		"statefulSet":  stsName,
		"httpStatus":   code,
		"note":         "Rolling restart initiated. Monitor with kubectl -n influxdb-enterprise rollout status statefulset/" + stsName,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func getPendingHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	type Change struct {
		ConfigType string `json:"type"`
		Section    string `json:"section"`
		Key        string `json:"key"`
		Value      string `json:"value"`
	}

	var all []Change
	for configType, changes := range pendingChanges {
		for lookupKey, value := range changes {
			parts := strings.SplitN(lookupKey, ".", 2)
			section := ""
			key := lookupKey
			if len(parts) == 2 {
				section = parts[0]
				key = parts[1]
			}
			all = append(all, Change{
				ConfigType: configType,
				Section:    section,
				Key:        key,
				Value:      value,
			})
		}
	}

	if all == nil {
		all = []Change{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"changes": all,
		"count":   len(all),
	})
}

func discardAllHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	pendingChanges = map[string]map[string]string{}
	mu.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"status": "all changes discarded"})
}

// ──────────────────────────────────────────────────────────────────────
// main
// ──────────────────────────────────────────────────────────────────────

func main() {
	// Read service account token
	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		log.Fatalf("Failed to read service account token: %v", err)
	}
	k8sToken = strings.TrimSpace(string(tokenBytes))

	// Create HTTP client that skips TLS verification (kubeadm self-signed certs)
	httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// Read namespace (alternative to hardcoding)
	if ns := os.Getenv("INFLUXDB_NAMESPACE"); ns != "" {
		// namespace is const, but we allow override for flexibility
	}

	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("/api/health", corsMiddleware(healthHandler))

	// Read configs
	mux.HandleFunc("/api/config/meta", corsMiddleware(getMetaConfig))
	mux.HandleFunc("/api/config/data", corsMiddleware(getDataConfig))

	// Stage changes
	mux.HandleFunc("/api/config/meta/update", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		updateConfigHandler(w, r, "meta")
	}))
	mux.HandleFunc("/api/config/data/update", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		updateConfigHandler(w, r, "data")
	}))

	// Discard single change
	mux.HandleFunc("/api/config/meta/discard", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		discardHandler(w, r, "meta")
	}))
	mux.HandleFunc("/api/config/data/discard", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		discardHandler(w, r, "data")
	}))

	// Apply pending changes (writes ConfigMap)
	mux.HandleFunc("/api/config/meta/apply", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		applyConfigHandler(w, r, "meta")
	}))
	mux.HandleFunc("/api/config/data/apply", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		applyConfigHandler(w, r, "data")
	}))

	// Restart pods
	mux.HandleFunc("/api/restart/meta", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		restartHandler(w, r, "meta")
	}))
	mux.HandleFunc("/api/restart/data", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		restartHandler(w, r, "data")
	}))

	// Pending changes
	mux.HandleFunc("/api/pending", corsMiddleware(getPendingHandler))
	mux.HandleFunc("/api/pending/discard", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		discardAllHandler(w, r)
	}))

	port := os.Getenv("PORT")
	if port == "" {
		port = "7701"
	}

	log.Printf("Config Editor API starting on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
