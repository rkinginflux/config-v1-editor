package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultNamespace = "influxdb-enterprise"
	defaultMetaCM    = "influxdb-enterprise-meta"
	defaultDataCM    = "influxdb-enterprise-data"
	defaultMetaKey   = "influxdb-meta.conf"
	defaultDataKey   = "influxdb.conf"
	defaultMetaSTS   = "influxdb-enterprise-meta"
	defaultDataSTS   = "influxdb-enterprise-data"
	defaultLicenseSecret = "influxdb-license"
)

type Setting struct {
	Section string `json:"section"`
	Key     string `json:"key"`
	Value   string `json:"value"`
	RawLine string `json:"-"`
}

type ConfigSection struct {
	Name     string    `json:"name"`
	Settings []Setting `json:"settings"`
}

type SchemaSetting struct {
	Scope       string `json:"scope"`
	Section     string `json:"section"`
	Key         string `json:"key"`
	Type        string `json:"type"`
	Default     string `json:"default"`
	Description string `json:"description"`
	Env         string `json:"env,omitempty"`
	Sensitive   bool   `json:"sensitive,omitempty"`
	HighImpact  bool   `json:"highImpact,omitempty"`
}

type SchemaDoc struct {
	Version       string          `json:"version"`
	GeneratedFrom string          `json:"generatedFrom"`
	Settings      []SchemaSetting `json:"settings"`
}

type PreflightFinding struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Section  string `json:"section"`
	Key      string `json:"key"`
	Message  string `json:"message"`
}

type AppTargets struct {
	Namespace       string `json:"namespace"`
	MetaConfigMap   string `json:"metaConfigMap"`
	DataConfigMap   string `json:"dataConfigMap"`
	MetaConfigKey   string `json:"metaConfigKey"`
	DataConfigKey   string `json:"dataConfigKey"`
	MetaStatefulSet string `json:"metaStatefulSet"`
	DataStatefulSet string `json:"dataStatefulSet"`
}

type K8sConnection struct {
	Mode          string
	APIServer     string
	Token         string
	CAData        []byte
	InsecureTLS   bool
	ClientCert    *tls.Certificate
	ContextName   string
	Targets       AppTargets
	Connected     bool
	LastTestError string
}

type ConnectionRequest struct {
	Mode            string `json:"mode"`
	Kubeconfig      string `json:"kubeconfig"`
	Context         string `json:"context"`
	Namespace       string `json:"namespace"`
	MetaConfigMap   string `json:"metaConfigMap"`
	DataConfigMap   string `json:"dataConfigMap"`
	MetaConfigKey   string `json:"metaConfigKey"`
	DataConfigKey   string `json:"dataConfigKey"`
	MetaStatefulSet string `json:"metaStatefulSet"`
	DataStatefulSet string `json:"dataStatefulSet"`
	InsecureTLS     *bool  `json:"insecureTLS"`
}

type kubeconfigFile struct {
	CurrentContext string `yaml:"current-context"`
	Clusters       []struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server                   string `yaml:"server"`
			CertificateAuthorityData string `yaml:"certificate-authority-data"`
			CertificateAuthority     string `yaml:"certificate-authority"`
			InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Users []struct {
		Name string `yaml:"name"`
		User struct {
			Token                 string `yaml:"token"`
			ClientCertificateData string `yaml:"client-certificate-data"`
			ClientKeyData         string `yaml:"client-key-data"`
			ClientCertificate     string `yaml:"client-certificate"`
			ClientKey             string `yaml:"client-key"`
		} `yaml:"user"`
	} `yaml:"users"`
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster   string `yaml:"cluster"`
			User      string `yaml:"user"`
			Namespace string `yaml:"namespace"`
		} `yaml:"context"`
	} `yaml:"contexts"`
}

var (
	connMu          sync.RWMutex
	currentConn     *K8sConnection
	pendingMu       sync.Mutex
	pendingChanges  = map[string]map[string]string{}
	inClusterToken  string
	inClusterCAData []byte
	schemaMu        sync.RWMutex
	settingsSchema  SchemaDoc
)

var influxDurationTokenRE = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)(ns|us|µs|u|ms|s|m|h|d|w)`)

func withDefaults(req ConnectionRequest) AppTargets {
	t := AppTargets{
		Namespace:       firstNonEmpty(req.Namespace, defaultNamespace),
		MetaConfigMap:   firstNonEmpty(req.MetaConfigMap, defaultMetaCM),
		DataConfigMap:   firstNonEmpty(req.DataConfigMap, defaultDataCM),
		MetaConfigKey:   firstNonEmpty(req.MetaConfigKey, defaultMetaKey),
		DataConfigKey:   firstNonEmpty(req.DataConfigKey, defaultDataKey),
		MetaStatefulSet: firstNonEmpty(req.MetaStatefulSet, defaultMetaSTS),
		DataStatefulSet: firstNonEmpty(req.DataStatefulSet, defaultDataSTS),
	}
	return t
}

func firstNonEmpty(v, fallback string) string {
	if strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func parseINI(raw string) []ConfigSection {
	var sections []ConfigSection
	var currentSection string
	sectionIndex := map[string]int{}

	lines := strings.Split(raw, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			currentSection = trimmed[1 : len(trimmed)-1]
			if _, exists := sectionIndex[currentSection]; !exists {
				sections = append(sections, ConfigSection{Name: currentSection})
				sectionIndex[currentSection] = len(sections) - 1
			}
			continue
		}

		eqIdx := strings.Index(line, "=")
		if eqIdx < 0 {
			continue
		}

		if currentSection == "" {
			currentSection = "global"
			if _, exists := sectionIndex["global"]; !exists {
				sections = append([]ConfigSection{{Name: "global"}}, sections...)
				sectionIndex = map[string]int{}
				for i, s := range sections {
					sectionIndex[s.Name] = i
				}
			}
		}

		key := strings.TrimSpace(line[:eqIdx])
		value := strings.Trim(strings.TrimSpace(line[eqIdx+1:]), "\"")
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

func loadSettingsSchema() error {
	paths := []string{"settings_schema.json", "backend/settings_schema.json", "/app/settings_schema.json"}
	var lastErr error
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			lastErr = err
			continue
		}
		var doc SchemaDoc
		if err := json.Unmarshal(b, &doc); err != nil {
			lastErr = err
			continue
		}
		schemaMu.Lock()
		settingsSchema = doc
		schemaMu.Unlock()
		log.Printf("loaded settings schema from %s with %d settings", p, len(doc.Settings))
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("settings schema not found")
}

func getSchemaSetting(configType, section, key string) (SchemaSetting, bool) {
	schemaMu.RLock()
	defer schemaMu.RUnlock()
	for _, set := range settingsSchema.Settings {
		if set.Section != section || set.Key != key {
			continue
		}
		if set.Scope == "both" || set.Scope == configType {
			return set, true
		}
	}
	return SchemaSetting{}, false
}

func listSchemaSettings(configType string) []SchemaSetting {
	schemaMu.RLock()
	defer schemaMu.RUnlock()
	out := make([]SchemaSetting, 0, len(settingsSchema.Settings))
	for _, set := range settingsSchema.Settings {
		if set.Scope == "both" || set.Scope == configType {
			out = append(out, set)
		}
	}
	return out
}

func normalizeDurationForParse(value string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return v
	}
	return strings.TrimSpace(influxDurationTokenRE.ReplaceAllString(v, `${1}${2} `))
}

func validateValueByType(vType, value string) error {
	v := strings.TrimSpace(value)
	switch vType {
	case "bool":
		if v != "true" && v != "false" {
			return fmt.Errorf("must be true or false")
		}
	case "int", "bytes":
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return fmt.Errorf("must be an integer")
		}
	case "duration":
		n := normalizeDurationForParse(v)
		if _, err := time.ParseDuration(n); err != nil {
			return fmt.Errorf("must be a valid duration (examples: 500ms, 10s, 5m, 1h30m, 7d)")
		}
	case "string":
		// any value allowed
	default:
		// unknown type: skip strict validation
	}
	return nil
}

func validateSetting(configType, section, key, value string) (SchemaSetting, error) {
	set, found := getSchemaSetting(configType, section, key)
	if !found {
		return SchemaSetting{}, fmt.Errorf("setting [%s].%s is not in schema for %s nodes", section, key, configType)
	}
	if err := validateValueByType(set.Type, value); err != nil {
		return set, fmt.Errorf("invalid value for [%s].%s: %w", section, key, err)
	}
	return set, nil
}

func buildHighImpactFinding(set SchemaSetting, value string) PreflightFinding {
	v := strings.TrimSpace(value)
	f := PreflightFinding{
		Severity: "warn",
		Code:     "HIGH_IMPACT_CHANGE",
		Section:  set.Section,
		Key:      set.Key,
		Message:  fmt.Sprintf("[%s].%s is high-impact: review restart plan and rollback path before apply", set.Section, set.Key),
	}
	if set.Key == "license-key" || set.Key == "license-path" {
		if v != "" {
			f.Code = "LICENSE_CHANGE"
			f.Severity = "critical"
			f.Message = fmt.Sprintf("[%s].%s changes license behavior: verify key/path exclusivity and coordinated node rollout", set.Section, set.Key)
		}
		return f
	}
	if set.Key == "auth-enabled" {
		f.Code = "AUTH_CHANGE"
		f.Severity = "critical"
		f.Message = fmt.Sprintf("[%s].%s affects authentication: ensure admin/user credentials and client auth settings are ready", set.Section, set.Key)
		return f
	}
	if strings.Contains(set.Key, "https") || strings.Contains(set.Key, "tls") {
		f.Code = "TLS_CHANGE"
		f.Severity = "critical"
		f.Message = fmt.Sprintf("[%s].%s affects TLS/certs: bad values can break inter-node or client connectivity", set.Section, set.Key)
		return f
	}
	if strings.Contains(set.Key, "shared-secret") {
		f.Code = "SECRET_CHANGE"
		f.Severity = "critical"
		f.Message = fmt.Sprintf("[%s].%s changes shared secrets: all participating nodes/services must match", set.Section, set.Key)
		return f
	}
	return f
}

func getConnection() (*K8sConnection, error) {
	connMu.RLock()
	defer connMu.RUnlock()
	if currentConn == nil {
		return nil, errors.New("no Kubernetes connection configured")
	}
	copyConn := *currentConn
	return &copyConn, nil
}

func saveConnection(conn *K8sConnection) {
	connMu.Lock()
	defer connMu.Unlock()
	currentConn = conn
}

func httpClientFromConn(conn *K8sConnection) *http.Client {
	tlsConfig := &tls.Config{InsecureSkipVerify: conn.InsecureTLS}

	if len(conn.CAData) > 0 {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(conn.CAData) {
			tlsConfig.RootCAs = pool
		}
	}
	if conn.ClientCert != nil {
		tlsConfig.Certificates = []tls.Certificate{*conn.ClientCert}
	}

	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}, Timeout: 20 * time.Second}
}

func k8sRequest(conn *K8sConnection, method, path string, body []byte) ([]byte, int, error) {
	url := strings.TrimRight(conn.APIServer, "/") + path
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	if strings.TrimSpace(conn.Token) != "" {
		req.Header.Set("Authorization", "Bearer "+conn.Token)
	}
	if method == http.MethodPatch {
		req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClientFromConn(conn).Do(req)
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

func getConfigMap(conn *K8sConnection, name string) (map[string]string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s", conn.Targets.Namespace, name)
	body, _, err := k8sRequest(conn, http.MethodGet, path, nil)
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

func updateConfigMap(conn *K8sConnection, name string, data map[string]string) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s", conn.Targets.Namespace, name)
	patchPayload := map[string]interface{}{"data": data}
	body, _ := json.Marshal(patchPayload)
	_, _, err := k8sRequest(conn, http.MethodPatch, path, body)
	return err
}

func buildInClusterConnection(req ConnectionRequest) (*K8sConnection, error) {
	if strings.TrimSpace(inClusterToken) == "" {
		return nil, errors.New("in-cluster service account token not available")
	}
	targets := withDefaults(req)
	conn := &K8sConnection{
		Mode:        "in-cluster",
		APIServer:   "https://kubernetes.default.svc",
		Token:       inClusterToken,
		CAData:      inClusterCAData,
		InsecureTLS: req.InsecureTLS != nil && *req.InsecureTLS,
		Targets:     targets,
		Connected:   false,
	}
	return conn, nil
}

func decodeB64(name, value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	return decoded, nil
}

func buildKubeconfigConnection(req ConnectionRequest) (*K8sConnection, []string, string, error) {
	if strings.TrimSpace(req.Kubeconfig) == "" {
		return nil, nil, "", errors.New("kubeconfig is required")
	}
	var kc kubeconfigFile
	if err := yaml.Unmarshal([]byte(req.Kubeconfig), &kc); err != nil {
		return nil, nil, "", fmt.Errorf("failed to parse kubeconfig YAML: %w", err)
	}
	if len(kc.Contexts) == 0 {
		return nil, nil, "", errors.New("kubeconfig has no contexts")
	}
	contexts := make([]string, 0, len(kc.Contexts))
	for _, c := range kc.Contexts {
		contexts = append(contexts, c.Name)
	}
	sort.Strings(contexts)

	selectedContext := strings.TrimSpace(req.Context)
	if selectedContext == "" {
		selectedContext = strings.TrimSpace(kc.CurrentContext)
	}
	if selectedContext == "" {
		selectedContext = kc.Contexts[0].Name
	}

	contextMap := map[string]struct {
		Cluster   string
		User      string
		Namespace string
	}{}
	for _, c := range kc.Contexts {
		contextMap[c.Name] = struct {
			Cluster   string
			User      string
			Namespace string
		}{Cluster: c.Context.Cluster, User: c.Context.User, Namespace: c.Context.Namespace}
	}
	selected, ok := contextMap[selectedContext]
	if !ok {
		return nil, contexts, selectedContext, fmt.Errorf("context %q not found", selectedContext)
	}

	clusterMap := map[string]struct {
		Server                string
		CAData                string
		CAFile                string
		InsecureSkipTLSVerify bool
	}{}
	for _, c := range kc.Clusters {
		clusterMap[c.Name] = struct {
			Server                string
			CAData                string
			CAFile                string
			InsecureSkipTLSVerify bool
		}{
			Server:                c.Cluster.Server,
			CAData:                c.Cluster.CertificateAuthorityData,
			CAFile:                c.Cluster.CertificateAuthority,
			InsecureSkipTLSVerify: c.Cluster.InsecureSkipTLSVerify,
		}
	}
	cluster, ok := clusterMap[selected.Cluster]
	if !ok {
		return nil, contexts, selectedContext, fmt.Errorf("cluster %q not found for context %q", selected.Cluster, selectedContext)
	}

	userMap := map[string]struct {
		Token          string
		ClientCertData string
		ClientKeyData  string
		ClientCertFile string
		ClientKeyFile  string
	}{}
	for _, u := range kc.Users {
		userMap[u.Name] = struct {
			Token          string
			ClientCertData string
			ClientKeyData  string
			ClientCertFile string
			ClientKeyFile  string
		}{
			Token:          u.User.Token,
			ClientCertData: u.User.ClientCertificateData,
			ClientKeyData:  u.User.ClientKeyData,
			ClientCertFile: u.User.ClientCertificate,
			ClientKeyFile:  u.User.ClientKey,
		}
	}
	user, ok := userMap[selected.User]
	if !ok {
		return nil, contexts, selectedContext, fmt.Errorf("user %q not found for context %q", selected.User, selectedContext)
	}

	if strings.TrimSpace(cluster.Server) == "" {
		return nil, contexts, selectedContext, errors.New("selected kubeconfig cluster has empty server URL")
	}

	targets := withDefaults(req)
	if strings.TrimSpace(req.Namespace) == "" {
		targets.Namespace = firstNonEmpty(selected.Namespace, defaultNamespace)
	}

	conn := &K8sConnection{
		Mode:        "kubeconfig",
		APIServer:   strings.TrimSpace(cluster.Server),
		Token:       strings.TrimSpace(user.Token),
		ContextName: selectedContext,
		Targets:     targets,
		Connected:   false,
		InsecureTLS: cluster.InsecureSkipTLSVerify,
	}

	if req.InsecureTLS != nil {
		conn.InsecureTLS = *req.InsecureTLS
	}

	if strings.TrimSpace(cluster.CAData) != "" {
		ca, err := decodeB64("certificate-authority-data", cluster.CAData)
		if err != nil {
			return nil, contexts, selectedContext, err
		}
		conn.CAData = ca
	} else if strings.TrimSpace(cluster.CAFile) != "" {
		return nil, contexts, selectedContext, errors.New("kubeconfig uses certificate-authority file path; paste kubeconfig with inline certificate-authority-data")
	}

	if strings.TrimSpace(user.ClientCertData) != "" || strings.TrimSpace(user.ClientKeyData) != "" {
		if strings.TrimSpace(user.ClientCertData) == "" || strings.TrimSpace(user.ClientKeyData) == "" {
			return nil, contexts, selectedContext, errors.New("kubeconfig requires both client-certificate-data and client-key-data")
		}
		certPEM, err := decodeB64("client-certificate-data", user.ClientCertData)
		if err != nil {
			return nil, contexts, selectedContext, err
		}
		keyPEM, err := decodeB64("client-key-data", user.ClientKeyData)
		if err != nil {
			return nil, contexts, selectedContext, err
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, contexts, selectedContext, fmt.Errorf("invalid client cert/key pair: %w", err)
		}
		conn.ClientCert = &cert
	} else if strings.TrimSpace(user.ClientCertFile) != "" || strings.TrimSpace(user.ClientKeyFile) != "" {
		return nil, contexts, selectedContext, errors.New("kubeconfig uses client cert/key file paths; paste kubeconfig with inline client-certificate-data and client-key-data")
	}

	if strings.TrimSpace(conn.Token) == "" && conn.ClientCert == nil {
		return nil, contexts, selectedContext, errors.New("selected kubeconfig user has neither token nor client certificate credentials")
	}

	return conn, contexts, selectedContext, nil
}

func parseConnectionRequest(req ConnectionRequest) (*K8sConnection, []string, string, error) {
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = "in-cluster"
	}
	switch mode {
	case "in-cluster":
		conn, err := buildInClusterConnection(req)
		return conn, nil, "", err
	case "kubeconfig":
		return buildKubeconfigConnection(req)
	default:
		return nil, nil, "", fmt.Errorf("unsupported mode %q", mode)
	}
}

func testConnection(conn *K8sConnection) (map[string]interface{}, error) {
	versionBody, _, err := k8sRequest(conn, http.MethodGet, "/version", nil)
	if err != nil {
		return nil, err
	}
	var version map[string]interface{}
	_ = json.Unmarshal(versionBody, &version)

	metaPath := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s", conn.Targets.Namespace, conn.Targets.MetaConfigMap)
	dataPath := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s", conn.Targets.Namespace, conn.Targets.DataConfigMap)
	metaSTSPath := fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s", conn.Targets.Namespace, conn.Targets.MetaStatefulSet)
	dataSTSPath := fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s", conn.Targets.Namespace, conn.Targets.DataStatefulSet)

	checks := []struct {
		Name string
		Path string
	}{
		{Name: "metaConfigMap", Path: metaPath},
		{Name: "dataConfigMap", Path: dataPath},
		{Name: "metaStatefulSet", Path: metaSTSPath},
		{Name: "dataStatefulSet", Path: dataSTSPath},
	}

	status := map[string]bool{}
	for _, c := range checks {
		_, _, err := k8sRequest(conn, http.MethodGet, c.Path, nil)
		status[c.Name] = err == nil
	}

	allCritical := status["metaConfigMap"] && status["dataConfigMap"] && status["metaStatefulSet"] && status["dataStatefulSet"]

	return map[string]interface{}{
		"serverVersion": version,
		"checks":        status,
		"ready":         allCritical,
	}, nil
}

func quoteValue(val string, original string) string {
	if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
		return val
	}
	if strings.HasPrefix(strings.TrimSpace(original), "\"") {
		return "\"" + val + "\""
	}
	if isNumeric(val) || val == "true" || val == "false" || isDuration(val) {
		return val
	}
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
	if len(s) == 0 {
		return false
	}
	if s == "0" || s == "0s" {
		return true
	}
	hasUnit := false
	for _, c := range s {
		if (c >= '0' && c <= '9') || c == '.' {
			continue
		}
		if c == 'n' || c == 'u' || c == 'µ' || c == 'm' || c == 's' || c == 'h' || c == 'd' || c == 'w' {
			hasUnit = true
			continue
		}
		return false
	}
	return hasUnit
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(200)
			return
		}
		next(w, r)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func connectionStatusHandler(w http.ResponseWriter, r *http.Request) {
	connMu.RLock()
	defer connMu.RUnlock()
	if currentConn == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"configured": false})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"configured":  true,
		"mode":        currentConn.Mode,
		"apiServer":   currentConn.APIServer,
		"context":     currentConn.ContextName,
		"targets":     currentConn.Targets,
		"connected":   currentConn.Connected,
		"lastError":   currentConn.LastTestError,
		"insecureTLS": currentConn.InsecureTLS,
	})
}

func connectionContextsHandler(w http.ResponseWriter, r *http.Request) {
	var req ConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), 400)
		return
	}
	_, contexts, selected, err := buildKubeconfigConnection(req)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"contexts": contexts,
		"selected": selected,
	})
}

func connectionTestHandler(w http.ResponseWriter, r *http.Request) {
	var req ConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), 400)
		return
	}
	conn, contexts, selected, err := parseConnectionRequest(req)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	result, err := testConnection(conn)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":       true,
		"result":   result,
		"contexts": contexts,
		"selected": selected,
		"targets":  conn.Targets,
	})
}

func connectionConfigHandler(w http.ResponseWriter, r *http.Request) {
	var req ConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), 400)
		return
	}
	conn, contexts, selected, err := parseConnectionRequest(req)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	result, err := testConnection(conn)
	if err != nil {
		conn.LastTestError = err.Error()
		conn.Connected = false
		http.Error(w, err.Error(), 400)
		return
	}
	conn.Connected = true
	conn.LastTestError = ""
	saveConnection(conn)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "saved",
		"contexts": contexts,
		"selected": selected,
		"result":   result,
		"targets":  conn.Targets,
	})
}

func getConfigHandler(w http.ResponseWriter, configType string) {
	conn, err := getConnection()
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	cmName := conn.Targets.MetaConfigMap
	cmKey := conn.Targets.MetaConfigKey
	if configType == "data" {
		cmName = conn.Targets.DataConfigMap
		cmKey = conn.Targets.DataConfigKey
	}

	data, err := getConfigMap(conn, cmName)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	raw := data[cmKey]
	sections := parseINI(raw)

	pendingMu.Lock()
	if pendingChanges[configType] == nil {
		pendingChanges[configType] = map[string]string{}
	}
	for si := range sections {
		for i := range sections[si].Settings {
			lookupKey := sections[si].Name + "." + sections[si].Settings[i].Key
			if v, ok := pendingChanges[configType][lookupKey]; ok {
				sections[si].Settings[i].Value = v
			}
		}
	}
	changed := len(pendingChanges[configType]) > 0
	pendingMu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"raw":      raw,
		"sections": sections,
		"changed":  changed,
		"schema":   listSchemaSettings(configType),
	})
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
	setting, err := validateSetting(configType, req.Section, req.Key, req.Value)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	pendingMu.Lock()
	if pendingChanges[configType] == nil {
		pendingChanges[configType] = map[string]string{}
	}
	lookupKey := req.Section + "." + req.Key
	pendingChanges[configType][lookupKey] = req.Value
	count := len(pendingChanges[configType])
	pendingMu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "staged",
		"section":    req.Section,
		"key":        req.Key,
		"value":      req.Value,
		"pending":    count,
		"type":       setting.Type,
		"highImpact": setting.HighImpact,
	})
}

func discardHandler(w http.ResponseWriter, r *http.Request, configType string) {
	var req struct {
		Section string `json:"section"`
		Key     string `json:"key"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	lookupKey := req.Section + "." + req.Key
	pendingMu.Lock()
	if pendingChanges[configType] != nil {
		delete(pendingChanges[configType], lookupKey)
	}
	remaining := len(pendingChanges[configType])
	pendingMu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{"status": "discarded", "pending": remaining})
}

func applyConfigHandler(w http.ResponseWriter, r *http.Request, configType string) {
	conn, err := getConnection()
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	pendingMu.Lock()
	changes := map[string]string{}
	for k, v := range pendingChanges[configType] {
		changes[k] = v
	}
	pendingMu.Unlock()

	for lookupKey, value := range changes {
		parts := strings.SplitN(lookupKey, ".", 2)
		if len(parts) != 2 {
			http.Error(w, "invalid staged key: "+lookupKey, 400)
			return
		}
		if _, err := validateSetting(configType, parts[0], parts[1], value); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
	}

	if len(changes) == 0 {
		json.NewEncoder(w).Encode(map[string]string{"status": "no changes to apply"})
		return
	}

	cmName := conn.Targets.MetaConfigMap
	cmKey := conn.Targets.MetaConfigKey
	if configType == "data" {
		cmName = conn.Targets.DataConfigMap
		cmKey = conn.Targets.DataConfigKey
	}

	data, err := getConfigMap(conn, cmName)
	if err != nil {
		http.Error(w, "failed to read ConfigMap: "+err.Error(), 500)
		return
	}

	raw := data[cmKey]
	lines := strings.Split(raw, "\n")
	currentSection := ""
	appliedChanges := map[string]bool{}

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			currentSection = trimmed[1 : len(trimmed)-1]
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
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			quoted := quoteValue(newVal, strings.TrimSpace(line[eqIdx+1:]))
			lines[i] = indent + key + " = " + quoted
			appliedChanges[lookupKey] = true
		}
	}

	for lookupKey, newVal := range changes {
		if appliedChanges[lookupKey] {
			continue
		}
		parts := strings.SplitN(lookupKey, ".", 2)
		if len(parts) != 2 {
			continue
		}
		section := parts[0]
		key := parts[1]
		quoted := quoteValue(newVal, "")
		newLine := "  " + key + " = " + quoted
		sectionHeader := "[" + section + "]"
		inserted := false

		if section == "global" {
			for i := len(lines) - 1; i >= 0; i-- {
				trim := strings.TrimSpace(lines[i])
				if trim == "" || strings.HasPrefix(trim, "[") || strings.HasPrefix(trim, "#") {
					continue
				}
				if strings.Contains(lines[i], "=") {
					lines = append(lines[:i+1], append([]string{newLine}, lines[i+1:]...)...)
					inserted = true
					break
				}
			}
		}

		if !inserted {
			sectionIdx := -1
			for i, line := range lines {
				if strings.TrimSpace(line) == sectionHeader {
					sectionIdx = i
				}
			}
			if sectionIdx >= 0 {
				insertAt := sectionIdx + 1
				for i := sectionIdx + 1; i < len(lines); i++ {
					trim := strings.TrimSpace(lines[i])
					if trim == "" || strings.HasPrefix(trim, "#") {
						continue
					}
					if strings.HasPrefix(trim, "[") {
						insertAt = i
						break
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
			lines = append(lines, "", sectionHeader, newLine)
		}
	}

	newRaw := strings.Join(lines, "\n")
	if err := updateConfigMap(conn, cmName, map[string]string{cmKey: newRaw}); err != nil {
		http.Error(w, "failed to update ConfigMap: "+err.Error(), 500)
		return
	}

	pendingMu.Lock()
	pendingChanges[configType] = map[string]string{}
	pendingMu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "applied",
		"changes":   len(changes),
		"configMap": cmName,
		"note":      "ConfigMap updated. Pods will pick up changes on next restart.",
	})
}

func restartHandler(w http.ResponseWriter, r *http.Request, configType string) {
	conn, err := getConnection()
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stsName := conn.Targets.MetaStatefulSet
	if configType == "data" {
		stsName = conn.Targets.DataStatefulSet
	}

	now := time.Now().UTC()
	annoVal := now.Format(time.RFC3339Nano)
	patchObj := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{
						"kubectl.kubernetes.io/restartedAt": annoVal,
						"config-v1/restarted":               fmt.Sprintf("%d", now.UnixNano()),
					},
				},
			},
		},
	}
	body, _ := json.Marshal(patchObj)
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s", conn.Targets.Namespace, stsName)
	_, code, err := k8sRequest(conn, http.MethodPatch, path, body)
	if err != nil {
		http.Error(w, "restart failed: "+err.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "restarting",
		"statefulSet": stsName,
		"httpStatus":  code,
		"note":        "Rolling restart initiated.",
	})
}

func preflightHandler(w http.ResponseWriter, r *http.Request, configType string) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req struct {
		Changes []struct {
			Section string `json:"section"`
			Key     string `json:"key"`
			Value   string `json:"value"`
		} `json:"changes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), 400)
		return
	}
	findings := []PreflightFinding{}
	validated := 0
	for _, c := range req.Changes {
		set, err := validateSetting(configType, c.Section, c.Key, c.Value)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		validated++
		if set.HighImpact {
			findings = append(findings, buildHighImpactFinding(set, c.Value))
		}
	}
	warnings := []string{}
	criticalCount := 0
	warnCount := 0
	for _, f := range findings {
		warnings = append(warnings, f.Message) // backward compatibility
		if f.Severity == "critical" {
			criticalCount++
		} else if f.Severity == "warn" {
			warnCount++
		}
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":        true,
		"validated": validated,
		"warnings":  warnings,
		"findings":  findings,
		"summary": map[string]int{
			"critical": criticalCount,
			"warn":     warnCount,
		},
	})
}

func getPendingHandler(w http.ResponseWriter, r *http.Request) {
	pendingMu.Lock()
	defer pendingMu.Unlock()
	if pendingChanges == nil {
		pendingChanges = map[string]map[string]string{}
	}
	type Change struct {
		ConfigType string `json:"type"`
		Section    string `json:"section"`
		Key        string `json:"key"`
		Value      string `json:"value"`
	}
	all := []Change{}
	for configType, changes := range pendingChanges {
		for lookupKey, value := range changes {
			parts := strings.SplitN(lookupKey, ".", 2)
			section := ""
			key := lookupKey
			if len(parts) == 2 {
				section, key = parts[0], parts[1]
			}
			all = append(all, Change{ConfigType: configType, Section: section, Key: key, Value: value})
		}
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"changes": all, "count": len(all)})
}

func discardAllHandler(w http.ResponseWriter, r *http.Request) {
	pendingMu.Lock()
	pendingChanges = map[string]map[string]string{}
	pendingMu.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"status": "all changes discarded"})
}

func normalizedKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	k = strings.ReplaceAll(k, "-", "")
	k = strings.ReplaceAll(k, "_", "")
	return k
}

func isSensitiveLicenseField(k string) bool {
	n := normalizedKey(k)
	if n == "signature" || n == "licensekey" || n == "key" {
		return true
	}
	return false
}

func sanitizeLicenseValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		out := map[string]interface{}{}
		for k, child := range val {
			if isSensitiveLicenseField(k) {
				continue
			}
			out[k] = sanitizeLicenseValue(child)
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(val))
		for _, child := range val {
			out = append(out, sanitizeLicenseValue(child))
		}
		return out
	default:
		return v
	}
}

func licenseInfoHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := getConnection()
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	secretName := os.Getenv("INFLUXDB_LICENSE_SECRET")
	if strings.TrimSpace(secretName) == "" {
		secretName = defaultLicenseSecret
	}

	path := fmt.Sprintf("/api/v1/namespaces/%s/secrets/%s", conn.Targets.Namespace, secretName)
	body, _, err := k8sRequest(conn, http.MethodGet, path, nil)
	if err != nil {
		http.Error(w, "failed to read license secret: "+err.Error(), 500)
		return
	}

	var sec struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &sec); err != nil {
		http.Error(w, "failed to parse secret JSON: "+err.Error(), 500)
		return
	}

	b64 := sec.Data["license.json"]
	if strings.TrimSpace(b64) == "" {
		http.Error(w, "license.json key missing in secret", 500)
		return
	}

	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		http.Error(w, "failed to decode license.json: "+err.Error(), 500)
		return
	}

	var raw interface{}
	if err := json.Unmarshal(decoded, &raw); err != nil {
		http.Error(w, "failed to parse license.json: "+err.Error(), 500)
		return
	}

	sanitized := sanitizeLicenseValue(raw)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"secret":  secretName,
		"license": sanitized,
	})
}

func loadInClusterMaterial() {
	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err == nil {
		inClusterToken = strings.TrimSpace(string(tokenBytes))
	} else {
		log.Printf("warning: in-cluster token unavailable: %v", err)
	}
	caBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err == nil {
		inClusterCAData = caBytes
	} else {
		log.Printf("warning: in-cluster CA unavailable: %v", err)
	}
}

func initDefaultConnection() {
	conn, err := buildInClusterConnection(ConnectionRequest{
		Mode:            "in-cluster",
		Namespace:       os.Getenv("INFLUXDB_NAMESPACE"),
		MetaConfigMap:   os.Getenv("INFLUXDB_META_CONFIGMAP"),
		DataConfigMap:   os.Getenv("INFLUXDB_DATA_CONFIGMAP"),
		MetaConfigKey:   os.Getenv("INFLUXDB_META_CONFIG_KEY"),
		DataConfigKey:   os.Getenv("INFLUXDB_DATA_CONFIG_KEY"),
		MetaStatefulSet: os.Getenv("INFLUXDB_META_STATEFULSET"),
		DataStatefulSet: os.Getenv("INFLUXDB_DATA_STATEFULSET"),
	})
	if err != nil {
		log.Printf("warning: default in-cluster connection not initialized: %v", err)
		return
	}
	result, err := testConnection(conn)
	if err != nil {
		conn.Connected = false
		conn.LastTestError = err.Error()
		log.Printf("warning: initial connection test failed: %v", err)
	} else {
		_ = result
		conn.Connected = true
	}
	saveConnection(conn)
}

func main() {
	loadInClusterMaterial()
	if err := loadSettingsSchema(); err != nil {
		log.Fatalf("failed to load settings schema: %v", err)
	}
	initDefaultConnection()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", corsMiddleware(healthHandler))

	mux.HandleFunc("/api/connection/status", corsMiddleware(connectionStatusHandler))
	mux.HandleFunc("/api/connection/contexts", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		connectionContextsHandler(w, r)
	}))
	mux.HandleFunc("/api/connection/test", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		connectionTestHandler(w, r)
	}))
	mux.HandleFunc("/api/connection/config", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		connectionConfigHandler(w, r)
	}))
	mux.HandleFunc("/api/schema/meta", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"settings": listSchemaSettings("meta"),
		})
	}))
	mux.HandleFunc("/api/schema/data", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"settings": listSchemaSettings("data"),
		})
	}))
	mux.HandleFunc("/api/license", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET required", 405)
			return
		}
		licenseInfoHandler(w, r)
	}))

	mux.HandleFunc("/api/config/meta", corsMiddleware(func(w http.ResponseWriter, r *http.Request) { getConfigHandler(w, "meta") }))
	mux.HandleFunc("/api/config/data", corsMiddleware(func(w http.ResponseWriter, r *http.Request) { getConfigHandler(w, "data") }))

	mux.HandleFunc("/api/config/meta/update", corsMiddleware(func(w http.ResponseWriter, r *http.Request) { updateConfigHandler(w, r, "meta") }))
	mux.HandleFunc("/api/config/data/update", corsMiddleware(func(w http.ResponseWriter, r *http.Request) { updateConfigHandler(w, r, "data") }))

	mux.HandleFunc("/api/config/meta/discard", corsMiddleware(func(w http.ResponseWriter, r *http.Request) { discardHandler(w, r, "meta") }))
	mux.HandleFunc("/api/config/data/discard", corsMiddleware(func(w http.ResponseWriter, r *http.Request) { discardHandler(w, r, "data") }))

	mux.HandleFunc("/api/config/meta/preflight", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		preflightHandler(w, r, "meta")
	}))
	mux.HandleFunc("/api/config/data/preflight", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		preflightHandler(w, r, "data")
	}))

	mux.HandleFunc("/api/config/meta/apply", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		applyConfigHandler(w, r, "meta")
	}))
	mux.HandleFunc("/api/config/data/apply", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		applyConfigHandler(w, r, "data")
	}))

	mux.HandleFunc("/api/restart/meta", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		restartHandler(w, r, "meta")
	}))
	mux.HandleFunc("/api/restart/data", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		restartHandler(w, r, "data")
	}))

	mux.HandleFunc("/api/pending", corsMiddleware(getPendingHandler))
	mux.HandleFunc("/api/pending/discard", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
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
