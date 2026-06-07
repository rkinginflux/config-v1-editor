# Config Editor — InfluxDB Enterprise v1

Interactive configuration management UI for InfluxDB Enterprise v1 clusters running on Kubernetes.

## Features

- **Live config viewer** — reads ConfigMaps from the cluster in real-time
- **Inline editing** — click any setting to edit, staged changes until applied
- **Search** — instant filtering across 60+ settings, descriptions, env vars
- **Node-type tabs** — separate editors for Meta and Data node config
- **Apply + Restart** — writes ConfigMap, then rolling-restarts StatefulSet
- **Safety** — staged changes, discard individual or all, no direct writes

## Quick Start

```
http://localhost:770/
http://192.168.0.30:770/
```

## Architecture

```
Browser (port 770)
    │
    ├─ /        → nginx (static HTML)
    └─ /api/*   → Go API (port 7701, internal)
                    │
                    ├─ GET  /api/config/meta   → reads influxdb-enterprise-meta ConfigMap
                    ├─ GET  /api/config/data   → reads influxdb-enterprise-data ConfigMap
                    ├─ PUT  /api/config/*/update  → stages a change
                    ├─ POST /api/config/*/apply   → writes ConfigMap
                    └─ POST /api/restart/*        → rolling-restart StatefulSet
```

## Deploy

```
kubectl apply -f deploy.yaml
```

Requires an existing InfluxDB Enterprise v1 cluster in the `influxdb-enterprise` namespace.

## Build from source

```
docker build -t config-v1:latest .
docker save config-v1:latest | sudo -S -p '' ctr -n k8s.io images import -
kubectl -n config-v1 delete pod -l app=config-v1 --force --grace-period=0
```

## Files

| File | Purpose |
|------|---------|
| `index.html` | Frontend — sidebar + config editor UI |
| `backend/main.go` | Go API — INI parser, K8s client, stage/apply/restart |
| `Dockerfile` | Multi-stage build (Go + nginx) |
| `deploy.yaml` | K8s manifest (namespace, SA, RBAC, service, deployment) |

## RBAC

The service account has read/write on these ConfigMaps in the `influxdb-enterprise` namespace:
- `influxdb-enterprise-meta`
- `influxdb-enterprise-data`

And patch access to these StatefulSets:
- `influxdb-enterprise-meta`
- `influxdb-enterprise-data`
