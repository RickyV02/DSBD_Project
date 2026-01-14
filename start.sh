#!/bin/bash

CLUSTER_NAME="dsbd-cluster"
KIND_CONFIG="k8s/kind-config.yaml"

echo "=== INIZIO SETUP ==="

# 1. Creation of Kind Cluster
if kind get clusters | grep -q "^$CLUSTER_NAME$"; then
    echo "Il cluster '$CLUSTER_NAME' esiste gia'."
else
    echo "Creazione del cluster '$CLUSTER_NAME' in corso..."

    if [ -f "$KIND_CONFIG" ]; then
        kind create cluster --config "$KIND_CONFIG" --name "$CLUSTER_NAME"
    else
        echo "ERRORE: Non trovo il file $KIND_CONFIG."
        echo "Assicurarsi che kind-config.yaml sia nella cartella k8s/ o aggiorna lo script."
        exit 1
    fi
fi

# 2. Install NGINX Ingress Controller
echo "Installazione NGINX Ingress Controller..."
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.9.6/deploy/static/provider/kind/deploy.yaml

echo "Attesa avvio Ingress Controller (max 180s)..."
sleep 5
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=180s

# 3. Build Docker images
echo "Building delle immagini Docker..."

# User Manager
if [ -d "user-manager" ]; then
    echo "   - Building User Manager..."
    docker build -t user-manager:latest -f user-manager/Dockerfile .
fi

# Data Collector
if [ -d "data-collector" ]; then
    echo "   - Building Data Collector..."
    docker build -t data-collector:latest -f data-collector/Dockerfile .
fi

# Alert System
if [ -d "alert-system" ]; then
    echo "   - Building Alert System..."
    docker build -t alert-system:latest -f alert-system/Dockerfile .
fi

# Alert Notifier System
if [ -d "alert-notifier-system" ]; then
    echo "   - Building Alert Notifier System..."
    docker build -t alert-notifier-system:latest -f alert-notifier-system/Dockerfile .
fi

# 4. Loading Docker images into Kind cluster
echo "Caricamento immagini nel cluster Kind..."
kind load docker-image user-manager:latest --name "$CLUSTER_NAME"
kind load docker-image data-collector:latest --name "$CLUSTER_NAME"
kind load docker-image alert-system:latest --name "$CLUSTER_NAME"
kind load docker-image alert-notifier-system:latest --name "$CLUSTER_NAME"


# 5. Apply Kubernetes configurations
echo "Applicazione configurazioni Kubernetes..."

kubectl apply -f k8s/namespace.yaml 2>/dev/null || true
kubectl apply -f k8s/config.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/nginx-secret.yaml

kubectl apply -f k8s/mysql-statefulset.yaml
kubectl apply -f k8s/kafka.yaml

# We could use "kubectl wait" here to avoid crash loops, but k8s will handle restarts anyway

kubectl apply -f k8s/user-manager.yaml
kubectl apply -f k8s/data-collector.yaml
kubectl apply -f k8s/alert-system.yaml
kubectl apply -f k8s/alert-notifier.yaml

kubectl apply -f k8s/ingress.yaml
kubectl apply -f k8s/prometheus-config.yaml

echo "=== SETUP COMPLETATO ==="
echo "Controllare lo stato con: kubectl get pods -n dsbd-ns -w"
