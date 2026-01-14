#!/bin/bash
CLUSTER_NAME="dsbd-cluster"
echo "Riavvio del cluster '$CLUSTER_NAME'..."
docker start $(kind get nodes --name "$CLUSTER_NAME")
echo "Controllare con: kubectl get pods -n dsbd-ns -w"
