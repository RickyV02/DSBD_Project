#!/bin/bash
CLUSTER_NAME="dsbd-cluster"
echo "Arresto del cluster '$CLUSTER_NAME'..."
docker stop $(kind get nodes --name "$CLUSTER_NAME")
echo "Cluster in pausa. I dati persistono nei volumi associati."
