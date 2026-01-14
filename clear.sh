#!/bin/bash

CLUSTER_NAME="dsbd-cluster"

echo "Eliminazione del cluster '$CLUSTER_NAME'..."
kind delete cluster --name "$CLUSTER_NAME"

echo "Cluster eliminato correttamente."
