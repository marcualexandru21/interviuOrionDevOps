#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Copiază CA-ul public al clusterului Kafka (Strimzi) din namespace-ul kafka în
# namespace-ul clienților (logging), ca Fluent Bit / Logstash să poată verifica
# certificatul brokerilor.
#
# Copiază doar certificatul public (ca.crt), nu și cheia privată a CA-ului.
# Idempotent: poate fi rulat oricând (create --dry-run | apply).
#
# Strimzi reînnoiește CA-ul clusterului înainte de expirare (implicit la 365 zile).
# După reînnoire scriptul trebuie rulat din nou și clienții reporniți (--restart).
# În producție această copiere o face un operator (cert-manager trust-manager sau
# External Secrets), nu un script.
#
# Utilizare: Kafka/sync-cluster-ca.sh [--restart]
# -----------------------------------------------------------------------------
set -euo pipefail

CLUSTER="orion-kafka"
SRC_NS="kafka"
DST_NS="logging"
DST_SECRET="kafka-cluster-ca"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

kubectl get secret "${CLUSTER}-cluster-ca-cert" -n "$SRC_NS" \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > "$tmp"

if [[ ! -s "$tmp" ]]; then
  echo "CA-ul clusterului nu a fost găsit (${SRC_NS}/${CLUSTER}-cluster-ca-cert). Kafka este Ready?" >&2
  exit 1
fi

kubectl create secret generic "$DST_SECRET" -n "$DST_NS" \
  --from-file=ca.crt="$tmp" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret ${DST_NS}/${DST_SECRET} actualizat."

if [[ "${1:-}" == "--restart" ]]; then
  kubectl rollout restart daemonset/fluent-bit -n "$DST_NS"
fi
