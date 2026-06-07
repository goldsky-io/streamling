#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ENV_FILE="${SCRIPT_DIR}/../infra/local-k8s/env.sh"

if [ ! -f "$ENV_FILE" ]; then
  echo "Environment file not found: $ENV_FILE"
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

NAMESPACE="streamling-e2e"
# In-cluster addresses for pods talking to Redpanda
KAFKA_INTERNAL="redpanda.${NAMESPACE}.svc.cluster.local:9092"
SCHEMA_REGISTRY_INTERNAL="http://redpanda.${NAMESPACE}.svc.cluster.local:18081"
TOPIC="mainnet.raw.blocks"

echo "Working from script directory: $SCRIPT_DIR"

echo "Waiting for Redpanda to be ready..."
until curl -s "${E2E_REDPANDA_ADMIN_URL}/v1/cluster/health_overview" > /dev/null; do
  echo "Waiting for Redpanda to start..."
  sleep 2
done

echo "Redpanda is ready!"

echo "Listing topics..."
kubectl exec -n "$NAMESPACE" deployment/redpanda -- rpk topic list --brokers localhost:9092

echo "Registering Avro schema with Redpanda schema registry..."
curl -X POST "${E2E_SCHEMA_REGISTRY_URL}/subjects/${TOPIC}-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\": \"record\", \"name\": \"Block\", \"fields\": [{\"name\": \"id\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"number\", \"type\": \"int\"}, {\"name\": \"hash\", \"type\": \"string\"}, {\"name\": \"message\", \"type\": \"string\"}]}"
  }' || echo "Schema already exists"

echo "Generating test data using Redpanda Connect..."

CONFIG_FILE="$(mktemp -t redpanda-connect-config.XXXXXX).yaml"
JOB_NAME="redpanda-connect-init"
CONFIGMAP_NAME="redpanda-connect-config"
cleanup() {
  rm -f "$CONFIG_FILE"
  kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete configmap "$CONFIGMAP_NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

cat > "$CONFIG_FILE" <<EOF
input:
  generate:
    interval: 1s
    count: 10
    mapping: |
      let number = counter()
      meta kafka_key = \$number.string()
      root.id = uuid_v4()
      root.timestamp = now().string()
      root.number = \$number
      root.hash = "0x" + uuid_v4().replace("-", "")
      root.message = "test block data"

pipeline:
  processors:
    - schema_registry_encode:
        url: ${SCHEMA_REGISTRY_INTERNAL}
        subject: ${TOPIC}-value
        avro_raw_json: false

output:
  kafka:
    addresses:
      - ${KAFKA_INTERNAL}
    topic: ${TOPIC}
    key: \${! @kafka_key }

logger:
  level: INFO
EOF

kubectl create configmap "$CONFIGMAP_NAME" \
  -n "$NAMESPACE" \
  --from-file=config.yaml="$CONFIG_FILE" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  ttlSecondsAfterFinished: 60
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: connect
          image: redpandadata/connect:latest
          args: ["run", "/config/config.yaml"]
          volumeMounts:
            - name: config
              mountPath: /config
      volumes:
        - name: config
          configMap:
            name: ${CONFIGMAP_NAME}
EOF

echo "Running Redpanda Connect to generate 10 test records..."
if ! kubectl wait --for=condition=complete --timeout=120s -n "$NAMESPACE" job/"$JOB_NAME"; then
  echo "Connect job failed; logs:"
  kubectl logs -n "$NAMESPACE" job/"$JOB_NAME" || true
  exit 1
fi

echo "Verifying data..."
kubectl run kcat-verify --rm -i --restart=Never \
  -n "$NAMESPACE" \
  --image=confluentinc/cp-kcat:7.4.1 \
  -- kcat -L -b "${KAFKA_INTERNAL}"

echo "Sample messages from ${TOPIC} topic:"
kubectl run kcat-sample --rm -i --restart=Never \
  -n "$NAMESPACE" \
  --image=confluentinc/cp-kcat:7.4.1 \
  -- kcat -C -b "${KAFKA_INTERNAL}" -t "${TOPIC}" -c 1 -J -r "${SCHEMA_REGISTRY_INTERNAL}"

echo "Redpanda initialization complete!"
