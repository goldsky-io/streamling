# Environment variables for e2e tests and local development
# Source this file: source infra/local-k8s/env.sh

# E2E test framework variables
# AWS credentials for ElasticMQ/SQS (test process SqsResource; subprocess gets from build_env_vars)
export AWS_ACCESS_KEY_ID='test'
export AWS_SECRET_ACCESS_KEY='test'
export AWS_DEFAULT_REGION='us-east-1'

export E2E_POSTGRES_URL='postgres://postgres:postgres@localhost:30432/postgres?sslmode=disable'
export E2E_KAFKA_BROKER='localhost:30092'
export E2E_SCHEMA_REGISTRY_URL='http://localhost:30081'
export E2E_REDPANDA_ADMIN_URL='http://localhost:30644'
export E2E_CLICKHOUSE_URL='http://localhost:30123'
export E2E_MYSQL_URL='mysql://root:root@localhost:30306/test'
export E2E_PROMETHEUS_URL='http://localhost:30090'
# ElasticMQ SQS endpoint (NodePort 30566)
export E2E_SQS_URL='http://localhost:30566'

# Streamling application variables (for cargo run)
export STREAMLING__KAFKA_SOURCE__BROKERS='localhost:30092'
export STREAMLING__KAFKA_SOURCE__SCHEMA_REGISTRY_URL='http://localhost:30081'
export STREAMLING__KAFKA_SINK__BROKERS='localhost:30092'
export STREAMLING__KAFKA_SINK__SCHEMA_REGISTRY_URL='http://localhost:30081'
export STREAMLING__POSTGRES_SINK__HOST='localhost'
export STREAMLING__POSTGRES_SINK__PORT='30432'
export STREAMLING__POSTGRES_SINK__USER='postgres'
export STREAMLING__POSTGRES_SINK__PASS='postgres'
export STREAMLING__POSTGRES_SINK__DB='postgres'
export STREAMLING__POSTGRES_SINK__SSLMODE='disable'
export STREAMLING__CLICKHOUSE_SOURCE__URL='http://localhost:30123'
export STREAMLING__CLICKHOUSE_SOURCE__DATABASE='default'
export STREAMLING__CLICKHOUSE_SINK__URL='http://localhost:30123'
export STREAMLING__CLICKHOUSE_SINK__DATABASE='default'
export STREAMLING__OPEN_TELEMETRY_METRICS__INGESTION_ENDPOINT='http://localhost:30090/api/v1/otlp/v1/metrics'

