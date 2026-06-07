#!/usr/bin/env bash
set -euo pipefail

# Clean up orphaned test resources (databases, topics) from failed test runs
# This script removes test databases and topics that weren't cleaned up properly

CLUSTER_NAME="${K3S_CLUSTER_NAME:-streamling-e2e}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common functions
source "${SCRIPT_DIR}/k3s-common.sh"

# Check kubectl context
check_context() {
    local provider
    provider=$(get_k8s_provider)
    local expected_context
    expected_context=$(get_expected_context "$provider")

    local current_context
    current_context=$(kubectl config current-context 2>/dev/null || echo "")

    if [[ "${current_context}" != "${expected_context}" ]]; then
        log_error "kubectl context is '${current_context}', expected '${expected_context}'"
        log_error "Run this command to switch:"
        echo ""
        echo "  kubectl config use-context ${expected_context}"
        echo ""
        exit 1
    else
        log_info "kubectl context: ${expected_context} (provider: ${provider})"
        return 0
    fi
}

# Clean up orphaned PostgreSQL databases
cleanup_postgres() {
    eval "$("${SCRIPT_DIR}/k3s-setup.sh" --env-only)"
    
    local databases
    databases=$(psql "${E2E_POSTGRES_URL}" -t -c "SELECT datname FROM pg_database WHERE datname LIKE 'test_%';" 2>/dev/null || echo "")
    
    [[ -z "${databases}" ]] && return 0
    
    local count=0
    while IFS= read -r db; do
        db=$(echo "$db" | xargs)
        if [[ -n "$db" ]]; then
            psql "${E2E_POSTGRES_URL}" -c "DROP DATABASE IF EXISTS \"$db\";" &>/dev/null || true
            ((count++)) || true
        fi
    done <<< "$databases"
    
    [[ $count -gt 0 ]] && log_info "Dropped $count PostgreSQL database(s)"
}

# Clean up orphaned Kafka topics
cleanup_kafka() {
    local redpanda_pod
    redpanda_pod=$(kubectl get pods -n streamling-e2e -l app=redpanda -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    
    [[ -z "${redpanda_pod}" ]] && return 0
    
    local topics
    topics=$(kubectl exec -n streamling-e2e "${redpanda_pod}" -- \
        rpk topic list --format json 2>/dev/null | \
        grep -oE '"name":"test_[^"]+_topic"' | \
        sed 's/"name":"//;s/"//' || echo "")
    
    [[ -z "${topics}" ]] && return 0
    
    local count=0
    for topic in $topics; do
        kubectl exec -n streamling-e2e "${redpanda_pod}" -- \
            rpk topic delete "$topic" &>/dev/null || true
        ((count++)) || true
    done
    
    [[ $count -gt 0 ]] && log_info "Deleted $count Kafka topic(s)"
}

# Clean up orphaned ClickHouse databases
cleanup_clickhouse() {
    eval "$("${SCRIPT_DIR}/k3s-setup.sh" --env-only)"
    
    local databases
    databases=$(curl -s "${E2E_CLICKHOUSE_URL}/?query=SELECT name FROM system.databases WHERE name LIKE 'test_%'" 2>/dev/null | grep -oE 'test_[a-f0-9]+' || echo "")
    
    [[ -z "${databases}" ]] && return 0
    
    local count=0
    for db in $databases; do
        curl -s "${E2E_CLICKHOUSE_URL}/?query=DROP DATABASE IF EXISTS \`$db\`" &>/dev/null || true
        ((count++)) || true
    done
    
    [[ $count -gt 0 ]] && log_info "Dropped $count ClickHouse database(s)"
}

# Clean up orphaned MySQL databases
cleanup_mysql() {
    eval "$("${SCRIPT_DIR}/k3s-setup.sh" --env-only)"
    
    # mysql CLI may not be installed; use kubectl exec instead
    local mysql_pod
    mysql_pod=$(kubectl get pods -n streamling-e2e -l app=mysql -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    
    [[ -z "${mysql_pod}" ]] && return 0
    
    local databases
    databases=$(kubectl exec -n streamling-e2e "${mysql_pod}" -- \
        mariadb -u root -proot -N -e "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'test_%';" 2>/dev/null || echo "")
    
    [[ -z "${databases}" ]] && return 0
    
    local count=0
    while IFS= read -r db; do
        db=$(echo "$db" | xargs)
        if [[ -n "$db" ]]; then
            kubectl exec -n streamling-e2e "${mysql_pod}" -- \
                mariadb -u root -proot -e "DROP DATABASE IF EXISTS \`$db\`;" &>/dev/null || true
            ((count++)) || true
        fi
    done <<< "$databases"
    
    [[ $count -gt 0 ]] && log_info "Dropped $count MySQL database(s)"
}

main() {
    check_context
    cleanup_postgres
    cleanup_kafka
    cleanup_clickhouse
    cleanup_mysql
    log_info "Cleanup complete"
}

main "$@"
