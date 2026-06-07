#!/usr/bin/env bash
set -euo pipefail

# k3s E2E Test Environment Status Script
# Shows the status of the k3d cluster and services

CLUSTER_NAME="${K3S_CLUSTER_NAME:-streamling-e2e}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common functions
source "${SCRIPT_DIR}/k3s-common.sh"

# Check if cluster exists
cluster_exists() {
    k3d cluster list -o json 2>/dev/null | grep -q "\"name\":\"${CLUSTER_NAME}\""
}

# Check if the correct kubectl context is selected
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

# Main
main() {
    local provider
    provider=$(get_k8s_provider)

    echo "E2E Test Environment Status"
    echo "================================"
    echo "Provider: ${provider}"
    echo ""

    case "$provider" in
        k3d)
            # Check cluster
            if cluster_exists; then
                log_info "Cluster '${CLUSTER_NAME}' exists"
                check_context
                echo ""

                # Show cluster info
                echo "Cluster Info:"
                k3d cluster list | grep -E "(NAME|${CLUSTER_NAME})"
                echo ""
            else
                log_warn "Cluster '${CLUSTER_NAME}' does not exist"
                echo ""
                echo "Run './scripts/k3s-setup.sh' to create the cluster"
                exit 0
            fi
            ;;
        docker-desktop)
            check_context
            echo ""
            ;;
        *)
            log_error "Unknown K8S_PROVIDER: ${provider}"
            exit 1
            ;;
    esac

    # Show namespace status
    if kubectl get namespace streamling-e2e &>/dev/null; then
        echo "Namespace: streamling-e2e exists"
        echo ""

        # Show pods
        echo "Pods in streamling-e2e namespace:"
        kubectl get pods -n streamling-e2e -o wide 2>/dev/null || log_warn "Cannot get pods"
        echo ""

        # Show services
        echo "Services:"
        kubectl get svc -n streamling-e2e 2>/dev/null || log_warn "Cannot get services"
        echo ""

        # Check connectivity
        echo "Service Connectivity:"

        # PostgreSQL
        if nc -z localhost 30432 2>/dev/null; then
            echo -e "  PostgreSQL (30432): ${GREEN}✓${NC}"
        else
            echo -e "  PostgreSQL (30432): ${RED}✗${NC}"
        fi

        # Kafka
        if nc -z localhost 30092 2>/dev/null; then
            echo -e "  Kafka (30092):      ${GREEN}✓${NC}"
        else
            echo -e "  Kafka (30092):      ${RED}✗${NC}"
        fi

        # Schema Registry
        if curl -s http://localhost:30081/subjects &>/dev/null; then
            echo -e "  Schema Registry (30081): ${GREEN}✓${NC}"
        else
            echo -e "  Schema Registry (30081): ${RED}✗${NC}"
        fi

        # ClickHouse
        if curl -s http://localhost:30123/ping &>/dev/null; then
            echo -e "  ClickHouse (30123): ${GREEN}✓${NC}"
        else
            echo -e "  ClickHouse (30123): ${RED}✗${NC}"
        fi

        # Prometheus
        if curl -s http://localhost:30090/-/ready &>/dev/null; then
            echo -e "  Prometheus (30090): ${GREEN}✓${NC}"
        else
            echo -e "  Prometheus (30090): ${RED}✗${NC}"
        fi

        # ElasticMQ (SQS)
        if nc -z localhost 30566 2>/dev/null; then
            echo -e "  ElasticMQ/SQS (30566): ${GREEN}✓${NC}"
        else
            echo -e "  ElasticMQ/SQS (30566): ${RED}✗${NC}"
        fi
    else
        log_warn "Namespace 'streamling-e2e' does not exist"
        echo ""
        echo "Run './scripts/k3s-setup.sh' to create the environment"
    fi
}

main "$@"

