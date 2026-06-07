#!/usr/bin/env bash
set -euo pipefail

# k3s E2E Test Environment Setup Script
# This script creates a k3d cluster and deploys the required services for e2e testing

CLUSTER_NAME="${K3S_CLUSTER_NAME:-streamling-e2e}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/../infra/local-k8s"

# Source common functions
source "${SCRIPT_DIR}/k3s-common.sh"

# Check if k3d is installed
check_k3d() {
    if ! command -v k3d &> /dev/null; then
        log_error "k3d is not installed. Please install it first:"
        echo "  brew install k3d  # macOS"
        echo "  curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash  # Linux"
        exit 1
    fi
    log_info "k3d version: $(k3d version | head -1)"
}

# Check if kubectl is installed
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed. Please install it first."
        exit 1
    fi
    log_info "kubectl version: $(kubectl version --client --short 2>/dev/null || kubectl version --client | head -1)"
}

# Check if Docker Desktop Kubernetes is running
check_docker_desktop() {
    if ! kubectl cluster-info &>/dev/null; then
        log_error "Docker Desktop Kubernetes is not running"
        log_error "Enable Kubernetes in Docker Desktop preferences:"
        echo "  Docker Desktop → Settings → Kubernetes → Enable Kubernetes"
        exit 1
    fi
    log_info "Docker Desktop Kubernetes is running"
}

# Check if cluster already exists
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

# Create k3d cluster
create_cluster() {
    if cluster_exists; then
        log_info "Cluster '${CLUSTER_NAME}' already exists"
        
        # Ensure kubeconfig is available
        k3d kubeconfig merge "${CLUSTER_NAME}" &>/dev/null || true
        check_context
        return 0
    fi
    
    log_info "Creating k3d cluster '${CLUSTER_NAME}'..."
    
    k3d cluster create "${CLUSTER_NAME}" \
        --servers 1 \
        --agents 1 \
        --port "30432:30432@server:0" \
        --port "30092:30092@server:0" \
        --port "30081:30081@server:0" \
        --port "30082:30082@server:0" \
        --port "30123:30123@server:0" \
        --port "30900:30900@server:0" \
        --port "30090:30090@server:0" \
        --port "30306:30306@server:0" \
        --port "30566:30566@server:0" \
        --port "30644:30644@server:0" \
        --wait
    
    log_info "Cluster created successfully"
    
    # k3d automatically switches context on create, but verify
    check_context
}

# Deploy services
deploy_services() {
    log_info "Deploying services to cluster..."
    
    # Create namespace first
    kubectl apply -f "${K8S_DIR}/namespace.yaml"
    
    # Wait for namespace to be ready
    kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/streamling-e2e --timeout=30s
    
    # Deploy all services in parallel
    log_info "Deploying services in parallel..."
    
    kubectl apply -f "${K8S_DIR}/postgres.yaml" &
    kubectl apply -f "${K8S_DIR}/redpanda.yaml" &
    kubectl apply -f "${K8S_DIR}/clickhouse.yaml" &
    kubectl apply -f "${K8S_DIR}/mysql.yaml" &
    kubectl apply -f "${K8S_DIR}/prometheus.yaml" &
    kubectl apply -f "${K8S_DIR}/elasticmq.yaml" &
    
    wait
    log_info "All service manifests applied"
}

# Wait for services to be ready
wait_for_services() {
    log_info "Waiting for services to be ready..."
    
    local timeout=180
    local services=("postgres" "redpanda" "clickhouse" "mysql" "prometheus" "elasticmq")
    
    for service in "${services[@]}"; do
        log_info "Waiting for ${service}..."
        if ! kubectl wait --for=condition=available deployment/${service} \
            -n streamling-e2e --timeout=${timeout}s 2>/dev/null; then
            log_warn "${service} deployment not ready within ${timeout}s, checking pods..."
            kubectl get pods -n streamling-e2e -l app=${service}
        fi
    done
    
    # Additional wait for readiness probes
    log_info "Waiting for readiness probes..."
    sleep 10
    
    # Verify services are accessible
    verify_services
}

# Verify services are accessible
verify_services() {
    log_info "Verifying services are accessible..."
    
    local all_ok=true
    
    # Check PostgreSQL
    if pg_isready -h localhost -p 30432 -U postgres &>/dev/null || \
       nc -z localhost 30432 2>/dev/null; then
        log_info "✓ PostgreSQL is accessible on localhost:30432"
    else
        log_warn "✗ PostgreSQL may not be ready yet on localhost:30432"
        all_ok=false
    fi
    
    # Check Redpanda/Kafka
    if nc -z localhost 30092 2>/dev/null; then
        log_info "✓ Kafka (Redpanda) is accessible on localhost:30092"
    else
        log_warn "✗ Kafka (Redpanda) may not be ready yet on localhost:30092"
        all_ok=false
    fi
    
    # Check Schema Registry
    if curl -s http://localhost:30081/subjects &>/dev/null; then
        log_info "✓ Schema Registry is accessible on localhost:30081"
    else
        log_warn "✗ Schema Registry may not be ready yet on localhost:30081"
        all_ok=false
    fi
    
    # Check ClickHouse
    if curl -s http://localhost:30123/ping &>/dev/null; then
        log_info "✓ ClickHouse is accessible on localhost:30123"
    else
        log_warn "✗ ClickHouse may not be ready yet on localhost:30123"
        all_ok=false
    fi
    
    # Check MySQL (MariaDB)
    if nc -z localhost 30306 2>/dev/null; then
        log_info "✓ MySQL (MariaDB) is accessible on localhost:30306"
    else
        log_warn "✗ MySQL (MariaDB) may not be ready yet on localhost:30306"
        all_ok=false
    fi

    # Check Prometheus
    if curl -s http://localhost:30090/-/ready &>/dev/null; then
        log_info "✓ Prometheus is accessible on localhost:30090"
    else
        log_warn "✗ Prometheus may not be ready yet on localhost:30090"
        all_ok=false
    fi
    
    # Check ElasticMQ (SQS)
    if nc -z localhost 30566 2>/dev/null; then
        log_info "✓ ElasticMQ (SQS) is accessible on localhost:30566"
    else
        log_warn "✗ ElasticMQ (SQS) may not be ready yet on localhost:30566"
        all_ok=false
    fi
    
    if [ "$all_ok" = false ]; then
        log_warn "Some services may need more time to start. Check with: kubectl get pods -n streamling-e2e"
    fi
}

# Main
main() {
    local provider
    provider=$(get_k8s_provider)
    local env_file="${K8S_DIR}/env.sh"

    if [[ "${1:-}" == "--env-only" ]]; then
        cat "${env_file}"
        exit 0
    fi

    log_info "Setting up E2E test environment with ${provider}..."
    echo ""

    check_kubectl

    case "$provider" in
        k3d)
            check_k3d
            create_cluster
            ;;
        docker-desktop)
            check_docker_desktop
            check_context
            ;;
        *)
            log_error "Unknown K8S_PROVIDER: ${provider}"
            exit 1
            ;;
    esac

    deploy_services
    wait_for_services

    echo ""
    log_info "Setup complete! (provider: ${provider})"
    log_info "Source environment variables with: source ${env_file}"
}

main "$@"

