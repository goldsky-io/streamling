#!/usr/bin/env bash
set -euo pipefail

# k3s E2E Test Environment Teardown Script
# This script removes the k3d cluster used for e2e testing

CLUSTER_NAME="${K3S_CLUSTER_NAME:-streamling-e2e}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common functions
source "${SCRIPT_DIR}/k3s-common.sh"

# Check if k3d is installed
check_k3d() {
    if ! command -v k3d &> /dev/null; then
        log_error "k3d is not installed."
        exit 1
    fi
}

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

# Delete cluster
delete_cluster() {
    if ! cluster_exists; then
        log_warn "Cluster '${CLUSTER_NAME}' does not exist"
        return 0
    fi
    
    log_info "Deleting k3d cluster '${CLUSTER_NAME}'..."
    k3d cluster delete "${CLUSTER_NAME}"
    log_info "Cluster deleted successfully"
}

# Main
main() {
    local provider
    provider=$(get_k8s_provider)
    local force=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            -f|--force)
                force=true
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done

    case "$provider" in
        k3d)
            check_k3d

            if ! cluster_exists; then
                log_info "Cluster '${CLUSTER_NAME}' does not exist. Nothing to do."
                exit 0
            fi

            check_context

            if [ "$force" = false ]; then
                read -p "Are you sure you want to delete the cluster '${CLUSTER_NAME}'? [y/N] " -n 1 -r
                echo
                [[ ! $REPLY =~ ^[Yy]$ ]] && exit 0
            fi

            delete_cluster
            ;;
        docker-desktop)
            check_context

            # Check if namespace exists
            if ! kubectl get namespace streamling-e2e &>/dev/null; then
                log_info "Namespace 'streamling-e2e' does not exist. Nothing to do."
                exit 0
            fi

            if [ "$force" = false ]; then
                read -p "Delete namespace 'streamling-e2e' from Docker Desktop? [y/N] " -n 1 -r
                echo
                [[ ! $REPLY =~ ^[Yy]$ ]] && exit 0
            fi

            log_info "Deleting namespace streamling-e2e..."
            kubectl delete namespace streamling-e2e --wait=true
            log_info "Namespace deleted successfully"
            ;;
        *)
            log_error "Unknown K8S_PROVIDER: ${provider}"
            exit 1
            ;;
    esac

    log_info "Teardown complete! (provider: ${provider})"
}

main "$@"

