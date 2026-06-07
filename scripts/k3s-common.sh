#!/usr/bin/env bash
# Common functions for E2E test environment scripts

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get K8S provider
get_k8s_provider() {
    local provider="${K8S_PROVIDER:-k3d}"
    # Normalize docker_desktop to docker-desktop
    echo "${provider//_/-}"
}

# Get expected kubectl context for provider
get_expected_context() {
    local provider="$1"
    case "$provider" in
        k3d)
            echo "k3d-${CLUSTER_NAME}"
            ;;
        docker-desktop)
            echo "docker-desktop"
            ;;
        *)
            log_error "Unknown K8S_PROVIDER: ${provider}"
            log_error "Supported providers: k3d, docker-desktop"
            exit 1
            ;;
    esac
}
