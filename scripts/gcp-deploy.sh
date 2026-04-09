#!/usr/bin/env bash
# ── SeleneDB GCP Deployment Script ─────────────────────────────────────
#
# Provisions an n1-standard-4 + T4 Spot VM with Tailscale networking.
# All traffic flows over the Tailscale mesh — no public ports exposed.
#
# Prerequisites:
#   1. gcloud CLI authenticated with appropriate permissions
#   2. HF token in Secret Manager: gcloud secrets create hf-token --data-file=.hf_token
#   3. Tailscale auth key (reusable, tagged: tag:server)
#   4. Artifact Registry repository for images
#
# Usage:
#   ./scripts/gcp-deploy.sh          # Full setup (first time)
#   ./scripts/gcp-deploy.sh build    # Build image only
#   ./scripts/gcp-deploy.sh create   # Create VM only
#   ./scripts/gcp-deploy.sh deploy   # Deploy to existing VM
#   ./scripts/gcp-deploy.sh teardown # Delete VM (preserves disk)

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-selene-gpu}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-4}"
GPU_TYPE="nvidia-tesla-t4"
GPU_COUNT=1
DISK_SIZE="50GB"
DISK_NAME="${VM_NAME}-data"
AR_REPO="${AR_REPO:-selene}"
IMAGE_NAME="selene-server"
BOOT_DISK_SIZE="30GB"

# Artifact Registry image path
AR_IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/${IMAGE_NAME}"

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

# ── Ensure Artifact Registry Repo ──────────────────────────────────────
ensure_ar_repo() {
    if ! gcloud artifacts repositories describe "${AR_REPO}" \
        --location="${REGION}" --format="value(name)" &>/dev/null; then
        log "Creating Artifact Registry repository: ${AR_REPO}"
        gcloud artifacts repositories create "${AR_REPO}" \
            --repository-format=docker \
            --location="${REGION}" \
            --description="SeleneDB container images"
    else
        log "Artifact Registry repository '${AR_REPO}' exists"
    fi
}

# ── Build Image via Cloud Build ─────────────────────────────────────────
build_image() {
    log "Submitting Cloud Build for GPU image..."

    # Verify HF token secret exists
    if ! gcloud secrets describe hf-token &>/dev/null; then
        err "Secret 'hf-token' not found. Create it with:\n  gcloud secrets create hf-token --data-file=.hf_token"
    fi

    ensure_ar_repo

    gcloud builds submit \
        --config=cloudbuild.yaml \
        --substitutions="_REGION=${REGION},_REPO=${AR_REPO}" \
        --timeout=3600s

    log "Image built and pushed: ${AR_IMAGE}:gpu-latest"
}

# ── Create Persistent Data Disk ─────────────────────────────────────────
ensure_data_disk() {
    if ! gcloud compute disks describe "${DISK_NAME}" --zone="${ZONE}" &>/dev/null; then
        log "Creating persistent data disk: ${DISK_NAME} (${DISK_SIZE})"
        gcloud compute disks create "${DISK_NAME}" \
            --zone="${ZONE}" \
            --size="${DISK_SIZE}" \
            --type=pd-ssd
    else
        log "Data disk '${DISK_NAME}' exists"
    fi
}

# ── Create VM ────────────────────────────────────────────────────────────
create_vm() {
    ensure_data_disk

    if gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" &>/dev/null; then
        log "VM '${VM_NAME}' already exists"
        return
    fi

    log "Creating Spot VM: ${VM_NAME} (${MACHINE_TYPE} + ${GPU_TYPE})"

    # Startup script: auto-install GPU drivers and mount data disk on boot
    local startup_script
    startup_script=$(cat <<'STARTUP'
#!/bin/bash
set -e
# Install GPU drivers on every boot (idempotent)
if ! nvidia-smi &>/dev/null; then
    cos-extensions install gpu 2>&1 | logger -t gpu-setup
    mount --bind /var/lib/nvidia /var/lib/nvidia
    mount -o remount,exec /var/lib/nvidia
fi
# Mount data disk
if ! mountpoint -q /mnt/disks/selene-data; then
    mkdir -p /mnt/disks/selene-data
    DISK_DEV=$(readlink -f /dev/disk/by-id/google-selene-data)
    if ! blkid "${DISK_DEV}" &>/dev/null; then
        mkfs.ext4 -m 0 -E lazy_itable_init=0,discard "${DISK_DEV}"
    fi
    mount -o discard,defaults "${DISK_DEV}" /mnt/disks/selene-data
    chown 65532:65532 /mnt/disks/selene-data
    chmod 750 /mnt/disks/selene-data
fi
STARTUP
    )

    gcloud compute instances create "${VM_NAME}" \
        --zone="${ZONE}" \
        --machine-type="${MACHINE_TYPE}" \
        --accelerator="type=${GPU_TYPE},count=${GPU_COUNT}" \
        --maintenance-policy=TERMINATE \
        --provisioning-model=SPOT \
        --instance-termination-action=STOP \
        --boot-disk-size="${BOOT_DISK_SIZE}" \
        --boot-disk-type=pd-balanced \
        --image-family=cos-113-lts \
        --image-project=cos-cloud \
        --disk="name=${DISK_NAME},device-name=selene-data,mode=rw,auto-delete=no" \
        --scopes=cloud-platform \
        --metadata=cos-update-strategy=update_disabled \
        --metadata-from-file=startup-script=<(echo "${startup_script}") \
        --no-address \
        --tags=selene-server

    log "VM created. No public IP — access via IAP SSH and Tailscale."
    log "Waiting for startup script to install GPU drivers (1-2 min)..."
    sleep 30
}

# ── SSH/SCP Wrappers (IAP tunnel for no-public-IP VMs) ───────────────────
vm_scp() {
    gcloud compute scp --tunnel-through-iap --zone="${ZONE}" "$@"
}

vm_ssh() {
    gcloud compute ssh --tunnel-through-iap --zone="${ZONE}" "${VM_NAME}" --command="$1"
}

# ── Generate and Upload Secrets ──────────────────────────────────────────
upload_secrets() {
    log "Generating MCP secrets and uploading to VM..."

    # Source local .env for Tailscale key
    if [[ -f .env ]]; then
        # shellcheck disable=SC1091
        source .env
    fi

    [[ -z "${TAILSCALE_AUTHKEY:-}" ]] && err "TAILSCALE_AUTHKEY not set. Add to .env or export it."

    # Generate MCP secrets if not already set
    local api_key="${SELENE_MCP_API_KEY:-$(openssl rand -hex 32)}"
    local signing_key="${SELENE_MCP_SIGNING_KEY:-$(openssl rand -hex 64)}"
    local reg_token="${SELENE_MCP_REGISTRATION_TOKEN:-$(openssl rand -hex 32)}"

    # Create .env on the VM
    local env_content
    env_content=$(cat <<ENVEOF
TAILSCALE_AUTHKEY=${TAILSCALE_AUTHKEY}
SELENE_MCP_API_KEY=${api_key}
SELENE_MCP_SIGNING_KEY=${signing_key}
SELENE_MCP_REGISTRATION_TOKEN=${reg_token}
ENVEOF
    )

    vm_ssh "cat > ~/.env <<'EOF'
${env_content}
EOF
chmod 600 ~/.env"

    log "Secrets uploaded. MCP API key: ${api_key:0:8}..."
}

# ── Deploy to VM ─────────────────────────────────────────────────────────
deploy_to_vm() {
    log "Deploying SeleneDB to ${VM_NAME}..."

    # Upload compose, config, and secrets
    vm_scp docker-compose.gpu.yml selene.cloud.toml "${VM_NAME}:~/"
    upload_secrets

    # Setup and start services
    vm_ssh "
        set -e

        # COS installs nvidia to /var/lib/nvidia/bin
        export PATH=/var/lib/nvidia/bin:\$PATH
        export LD_LIBRARY_PATH=/var/lib/nvidia/lib64:\$LD_LIBRARY_PATH

        # Wait for startup script to complete GPU setup
        echo '==> Waiting for GPU drivers...'
        for i in \$(seq 1 30); do
            nvidia-smi &>/dev/null && break
            echo '  Waiting... ('\$i'/30)'
            sleep 10
        done
        nvidia-smi &>/dev/null || { echo 'ERROR: GPU drivers not ready after 5 min'; exit 1; }
        echo '==> GPU ready:'
        nvidia-smi --query-gpu=name,memory.total --format=csv,noheader

        # Verify data disk (mount if startup script didn't)
        if ! mountpoint -q /mnt/disks/selene-data; then
            echo '==> Mounting data disk...'
            sudo mkdir -p /mnt/disks/selene-data
            DISK_DEV=\$(readlink -f /dev/disk/by-id/google-selene-data)
            if ! sudo blkid \"\${DISK_DEV}\" &>/dev/null; then
                sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,discard \"\${DISK_DEV}\"
            fi
            sudo mount -o discard,defaults \"\${DISK_DEV}\" /mnt/disks/selene-data
            sudo chown 65532:65532 /mnt/disks/selene-data
            sudo chmod 750 /mnt/disks/selene-data
        fi

        # Authenticate to Artifact Registry
        docker-credential-gcr configure-docker --registries=${REGION}-docker.pkg.dev

        # Pull latest image
        docker pull ${AR_IMAGE}:gpu-latest

        # Load secrets and start services
        set -a; source ~/.env; set +a
        cd ~
        docker compose -f docker-compose.gpu.yml up -d

        echo '==> Deployment complete. Access via Tailscale: http://selene-gcp:8080'
    "
}

# ── Teardown (preserves data disk) ───────────────────────────────────────
teardown() {
    log "Deleting VM ${VM_NAME} (data disk preserved)..."
    gcloud compute instances delete "${VM_NAME}" \
        --zone="${ZONE}" \
        --quiet
    log "VM deleted. Data disk '${DISK_NAME}' preserved for re-attach."
}

# ── Status ───────────────────────────────────────────────────────────────
status() {
    log "Checking SeleneDB deployment status..."

    if ! gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" &>/dev/null; then
        log "VM '${VM_NAME}' does not exist."
        return
    fi

    local vm_status
    vm_status=$(gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" \
        --format="value(status)")
    log "VM status: ${vm_status}"

    if [[ "${vm_status}" == "RUNNING" ]]; then
        log "Checking GPU and services..."
        vm_ssh "
            export PATH=/var/lib/nvidia/bin:\$PATH
            echo '--- GPU ---'
            nvidia-smi --query-gpu=name,memory.total,memory.used --format=csv,noheader 2>/dev/null || echo 'GPU drivers not ready'
            echo '--- Containers ---'
            docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || echo 'No containers'
            echo '--- Disk ---'
            df -h /mnt/disks/selene-data 2>/dev/null || echo 'Data disk not mounted'
            echo '--- Tailscale ---'
            docker exec \$(docker ps -q -f name=tailscale) tailscale status 2>/dev/null || echo 'Tailscale not running'
        " 2>/dev/null || log "Cannot SSH to VM (may still be booting)"
    fi
}

# ── Main ─────────────────────────────────────────────────────────────────
case "${1:-all}" in
    build)    build_image ;;
    create)   create_vm ;;
    deploy)   deploy_to_vm ;;
    status)   status ;;
    teardown) teardown ;;
    all)
        log "Full deployment: build → create → deploy"
        build_image
        create_vm
        deploy_to_vm
        ;;
    *)
        echo "Usage: $0 {build|create|deploy|status|teardown|all}"
        exit 1
        ;;
esac
