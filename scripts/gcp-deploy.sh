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
        --no-address \
        --tags=selene-server

    log "VM created. No public IP — access via Tailscale only."
}

# ── Deploy to VM ─────────────────────────────────────────────────────────
deploy_to_vm() {
    log "Deploying SeleneDB to ${VM_NAME}..."

    # Upload compose and config files
    gcloud compute scp \
        docker-compose.gpu.yml selene.cloud.toml \
        "${VM_NAME}:~/" --zone="${ZONE}"

    # Setup and start services
    gcloud compute ssh "${VM_NAME}" --zone="${ZONE}" --command="
        set -e

        # Install NVIDIA GPU drivers (Container-Optimized OS)
        if ! nvidia-smi &>/dev/null; then
            echo '==> Installing NVIDIA drivers...'
            sudo cos-extensions install gpu
            sudo mount --bind /var/lib/nvidia /var/lib/nvidia
            sudo mount -o remount,exec /var/lib/nvidia
        fi

        # Format and mount data disk if needed
        if ! mountpoint -q /mnt/disks/selene-data; then
            echo '==> Mounting data disk...'
            sudo mkdir -p /mnt/disks/selene-data
            DISK_DEV=\$(readlink -f /dev/disk/by-id/google-selene-data)
            if ! blkid \"\${DISK_DEV}\" &>/dev/null; then
                sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,discard \"\${DISK_DEV}\"
            fi
            sudo mount -o discard,defaults \"\${DISK_DEV}\" /mnt/disks/selene-data
            sudo chmod 777 /mnt/disks/selene-data
        fi

        # Authenticate to Artifact Registry
        docker-credential-gcr configure-docker --registries=${REGION}-docker.pkg.dev

        # Pull latest image
        docker pull ${AR_IMAGE}:gpu-latest

        # Start services
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

# ── Main ─────────────────────────────────────────────────────────────────
case "${1:-all}" in
    build)    build_image ;;
    create)   create_vm ;;
    deploy)   deploy_to_vm ;;
    teardown) teardown ;;
    all)
        log "Full deployment: build → create → deploy"
        build_image
        create_vm
        deploy_to_vm
        ;;
    *)
        echo "Usage: $0 {build|create|deploy|teardown|all}"
        exit 1
        ;;
esac
