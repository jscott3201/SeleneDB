#!/usr/bin/env bash
set -euo pipefail

# Downloads EmbeddingGemma-300M for Selene vector embeddings.
# Requires a Hugging Face token with Gemma license acceptance.
# Model files are gitignored.
#
# Usage:
#   ./scripts/fetch-embeddinggemma.sh                    # prompts for token
#   ./scripts/fetch-embeddinggemma.sh --token hf_xxx     # token via flag
#   HF_TOKEN=hf_xxx ./scripts/fetch-embeddinggemma.sh    # token via env
#   ./scripts/fetch-embeddinggemma.sh --dir /custom/path # custom model dir

MODEL_DIR="data/models/embeddinggemma-300m"
REPO="google/embeddinggemma-300m"
BASE_URL="https://huggingface.co/${REPO}/resolve/main"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --token|-t)
            HF_TOKEN="$2"
            shift 2
            ;;
        --dir|-d)
            MODEL_DIR="$2"
            shift 2
            ;;
        *)
            MODEL_DIR="$1"
            shift
            ;;
    esac
done

# Try loading from .env if not set
if [ -z "${HF_TOKEN:-}" ] && [ -f .env ]; then
    HF_TOKEN=$(grep '^HF_TOKEN=' .env | sed "s/^HF_TOKEN=//" | tr -d "'" | tr -d '"')
fi

# Prompt interactively if still not set
if [ -z "${HF_TOKEN:-}" ]; then
    echo "Hugging Face token required (accept license at https://huggingface.co/${REPO})"
    printf "Enter HF token: "
    read -r HF_TOKEN
fi

if [ -z "${HF_TOKEN:-}" ]; then
    echo "ERROR: No token provided."
    exit 1
fi

echo "Downloading EmbeddingGemma-300M to ${MODEL_DIR}..."
mkdir -p "${MODEL_DIR}/2_Dense" "${MODEL_DIR}/3_Dense"

AUTH="Authorization: Bearer ${HF_TOKEN}"

# Backbone
curl -fSL -H "${AUTH}" "${BASE_URL}/model.safetensors" -o "${MODEL_DIR}/model.safetensors"
curl -fSL -H "${AUTH}" "${BASE_URL}/config.json" -o "${MODEL_DIR}/config.json"
curl -fSL -H "${AUTH}" "${BASE_URL}/tokenizer.json" -o "${MODEL_DIR}/tokenizer.json"

# Dense projection layers (768 -> 3072 -> 768)
curl -fSL -H "${AUTH}" "${BASE_URL}/2_Dense/model.safetensors" -o "${MODEL_DIR}/2_Dense/model.safetensors"
curl -fSL -H "${AUTH}" "${BASE_URL}/3_Dense/model.safetensors" -o "${MODEL_DIR}/3_Dense/model.safetensors"

echo "Done. Model size: $(du -sh "${MODEL_DIR}" | cut -f1)"
