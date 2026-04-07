#!/usr/bin/env bash
set -euo pipefail

# Downloads EmbeddingGemma-300M for Selene vector embeddings.
# Requires a Hugging Face token with Gemma license acceptance.
# Model files are gitignored.
#
# Usage:
#   ./scripts/fetch-embeddinggemma.sh                    # base model (bf16, 1.1GB)
#   ./scripts/fetch-embeddinggemma.sh --qat-q8           # QAT int8 variant
#   ./scripts/fetch-embeddinggemma.sh --qat-q4           # QAT int4 variant
#   ./scripts/fetch-embeddinggemma.sh --token hf_xxx     # token via flag
#   HF_TOKEN=hf_xxx ./scripts/fetch-embeddinggemma.sh    # token via env
#   ./scripts/fetch-embeddinggemma.sh --dir /custom/path # custom model dir
#
# Model variants:
#   base     google/embeddinggemma-300m                     (bf16, ~1.1GB, default)
#   qat-q8   google/embeddinggemma-300m-qat-q8_0-unquantized (QAT int8, ~1.1GB, near-lossless)
#   qat-q4   google/embeddinggemma-300m-qat-q4_0-unquantized (QAT int4, ~1.1GB, optimized for quantization)
#
# QAT (Quantization-Aware Training) variants load identically to the base
# model (same safetensors format, same architecture). They are trained to
# maintain quality when later quantized to int8/int4.

MODEL_DIR="data/models/embeddinggemma-300m"
VARIANT="base"

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
        --qat-q8)
            VARIANT="qat-q8"
            shift
            ;;
        --qat-q4)
            VARIANT="qat-q4"
            shift
            ;;
        *)
            MODEL_DIR="$1"
            shift
            ;;
    esac
done

# Select repository based on variant
case "${VARIANT}" in
    base)
        REPO="google/embeddinggemma-300m"
        LABEL="EmbeddingGemma-300M (base, bf16)"
        ;;
    qat-q8)
        REPO="google/embeddinggemma-300m-qat-q8_0-unquantized"
        LABEL="EmbeddingGemma-300M (QAT int8)"
        ;;
    qat-q4)
        REPO="google/embeddinggemma-300m-qat-q4_0-unquantized"
        LABEL="EmbeddingGemma-300M (QAT int4)"
        ;;
esac

BASE_URL="https://huggingface.co/${REPO}/resolve/main"

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

echo "Downloading ${LABEL} to ${MODEL_DIR}..."
mkdir -p "${MODEL_DIR}/2_Dense" "${MODEL_DIR}/3_Dense"

AUTH="Authorization: Bearer ${HF_TOKEN}"

# Backbone
curl -fSL -H "${AUTH}" "${BASE_URL}/model.safetensors" -o "${MODEL_DIR}/model.safetensors"
curl -fSL -H "${AUTH}" "${BASE_URL}/config.json" -o "${MODEL_DIR}/config.json"
curl -fSL -H "${AUTH}" "${BASE_URL}/tokenizer.json" -o "${MODEL_DIR}/tokenizer.json"

# Dense projection layers (768 -> 3072 -> 768)
curl -fSL -H "${AUTH}" "${BASE_URL}/2_Dense/model.safetensors" -o "${MODEL_DIR}/2_Dense/model.safetensors"
curl -fSL -H "${AUTH}" "${BASE_URL}/3_Dense/model.safetensors" -o "${MODEL_DIR}/3_Dense/model.safetensors"

echo "Done. Variant: ${VARIANT}, size: $(du -sh "${MODEL_DIR}" | cut -f1)"
