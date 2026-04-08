#!/usr/bin/env bash
set -euo pipefail

# Downloads EmbeddingGemma-300M for Selene vector embeddings.
# Requires a Hugging Face token with Gemma license acceptance.
# Model files are gitignored.
#
# Usage:
#   ./scripts/fetch-embeddinggemma.sh                    # base model (bf16, 1.1GB)
#   ./scripts/fetch-embeddinggemma.sh --qat-q8           # QAT int8 safetensors
#   ./scripts/fetch-embeddinggemma.sh --qat-q4           # QAT int4 safetensors
#   ./scripts/fetch-embeddinggemma.sh --gguf-q8          # GGUF Q8_0 (~329MB, 85% smaller)
#   ./scripts/fetch-embeddinggemma.sh --token hf_xxx     # token via flag
#   HF_TOKEN=hf_xxx ./scripts/fetch-embeddinggemma.sh    # token via env
#   ./scripts/fetch-embeddinggemma.sh --dir /custom/path # custom model dir
#
# Model variants:
#   base     google/embeddinggemma-300m                       (bf16 safetensors, ~1.1GB, default)
#   qat-q8   google/embeddinggemma-300m-qat-q8_0-unquantized  (QAT int8 safetensors, ~1.1GB)
#   qat-q4   google/embeddinggemma-300m-qat-q4_0-unquantized  (QAT int4 safetensors, ~1.1GB)
#   gguf-q8  ggml-org/embeddinggemma-300M-GGUF                (Q8_0 GGUF, ~329MB, needs Dense from base)
#
# GGUF variants load the backbone from a quantized GGUF file (~85% memory
# savings). Dense projection layers (2_Dense, 3_Dense) are always needed
# from the base model. The script downloads both automatically.

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
        --gguf-q8)
            VARIANT="gguf-q8"
            shift
            ;;
        *)
            MODEL_DIR="$1"
            shift
            ;;
    esac
done

# Select repository based on variant
GGUF_REPO=""
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
    gguf-q8)
        REPO="google/embeddinggemma-300m"
        GGUF_REPO="ggml-org/embeddinggemma-300M-GGUF"
        LABEL="EmbeddingGemma-300M (GGUF Q8_0, ~329MB backbone)"
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

if [ -n "${GGUF_REPO}" ]; then
    # GGUF variant: download quantized backbone from ggml-org
    GGUF_URL="https://huggingface.co/${GGUF_REPO}/resolve/main/embeddinggemma-300M-Q8_0.gguf"
    echo "Downloading GGUF backbone from ${GGUF_REPO}..."
    curl -fSL -H "${AUTH}" "${GGUF_URL}" -o "${MODEL_DIR}/model.gguf"
    # Config and tokenizer from the base model (GGUF doesn't include these)
    curl -fSL -H "${AUTH}" "${BASE_URL}/config.json" -o "${MODEL_DIR}/config.json"
    curl -fSL -H "${AUTH}" "${BASE_URL}/tokenizer.json" -o "${MODEL_DIR}/tokenizer.json"
else
    # Safetensors variant: download backbone weights
    curl -fSL -H "${AUTH}" "${BASE_URL}/model.safetensors" -o "${MODEL_DIR}/model.safetensors"
    curl -fSL -H "${AUTH}" "${BASE_URL}/config.json" -o "${MODEL_DIR}/config.json"
    curl -fSL -H "${AUTH}" "${BASE_URL}/tokenizer.json" -o "${MODEL_DIR}/tokenizer.json"
fi

# Dense projection layers always from the base model (768 -> 3072 -> 768, ~9.4MB total)
DENSE_URL="https://huggingface.co/google/embeddinggemma-300m/resolve/main"
curl -fSL -H "${AUTH}" "${DENSE_URL}/2_Dense/model.safetensors" -o "${MODEL_DIR}/2_Dense/model.safetensors"
curl -fSL -H "${AUTH}" "${DENSE_URL}/3_Dense/model.safetensors" -o "${MODEL_DIR}/3_Dense/model.safetensors"

echo "Done. Variant: ${VARIANT}, size: $(du -sh "${MODEL_DIR}" | cut -f1)"
