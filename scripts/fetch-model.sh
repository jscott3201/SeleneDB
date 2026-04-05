#!/usr/bin/env bash
set -euo pipefail

# Downloads all-MiniLM-L6-v2 sentence embedding model for Selene vector embeddings.
# Run once during development setup. Model files are gitignored.
#
# Usage:
#   ./scripts/fetch-model.sh                              # default: data/models/all-MiniLM-L6-v2
#   ./scripts/fetch-model.sh /var/selene/models/my-model  # custom path

MODEL_DIR="${1:-data/models/all-MiniLM-L6-v2}"
BASE_URL="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main"

echo "Downloading all-MiniLM-L6-v2 to ${MODEL_DIR}..."
mkdir -p "${MODEL_DIR}"

curl -fSL "${BASE_URL}/model.safetensors" -o "${MODEL_DIR}/model.safetensors"
curl -fSL "${BASE_URL}/tokenizer.json" -o "${MODEL_DIR}/tokenizer.json"
curl -fSL "${BASE_URL}/config.json" -o "${MODEL_DIR}/config.json"

echo "Done. Model files: $(du -sh "${MODEL_DIR}" | cut -f1)"
echo ""
echo "Set model path via:"
echo "  export SELENE_MODEL_PATH=${MODEL_DIR}"
echo "  or in selene.toml: [vector] model_path = \"${MODEL_DIR}\""
