#!/usr/bin/env bash
# ==============================================================================
# UrbanPulse Technical Documentation PDF Generation Script
# ==============================================================================
# This script compiles the project's Markdown reports to print-ready PDF files.
# It automatically renders any embedded Mermaid diagrams to SVG graphics
# using mermaid-cli before compiling the final PDFs with md-to-pdf.
#
# Requirements:
#   - Node.js / npm / npx installed on the system
# ==============================================================================

set -euo pipefail

# Ensure we are in the project root directory
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${BASE_DIR}"

echo "======================================================================"
echo "      UrbanPulse — Technical Report PDF Generation Utility"
echo "======================================================================"

# Create temporary Puppeteer config to disable sandbox (required in headless/container environments)
TEMP_PUPPETEER_CONFIG=".puppeteer-config-temp.json"
cat <<EOF > "${TEMP_PUPPETEER_CONFIG}"
{
  "args": ["--no-sandbox", "--disable-setuid-sandbox"]
}
EOF

# Define documents to compile
DOCS=(
  "docs/UrbanPulse_Technical_Report.md"
)

# 1. Compile Mermaid Diagrams to SVGs and update markdown references
echo -e "\n1. Compiling embedded Mermaid diagrams to SVG..."
for doc in "${DOCS[@]}"; do
  if [ -f "${doc}" ]; then
    echo "   Processing diagrams in ${doc}..."
    # Run mermaid-cli. This will automatically extract ```mermaid blocks,
    # generate SVG files in the document directory, and update the markdown file.
    npx --yes @mermaid-js/mermaid-cli \
      -p "${TEMP_PUPPETEER_CONFIG}" \
      -i "${doc}" \
      -o "${doc}" || echo "   (No new diagrams found or compiled for ${doc})"
  else
    echo "   [Warning] File not found: ${doc}"
  fi
done

# 2. Compile Markdown files to PDF
echo -e "\n2. Compiling Markdown reports to PDF..."
mkdir -p docs/pdf

for doc in "${DOCS[@]}"; do
  if [ -f "${doc}" ]; then
    filename=$(basename "${doc}" .md)
    echo "   Compiling ${doc} to PDF..."
    
    npx --yes md-to-pdf "${doc}" \
      --launch-options "{\"args\": [\"--no-sandbox\", \"--disable-setuid-sandbox\"]}"
      
    pdf_path="docs/${filename}.pdf"
    if [ -f "${pdf_path}" ]; then
      mv "${pdf_path}" docs/pdf/
      echo "   ✓ Successfully compiled: docs/pdf/${filename}.pdf"
    else
      echo "   ✗ Failed to generate PDF for ${doc}"
    fi
  fi
done

# Copy to root directory
cp docs/pdf/UrbanPulse_Technical_Report.pdf Report.pdf

# Clean up temporary configuration
rm -f "${TEMP_PUPPETEER_CONFIG}"

echo -e "\n======================================================================"
echo "  All reports successfully compiled and stored in: docs/pdf/"
echo "======================================================================"
