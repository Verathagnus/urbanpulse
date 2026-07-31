#!/usr/bin/env bash
# ============================================================
# UrbanPulse — Post-Testing Cleanup Script
# ============================================================
# Removes all runtime artifacts generated during testing while
# preserving source code, static data (CSVs), and documentation.
#
# Usage:
#   chmod +x cleanup.sh
#   ./cleanup.sh          # interactive (asks before deleting)
#   ./cleanup.sh --force  # no prompts, delete everything
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FORCE=false
if [[ "${1:-}" == "--force" || "${1:-}" == "-f" ]]; then
    FORCE=true
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN} UrbanPulse — Post-Testing Cleanup${NC}"
echo -e "${CYAN}============================================================${NC}"
echo ""

# Collect what needs to be cleaned
ITEMS_TO_CLEAN=()

# 1. Python bytecode caches
PYCACHE_DIRS=$(find . -type d -name "__pycache__" 2>/dev/null || true)
if [[ -n "$PYCACHE_DIRS" ]]; then
    ITEMS_TO_CLEAN+=("__pycache__ directories")
    echo -e "${YELLOW}[1] __pycache__ directories:${NC}"
    echo "$PYCACHE_DIRS" | sed 's/^/     /'
fi

# 2. Spark/Flink checkpoints
if [[ -d "urbanpulse/data/checkpoints" ]]; then
    ITEMS_TO_CLEAN+=("Spark/Flink checkpoints")
    CKPT_SIZE=$(du -sh urbanpulse/data/checkpoints 2>/dev/null | cut -f1 || echo "unknown")
    echo -e "${YELLOW}[2] Spark/Flink checkpoints:${NC}  urbanpulse/data/checkpoints/ (${CKPT_SIZE})"
fi

# 3. Ward energy Parquet output
if [[ -d "urbanpulse/data/ward_energy_parquet" ]]; then
    ITEMS_TO_CLEAN+=("Ward energy Parquet files")
    PARQ_SIZE=$(du -sh urbanpulse/data/ward_energy_parquet 2>/dev/null | cut -f1 || echo "unknown")
    echo -e "${YELLOW}[3] Ward energy Parquet output:${NC}  urbanpulse/data/ward_energy_parquet/ (${PARQ_SIZE})"
fi

# 4. Spark metastore (Derby) — created if Spark Hive is used
DERBY_ITEMS=""
[[ -d "metastore_db" ]] && DERBY_ITEMS+=" metastore_db/"
[[ -f "derby.log" ]] && DERBY_ITEMS+=" derby.log"
[[ -d "spark-warehouse" ]] && DERBY_ITEMS+=" spark-warehouse/"
if [[ -n "$DERBY_ITEMS" ]]; then
    ITEMS_TO_CLEAN+=("Spark metastore artifacts")
    echo -e "${YELLOW}[4] Spark metastore artifacts:${NC} $DERBY_ITEMS"
fi

# 5. .pyc files outside __pycache__
STRAY_PYC=$(find . -name "*.pyc" -not -path "./__pycache__/*" -not -path "*/__pycache__/*" 2>/dev/null || true)
if [[ -n "$STRAY_PYC" ]]; then
    ITEMS_TO_CLEAN+=("Stray .pyc files")
    echo -e "${YELLOW}[5] Stray .pyc files:${NC}"
    echo "$STRAY_PYC" | sed 's/^/     /'
fi

# 6. Faust/Kafka Streams RocksDB tables
FAUST_TABLES=$(find . -type d -name "*-data" -path "*/faust/*" 2>/dev/null || true)
ROCKSDB_DIRS=$(find . -type d -name "rocksdb" 2>/dev/null || true)
FAUST_ALL="${FAUST_TABLES}${ROCKSDB_DIRS}"
if [[ -n "$FAUST_ALL" ]]; then
    ITEMS_TO_CLEAN+=("Faust/RocksDB state tables")
    echo -e "${YELLOW}[6] Faust/RocksDB state directories:${NC}"
    echo "$FAUST_ALL" | sed 's/^/     /'
fi

# 7. Temporary/lock files
TEMP_FILES=$(find . \( -name "*.tmp" -o -name "*.lock" -o -name ".~lock.*" -o -name "*.swp" \) 2>/dev/null || true)
if [[ -n "$TEMP_FILES" ]]; then
    ITEMS_TO_CLEAN+=("Temporary/lock files")
    echo -e "${YELLOW}[7] Temporary files:${NC}"
    echo "$TEMP_FILES" | sed 's/^/     /'
fi

# 8. Old submission ZIP
if [[ -f "UrbanPulse_Submission.zip" ]]; then
    ITEMS_TO_CLEAN+=("Old submission ZIP")
    ZIP_SIZE=$(du -sh UrbanPulse_Submission.zip 2>/dev/null | cut -f1 || echo "unknown")
    echo -e "${YELLOW}[8] Old submission ZIP:${NC}  UrbanPulse_Submission.zip (${ZIP_SIZE})"
fi

echo ""

# Nothing to clean?
if [[ ${#ITEMS_TO_CLEAN[@]} -eq 0 ]]; then
    echo -e "${GREEN}✅ Workspace is already clean! Nothing to remove.${NC}"
    exit 0
fi

echo -e "${CYAN}Found ${#ITEMS_TO_CLEAN[@]} categories of artifacts to clean.${NC}"
echo ""

# Confirm
if [[ "$FORCE" != true ]]; then
    read -rp "Proceed with cleanup? (y/N): " CONFIRM
    if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
        echo -e "${RED}Aborted.${NC}"
        exit 1
    fi
fi

echo ""
echo -e "${CYAN}Cleaning...${NC}"

# Execute cleanup
# 1. __pycache__
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Removed __pycache__ directories"

# 2. Checkpoints
rm -rf urbanpulse/data/checkpoints 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Removed Spark/Flink checkpoints"

# 3. Parquet output
rm -rf urbanpulse/data/ward_energy_parquet 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Removed ward energy Parquet output"

# 4. Spark metastore
rm -rf metastore_db spark-warehouse 2>/dev/null || true
rm -f derby.log 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Removed Spark metastore artifacts"

# 5. Stray .pyc files
find . -name "*.pyc" -delete 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Removed stray .pyc files"

# 6. Faust/RocksDB state
find . -type d -name "*-data" -path "*/faust/*" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "rocksdb" -exec rm -rf {} + 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Removed Faust/RocksDB state directories"

# 7. Temporary files
find . \( -name "*.tmp" -o -name "*.lock" -o -name ".~lock.*" -o -name "*.swp" \) -delete 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Removed temporary/lock files"

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN} ✅ Cleanup complete! Workspace is ready for fresh testing.${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo -e "Preserved files:"
echo -e "  ${CYAN}•${NC} Source code (urbanpulse/**/*.py, streamlit_app.py)"
echo -e "  ${CYAN}•${NC} Static data (route_schedule.csv, zone_profile.csv)"
echo -e "  ${CYAN}•${NC} Documentation (docs/, README.md)"
echo -e "  ${CYAN}•${NC} Docker config (docker-compose.yml, cluster_setup.sh)"
echo -e "  ${CYAN}•${NC} Dependencies (requirements.txt, conda_env.yml)"
