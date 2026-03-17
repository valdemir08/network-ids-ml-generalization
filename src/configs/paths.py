from pathlib import Path

# raiz
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# outros
DATASETS_DIR = PROJECT_ROOT / "datasets"
INTERMEDIATE_DATA_DIR = DATASETS_DIR / "intermediate"
PROCESSED_DATA_DIR = DATASETS_DIR / "processed"