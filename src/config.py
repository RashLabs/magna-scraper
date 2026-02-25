"""Magna scraper configuration."""

import os
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
TMP_DIR = PROJECT_ROOT / "tmp"
DB_PATH = DATA_DIR / "magna_v2.db"
ATTACHMENTS_DIR = DATA_DIR / "attachments"
COMPANY_LIST_PATH = DATA_DIR / "ta125_magna.json"

# Ensure dirs exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

# MAGNA
MAGNA_URL = "https://www.magna.isa.gov.il/"
API_RESULTS_URL = "https://www.magna.isa.gov.il/api/results"
DEFAULT_SINCE = "2024-01-01"

# Browser
SLOW_MO = 100
TIMEOUT_MS = 60_000
VIEWPORT = {"width": 1280, "height": 900}
DELAY_BETWEEN_PAGES = 1
DELAY_BETWEEN_COMPANIES = 2
DELAY_BETWEEN_DOWNLOADS = 0.5
MAX_RETRIES = 1

# Qdrant
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = "magna"
EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIMS = 1536

# Gemini
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# Chunking
CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_PAGE_WORDS = 10
MAX_PAGE_WORDS_SINGLE = 1000

# API
API_PORT = int(os.environ.get("MAGNA_API_PORT", "8400"))
