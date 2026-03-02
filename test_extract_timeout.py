"""Standalone test: send timed-out PDFs to Libre /extract one by one and log timing.

Usage:
    conda run -n moses python Scrapers/magna-scraper/test_extract_timeout.py

Requires Libre running on 127.0.0.1:8008.
"""

import httpx
import json
import time
import sys
from pathlib import Path

LIBRE_URL = "http://127.0.0.1:8008"

TEST_FILES = [
    Path(r"C:\dev\MosesLabs\Scrapers\magna-scraper\data\attachments\2024-02-015465\2023_Form_10K_isa.pdf"),
    Path(r"C:\dev\MosesLabs\Scrapers\magna-scraper\data\attachments\2025-02-036522\Final_Prospectus_Supplement_isa.pdf"),
    Path(r"C:\dev\MosesLabs\Scrapers\magna-scraper\data\attachments\2024-02-010972\Form_4__Amir_Weiss__05_02_2024_isa.pdf"),
]


def test_one(filepath: Path):
    if not filepath.exists():
        print(f"  SKIP — file not found: {filepath}")
        return

    size_kb = filepath.stat().st_size / 1024
    print(f"  File: {filepath.name}  ({size_kb:.0f} KB)")

    start = time.perf_counter()
    try:
        with open(filepath, "rb") as f:
            resp = httpx.post(
                f"{LIBRE_URL}/extract",
                files={"file": (filepath.name, f, "application/pdf")},
                timeout=600.0,
            )
        elapsed = time.perf_counter() - start

        result = resp.json()
        success = result.get("success")
        error = result.get("error")
        pages = result.get("pages", [])
        stats = result.get("stats", {})

        print(f"  HTTP {resp.status_code}  |  {elapsed:.1f}s  |  success={success}  |  pages={len(pages)}")
        if error:
            print(f"  error: {json.dumps(error, indent=2, ensure_ascii=False)}")
        if stats:
            print(f"  stats: {json.dumps(stats)}")

    except httpx.TimeoutException as e:
        elapsed = time.perf_counter() - start
        print(f"  TIMEOUT after {elapsed:.1f}s: {e}")
    except httpx.ConnectError:
        print(f"  Connection refused — is Libre running on {LIBRE_URL}?")
        sys.exit(1)
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"  ERROR after {elapsed:.1f}s: {type(e).__name__}: {e}")


def main():
    print(f"Testing {len(TEST_FILES)} files against {LIBRE_URL}/extract\n")
    for i, fp in enumerate(TEST_FILES, 1):
        print(f"[{i}/{len(TEST_FILES)}]")
        test_one(fp)
        print()


if __name__ == "__main__":
    main()
