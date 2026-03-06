"""
Hebrew lemmatizer — Python port of pg_hebrew_sql/hebrew_fts.

Loads Hspell dictionary data and provides text lemmatization for use in
search indexing pipelines (e.g., Qdrant BM25) without requiring PostgreSQL.

Usage:
    from hebrew_lemmatizer import HebrewLemmatizer

    lem = HebrewLemmatizer()                     # loads from bundled data
    lem = HebrewLemmatizer("/path/to/data.gz")   # or custom path

    lem.lemmatize_text("הלקוח חתם על החוזה")
    # => "לקוח חתם חוזה"

    lem.lexize("בחוזים")
    # => ["חוזה"]
"""

from __future__ import annotations

import gzip
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

_DEFAULT_DATA_PATH = Path(__file__).parent / "hspell_data.json.gz"

# Niqqud / cantillation mark ranges
_NIQQUD_RE = re.compile(r'[\u0591-\u05BD\u05BF\u05C1\u05C2\u05C4\u05C5\u05C7]')

# Tokenizer: split on non-Hebrew, non-alphanumeric, preserving quote chars inside words
_TOKEN_RE = re.compile(r'[א-תa-zA-Z0-9\'"״׳]+')


class HebrewLemmatizer:
    """Hebrew morphological lemmatizer based on Hspell dictionary."""

    def __init__(self, data_path: Optional[str] = None):
        path = Path(data_path) if data_path else _DEFAULT_DATA_PATH
        if not path.exists():
            raise FileNotFoundError(
                f"Hspell data file not found: {path}\n"
                f"Generate it with: python -m hebrew_lemmatizer.build_data"
            )
        self._load(path)

    def _load(self, path: Path) -> None:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)

        self._dictionary: Dict[str, int] = data["dictionary"]
        # lemmas: word -> [(lemma, desc_flag, prefix_type), ...]
        self._lemmas: Dict[str, List[Tuple[str, int, int]]] = {
            k: [tuple(e) for e in v] for k, v in data["lemmas"].items()
        }
        self._prefixes: Dict[str, int] = data["prefixes"]
        self._stop_words: Set[str] = set(data["stop_words"])

    @staticmethod
    def remove_niqqud(text: str) -> str:
        """Remove Hebrew vowel/cantillation marks."""
        return _NIQQUD_RE.sub("", text)

    def is_stop_word(self, word: str) -> bool:
        return word in self._stop_words

    def is_legal_prefix(self, prefix: str) -> bool:
        if not prefix:
            return True
        return prefix in self._prefixes

    def get_prefix_mask(self, prefix: str) -> Optional[int]:
        return self._prefixes.get(prefix)

    def _try_strip_prefix_quote(self, word: str) -> str:
        """Try stripping prefix before gershayim/geresh."""
        chars = list(word)
        # Double-quote (gershayim)
        for i, c in enumerate(chars):
            if c in ('"', '\u05F4'):
                if i > 0 and i < len(chars) - 1:
                    prefix = "".join(chars[:i])
                    if self.is_legal_prefix(prefix):
                        return "".join(chars[i + 1:])
                break

        # Single-quote (geresh)
        for i, c in enumerate(chars):
            if c in ("'", '\u05F3'):
                if i > 0:
                    prefix = "".join(chars[:i])
                    if self.is_legal_prefix(prefix):
                        return "".join(chars[i + 1:])
                break

        return word

    def lexize(self, token: str) -> List[str]:
        """Lemmatize a single token. Returns list of lemmas, or [cleaned_token] if unknown."""
        clean = self.remove_niqqud(token)
        clean = self._try_strip_prefix_quote(clean)

        if self.is_stop_word(clean):
            return []

        # 1. Direct lookup
        entries = self._lemmas.get(clean)
        if entries:
            return list(set(e[0] for e in entries))

        # 2. Try omitting trailing geresh
        if clean and clean[-1] in ("'", '\u05F3'):
            stripped = clean[:-1]
            entries = self._lemmas.get(stripped)
            if entries:
                return list(set(e[0] for e in entries))

        # 3. Prefix stripping
        result: List[str] = []
        chars = list(clean)
        for pref_len in range(1, len(chars)):
            if len(chars) - pref_len < 2:
                break

            prefix_str = "".join(chars[:pref_len])
            prefix_mask = self.get_prefix_mask(prefix_str)
            if prefix_mask is None:
                break

            remainder = "".join(chars[pref_len:])
            entries = self._lemmas.get(remainder)
            if not entries:
                continue

            dict_entry_prefixes = self._dictionary.get(remainder)
            if dict_entry_prefixes is None:
                continue

            for lemma, desc_flag, prefix_type in entries:
                if (dict_entry_prefixes & prefix_mask) > 0 and (prefix_type & prefix_mask) > 0:
                    if lemma not in result:
                        result.append(lemma)

        if result:
            return result

        # 4. Unknown word — return cleaned token
        return [clean]

    def lemmatize_text(self, text: str) -> str:
        """Lemmatize full text. Returns space-separated lemmas.

        Stop words are removed. Each token is replaced by its lemma(s).
        Multiple lemmas for one token are all included (bag-of-lemmas).
        """
        text_lower = text.lower()
        tokens = _TOKEN_RE.findall(text_lower)

        result_tokens: List[str] = []
        for token in tokens:
            lemmas = self.lexize(token)
            result_tokens.extend(lemmas)

        return " ".join(result_tokens)

    def lemmatize_text_dedup(self, text: str) -> str:
        """Like lemmatize_text but deduplicates lemmas while preserving order."""
        text_lower = text.lower()
        tokens = _TOKEN_RE.findall(text_lower)

        seen: Set[str] = set()
        result: List[str] = []
        for token in tokens:
            lemmas = self.lexize(token)
            for lemma in lemmas:
                if lemma not in seen:
                    seen.add(lemma)
                    result.append(lemma)

        return " ".join(result)


# Module-level singleton for convenience
_instance: Optional[HebrewLemmatizer] = None


def get_lemmatizer(data_path: Optional[str] = None) -> HebrewLemmatizer:
    """Get or create the module-level singleton lemmatizer."""
    global _instance
    if _instance is None:
        _instance = HebrewLemmatizer(data_path)
    return _instance


def lemmatize_text(text: str) -> str:
    """Convenience: lemmatize text using the singleton instance."""
    return get_lemmatizer().lemmatize_text(text)


def lexize(token: str) -> List[str]:
    """Convenience: lexize a single token using the singleton instance."""
    return get_lemmatizer().lexize(token)
