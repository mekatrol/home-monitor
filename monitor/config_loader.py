"""
config_loader.py

Robust YAML config loader for Linux and Windows with unknown encodings.
- Tries UTF-8 first, then UTF-8 with BOM, then common Windows code pages.
- Normalizes curly quotes and other smart punctuation to ASCII.
- Performs deep merge: debug overrides base recursively.
- Produces clear, actionable log messages.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple, Union
from copy import deepcopy

import yaml


# Encodings to try in order. Add others if you see them in the wild.
# 'utf-8-sig' handles BOM. 'cp1252' covers most Windows "ANSI" files.
# 'latin-1' never fails but will mis-decode some glyphs; keep last.
CANDIDATE_ENCODINGS: Tuple[str, ...] = ("utf-8", "utf-8-sig", "cp1252", "latin-1")

# Map smart punctuation and lookalikes to plain ASCII so YAML remains simple.
SMART_PUNCT_MAP = {
    "\u2018": "'",  # left single quotation mark
    "\u2019": "'",  # right single quotation mark / apostrophe
    "\u201c": '"',  # left double quotation mark
    "\u201d": '"',  # right double quotation mark
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
    "\u00a0": " ",  # no-break space
    "\u2212": "-",  # minus sign
}


# --------- PUBLIC API ----------
def load_config(
    base_path: Union[str, Path],
    debug_path: Union[str, Path],
    *,
    allow_empty: bool = True,
) -> Dict[str, Any]:
    """
    Load and deep-merge two YAML files.
    - base_path: required base config path.
    - debug_path: optional override config path; when present, it overrides base.
    - allow_empty: if False, raises FileNotFoundError if base_path does not exist.

    Returns a merged dict. Missing files are treated as {} if allow_empty=True.
    """
    base_p = Path(base_path)
    debug_p = Path(debug_path)

    base_cfg: Dict[str, Any] = {}
    debug_cfg: Dict[str, Any] = {}

    if base_p.exists():
        base_cfg = _read_yaml_file(base_p)
    elif not allow_empty:
        raise FileNotFoundError(f"Base config not found: {base_p}")

    if debug_p.exists():
        debug_cfg = _read_yaml_file(debug_p)
    else:
        logging.debug("Debug override not found: %s (treated as empty)", debug_p)

    merged = deep_merge(base_cfg, debug_cfg)
    return merged


# --------- INTERNAL IMPLEMENTATION ----------
def _read_yaml_file(path: Path) -> Dict[str, Any]:
    """
    Read a YAML file robustly across platforms and unknown encodings.
    Steps:
      1) Read the file as raw bytes to avoid locale-dependent defaults.
      2) Try a list of encodings in order. On success:
         - Normalize newlines.
         - Normalize smart punctuation to ASCII.
      3) Parse as YAML using safe_load.
    Errors:
      - Logs warnings with the exact encoding used and any fallbacks.
      - Raises yaml.YAMLError for invalid YAML syntax.
    """
    data = path.read_bytes()  # never subject to text encoding assumptions
    text, used_encoding, had_normalization = _best_effort_decode_and_normalize(data)

    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        _log_yaml_error(path, text, exc)
        raise

    if doc is None:
        logging.info("Loaded empty YAML from %s (encoding=%s)", path, used_encoding)
        return {}

    if not isinstance(doc, dict):
        logging.warning(
            "Top-level YAML in %s is %s, not dict. Returning as-is.",
            path,
            type(doc).__name__,
        )
        # Coerce to dict to keep downstream expectations simple.
        return {"_value": doc}

    logging.debug(
        "Loaded YAML %s with encoding=%s normalized_smart_punct=%s",
        path,
        used_encoding,
        had_normalization,
    )
    return doc


def _best_effort_decode_and_normalize(
    data: bytes,
) -> Tuple[str, str, bool]:
    """
    Try multiple encodings and normalize smart punctuation.
    Returns: (text, encoding_used, did_normalize)
    """
    last_error: Exception | None = None
    for enc in CANDIDATE_ENCODINGS:
        try:
            text = data.decode(enc, errors="strict")
            used = enc
            # Normalize newlines to '\n' for consistent parsing and diffing.
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            # Apply smart punctuation normalization.
            normalized, did_norm = _normalize_smart_punct(text)
            if enc != "utf-8":
                logging.warning("Decoded with fallback encoding %s", enc)
            return normalized, used, did_norm
        except UnicodeDecodeError as e:
            last_error = e
            continue

    # Final fallback: decode with 'utf-8' using replacement so we never crash,
    # then normalize. Surface a warning with context.
    logging.error(
        "Failed to decode bytes with encodings %s. Using utf-8 with replacement. "
        "This may corrupt non-ASCII characters.",
        list(CANDIDATE_ENCODINGS),
    )
    text = data.decode("utf-8", errors="replace")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized, did_norm = _normalize_smart_punct(text)
    if last_error:
        logging.debug("Last UnicodeDecodeError: %r", last_error)
    return normalized, "utf-8*replace", did_norm


def _normalize_smart_punct(text: str) -> Tuple[str, bool]:
    """
    Replace smart punctuation commonly produced by editors with plain ASCII.
    This prevents YAML quoting and scalar parsing surprises on Windows.
    """
    did = False
    for src, dst in SMART_PUNCT_MAP.items():
        if src in text:
            text = text.replace(src, dst)
            did = True
    return text, did


def deep_merge(a: Mapping[str, Any], b: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two mappings. Values from b overwrite a.
    - Dict vs dict -> recurse
    - Anything else -> overwrite with b (including None and lists)
    - Uses deepcopy to avoid aliasing bugs
    """
    out: Dict[str, Any] = deepcopy(dict(a))
    for k, v in b.items():
        av = out.get(k)
        if isinstance(av, Mapping) and isinstance(v, Mapping):
            out[k] = deep_merge(av, v)
        else:
            out[k] = deepcopy(v)
    return out


def _log_yaml_error(path: Path, text: str, exc: yaml.YAMLError) -> None:
    """
    Emit compact diagnostics for YAML syntax errors, including line and column.
    """
    # Try to extract mark info if present.
    mark_info = ""
    mark = getattr(exc, "problem_mark", None)
    if mark is not None:
        mark = exc.problem_mark  # type: ignore[attr-defined]
        mark_info = f" at line {mark.line + 1}, column {mark.column + 1}"
        # Show a snippet of the offending line.
        try:
            line = text.splitlines()[mark.line]
            logging.error(
                "YAML error in %s%s: %s\n> %s",
                path,
                mark_info,
                getattr(exc, "problem", str(exc)),
                line,
            )
            return
        except Exception:
            pass

    logging.error("YAML error in %s%s: %r", path, mark_info, exc)


# --------- EXAMPLE USAGE ----------

if __name__ == "__main__":
    # Example:
    #   python config_loader.py /etc/app/devices.yml ./devices.debug.yml
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    if len(sys.argv) < 2:
        print("Usage: python config_loader.py BASE_YML [DEBUG_YML]", file=sys.stderr)
        sys.exit(2)

    base = Path(sys.argv[1])
    debug = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("devices.debug.yml")
    cfg = load_config(base, debug, allow_empty=True)

    # Print a minimal sanity view without dumping secrets or large blobs.
    def _summarize(d: Any, depth: int = 0) -> Any:
        if depth > 3:
            return "...depth limit..."
        if isinstance(d, dict):
            return {k: _summarize(v, depth + 1) for k, v in d.items()}
        if isinstance(d, list):
            return f"[list:{len(d)}]"
        if isinstance(d, (str, int, float, bool)) or d is None:
            return d
        return f"<{type(d).__name__}>"

    logging.info("Merged config summary: %s", _summarize(cfg))
