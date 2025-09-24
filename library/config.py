from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict

import yaml

LOGGER = logging.getLogger(__name__)

_BACKSLASH_KEYS_PASSTHROUGH = {"line_terminator"}
_BACKSLASH_PATTERN = re.compile(
    r"(?m)^(?P<indent>\s*)(?P<key>[^:\n]+):\s*\"(?P<value>[^\"\n]*\\[^\"\n]*)\""
)
_VALID_ESCAPE_CHARS = set("abfnrtv0-7xuU\"'\\NLP")


def load_config(path: Path) -> Dict[str, Any]:
    """Load a YAML configuration file with Windows path safeguards."""

    text = path.read_text(encoding="utf-8")
    try:
        return _parse_yaml(text)
    except yaml.YAMLError as error:
        if not _is_unknown_escape(error):
            raise

        LOGGER.debug("Retrying config parse after escaping backslashes", exc_info=error)
        escaped_text = _escape_windows_paths(text)
        try:
            return _parse_yaml(escaped_text)
        except yaml.YAMLError as retry_error:
            raise ValueError(
                "Failed to parse configuration file. "
                "Ensure Windows paths use forward slashes or escaped backslashes."
            ) from retry_error


def _parse_yaml(text: str) -> Dict[str, Any]:
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise TypeError("Configuration root must be a mapping")
    return data


def _is_unknown_escape(error: yaml.YAMLError) -> bool:
    message = getattr(error, "problem", None) or str(error)
    return "unknown escape character" in message


def _escape_windows_paths(text: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        key = match.group("key").strip()
        if key in _BACKSLASH_KEYS_PASSTHROUGH:
            return match.group(0)

        value = match.group("value")
        if not _should_escape_value(value):
            return match.group(0)

        escaped_value = value.replace("\\", "\\\\")
        return f'{match.group("indent")}{key}: "{escaped_value}"'

    return _BACKSLASH_PATTERN.sub(replacer, text)


def _should_escape_value(value: str) -> bool:
    if "\\" not in value:
        return False

    escape_markers = re.findall(r"\\(.)", value)
    if any(marker not in _VALID_ESCAPE_CHARS for marker in escape_markers):
        return True

    if value.startswith("\\\\") or re.match(r"(?i)[A-Z]:\\", value):
        return True

    return False


__all__ = ["load_config"]

