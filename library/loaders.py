from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)


_QUOTING_MAP = {
    "minimal": csv.QUOTE_MINIMAL,
    "all": csv.QUOTE_ALL,
    "none": csv.QUOTE_NONE,
}


class LoaderError(RuntimeError):
    """Exception raised when a dataset cannot be accessed."""


def _resolve_base_path(config: Dict[str, Any]) -> Path:
    source_cfg = config.get("source", {})
    kind = source_cfg.get("kind", "file").lower()
    if kind == "file":
        base = source_cfg.get("base_path", "")
        return Path(base)
    if kind == "http":
        base_url = source_cfg.get("http_base", "")
        if not base_url:
            raise LoaderError("HTTP base URL is not configured")
        return Path(base_url)
    if kind == "sharepoint":
        sharepoint_cfg = source_cfg.get("sharepoint", {})
        site_url = sharepoint_cfg.get("site_url")
        if not site_url:
            raise LoaderError("SharePoint site_url must be configured")
        return Path(site_url)
    raise LoaderError(f"Unsupported source kind: {kind}")


def _build_path(path_key: str, config: Dict[str, Any]) -> Path:
    files_cfg = config.get("files", {})
    try:
        rel_path = files_cfg[path_key]
    except KeyError as exc:  # pragma: no cover - defensive
        raise LoaderError(f"Path key '{path_key}' is not defined in config") from exc
    base_path = _resolve_base_path(config)
    if base_path and not str(base_path).startswith("http"):
        return (base_path / rel_path).resolve()
    return Path(rel_path)


def _encoding_candidates(io_cfg: Dict[str, Any]) -> list[str]:
    primary = io_cfg.get("encoding_in", "utf8")
    fallbacks_cfg = io_cfg.get("encoding_fallbacks")
    if fallbacks_cfg is None:
        candidates = [primary, "cp1252", "latin1"]
    else:
        if isinstance(fallbacks_cfg, str):
            fallback_list = [fallbacks_cfg]
        else:
            fallback_list = list(fallbacks_cfg)
        candidates = [primary, *fallback_list]
    seen: set[str] = set()
    unique_candidates: list[str] = []
    for encoding in candidates:
        if not encoding:
            continue
        if encoding in seen:
            continue
        seen.add(encoding)
        unique_candidates.append(encoding)
    return unique_candidates


def _read_kwargs(config: Dict[str, Any], *, encoding: str) -> Dict[str, Any]:
    io_cfg = config.get("io", {})
    quoting = io_cfg.get("quoting", "minimal").lower()
    if quoting not in _QUOTING_MAP:
        raise LoaderError(f"Unsupported quoting option: {quoting}")
    kwargs: Dict[str, Any] = {
        "encoding": encoding,
        "sep": io_cfg.get("delimiter", ","),
        "na_values": io_cfg.get("na_values", ["", "NA", "null", "None"]),
        "keep_default_na": False,
        "dtype": None,
        "low_memory": False,
    }
    encoding_errors = io_cfg.get("encoding_errors")
    if encoding_errors:
        kwargs["encoding_errors"] = encoding_errors
    kwargs["quoting"] = _QUOTING_MAP[quoting]
    return kwargs


def read_csv(path_key: str, config: Dict[str, Any]) -> pd.DataFrame:
    """Read a CSV identified by *path_key* using *config* options."""

    path = _build_path(path_key, config)
    io_cfg = config.get("io", {})
    encodings = _encoding_candidates(io_cfg)
    source_kind = config.get("source", {}).get("kind", "file").lower()
    logger.info("Loading CSV", extra={"path_key": path_key, "resolved_path": str(path)})

    last_error: UnicodeDecodeError | None = None

    if source_kind == "file":
        if not path.exists():
            raise LoaderError(f"File not found: {path}")
        for encoding in encodings:
            try:
                kwargs = _read_kwargs(config, encoding=encoding)
                return pd.read_csv(path, **kwargs)
            except UnicodeDecodeError as exc:
                last_error = exc
                logger.warning(
                    "Failed to decode CSV",
                    extra={
                        "path_key": path_key,
                        "resolved_path": str(path),
                        "encoding": encoding,
                    },
                )
        assert last_error is not None  # for type checkers
        raise last_error

    if source_kind == "http":
        full_url = os.path.join(str(path))
        for encoding in encodings:
            try:
                kwargs = _read_kwargs(config, encoding=encoding)
                return pd.read_csv(full_url, **kwargs)
            except UnicodeDecodeError as exc:
                last_error = exc
                logger.warning(
                    "Failed to decode CSV", extra={"url": full_url, "encoding": encoding}
                )
        assert last_error is not None
        raise last_error

    if source_kind == "sharepoint":
        # Actual SharePoint access is out of scope for unit tests.
        raise LoaderError("SharePoint loading is not implemented in this environment")

    raise LoaderError(f"Unsupported source kind: {source_kind}")


def write_csv(df: pd.DataFrame, path: str | Path, config: Dict[str, Any]) -> None:
    """Write *df* to *path* applying configuration controlled options."""

    io_cfg = config.get("io", {})
    outputs_cfg = config.get("outputs", {})
    quoting = io_cfg.get("quoting", "minimal").lower()
    if quoting not in _QUOTING_MAP:
        raise LoaderError(f"Unsupported quoting option: {quoting}")
    line_terminator = outputs_cfg.get("line_terminator", "\n")
    encoding = io_cfg.get("encoding_out", io_cfg.get("encoding_in", "utf8"))
    delimiter = io_cfg.get("delimiter", ",")

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Writing CSV", extra={"path": str(output_path)})
    df.to_csv(
        output_path,
        index=False,
        sep=delimiter,
        encoding=encoding,
        quoting=_QUOTING_MAP[quoting],
        lineterminator=line_terminator,
    )
