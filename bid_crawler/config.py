"""Configuration dataclasses and loaders."""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import yaml


@dataclass
class RateLimits:
    default_delay: float = 1.0
    bidnet_delay: float = 2.0
    sam_gov_delay: float = 0.5


@dataclass
class CrawlerConfig:
    db_path: str = "data/bids.duckdb"
    log_level: str = "INFO"
    export_dir: str = "data/exports"
    rate_limits: RateLimits = field(default_factory=RateLimits)
    fresh_threshold_hours: int = 20

    @classmethod
    def from_yaml(cls, path: str | Path) -> "CrawlerConfig":
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        rl_data = data.pop("rate_limits", {})
        cfg = cls(**{k: v for k, v in data.items() if k != "rate_limits"})
        if rl_data:
            cfg.rate_limits = RateLimits(**rl_data)
        return cfg


@dataclass
class SourceConfig:
    id: str
    source_type: str
    enabled: bool = True
    base_url: str = ""
    search_url: str = ""
    page_size: int = 50
    max_pages: int = 10
    delay: float = 1.0
    env_key: Optional[str] = None
    env_email: Optional[str] = None
    # Extra fields stored as extras dict
    extras: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "SourceConfig":
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        extras = {k: v for k, v in data.items() if k not in known}
        known_data = {k: v for k, v in data.items() if k in known}
        cfg = cls(**known_data)
        cfg.extras = extras
        return cfg

    def api_key(self) -> Optional[str]:
        if self.env_key:
            return os.environ.get(self.env_key)
        return None

    def api_email(self) -> Optional[str]:
        if self.env_email:
            return os.environ.get(self.env_email)
        return None


@dataclass
class CriteriaConfig:
    keywords: list[str] = field(default_factory=list)
    naics_prefixes: list[str] = field(default_factory=list)
    counties: list[str] = field(default_factory=list)
    min_value: float = 50000.0

    @classmethod
    def from_yaml(cls, path: str | Path) -> "CriteriaConfig":
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return cls(
            keywords=data.get("keywords", []),
            naics_prefixes=data.get("naics_prefixes", []),
            counties=data.get("counties", []),
            min_value=float(data.get("min_value", 50000)),
        )


def load_configs(project_root: Path) -> tuple[CrawlerConfig, CriteriaConfig, list[SourceConfig]]:
    """Load all config files from the project root."""
    settings_path = project_root / "config" / "settings.yaml"
    criteria_path = project_root / "config" / "criteria.yaml"
    sources_dir = project_root / "config" / "sources"

    crawler_cfg = CrawlerConfig.from_yaml(settings_path)
    criteria_cfg = CriteriaConfig.from_yaml(criteria_path)

    source_cfgs = []
    for yaml_file in sorted(sources_dir.glob("*.yaml")):
        sc = SourceConfig.from_yaml(yaml_file)
        source_cfgs.append(sc)

    return crawler_cfg, criteria_cfg, source_cfgs
