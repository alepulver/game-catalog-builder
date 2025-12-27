from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..utils import load_credentials
from .provider_clients import build_provider_clients


@dataclass(frozen=True)
class PipelineContext:
    cache_dir: Path
    credentials_path: Path
    sources: list[str]

    def credentials(self) -> dict[str, Any]:
        return load_credentials(self.credentials_path)

    def build_clients(self) -> dict[str, object]:
        return build_provider_clients(
            sources=set(self.sources),
            credentials=self.credentials(),
            cache_dir=self.cache_dir,
        )
