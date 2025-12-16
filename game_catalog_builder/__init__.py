"""Game Catalog Builder - Enrich video game catalogs with metadata from multiple APIs."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("game-catalog-builder")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.1.0"
