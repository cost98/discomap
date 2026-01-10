"""Downloaders for different data sources."""

from abc import ABC, abstractmethod
from pathlib import Path

from src.services.downloaders.url_downloader import URLDownloader


class BaseDownloader(ABC):
    """Abstract base class for data downloaders."""

    @abstractmethod
    async def download(self, *args, **kwargs) -> Path | list[Path]:
        """
        Download data and return path(s) to downloaded file(s).
        
        Returns:
            Path or list of Paths to downloaded files
        """
        pass


__all__ = ["BaseDownloader", "URLDownloader"]
