"""Configuration management for magicscroll."""

import os
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Storage paths
    data_dir: Path = Field(
        default_factory=lambda: Path.home() / ".magicscroll",
        description="Directory for storing magicscroll data"
    )
    
    # Database settings
    milvus_path: Optional[Path] = None
    sqlite_path: Optional[Path] = None
    kuzu_path: Optional[Path] = None
    
    # API settings
    host: str = "127.0.0.1"
    port: int = 8000
    debug: bool = False
    
    model_config = {
        "env_prefix": "MAGICSCROLL_",
        "env_file": ".env"
    }
    
    def model_post_init(self, __context) -> None:
        """Set up default paths after initialization."""
        if self.milvus_path is None:
            self.milvus_path = self.data_dir / "milvus" / "milvus_lite.db"
        if self.sqlite_path is None:
            self.sqlite_path = self.data_dir / "sqlite" / "magicscroll-sqlite.db"
        if self.kuzu_path is None:
            self.kuzu_path = self.data_dir / "kuzu"
    
    def ensure_data_dir(self) -> None:
        """Create data directory if it doesn't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.milvus_path.parent.mkdir(parents=True, exist_ok=True)
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.kuzu_path.mkdir(parents=True, exist_ok=True)
    
    def get_milvus_path(self) -> Path:
        """Get the Milvus database path."""
        return self.milvus_path
    
    def get_sqlite_path(self) -> Path:
        """Get the SQLite database path."""
        return self.sqlite_path
    
    def get_kuzu_path(self) -> Path:
        """Get the Kuzu graph database path."""
        return self.kuzu_path


# Global settings instance
settings = Settings()

# Alias for backwards compatibility
Config = Settings
