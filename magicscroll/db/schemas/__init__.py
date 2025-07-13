"""Schema definitions for all MagicScroll databases."""

from .sqlite_schema import SQLiteSchema
from .milvus_schema import MilvusSchema
from .kuzu_schema import KuzuSchema

__all__ = [
    "SQLiteSchema",
    "MilvusSchema", 
    "KuzuSchema"
]
