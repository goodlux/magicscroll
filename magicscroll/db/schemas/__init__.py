"""Schema definitions for all MagicScroll databases."""

from .sqlite_schema import SQLiteSchema
from .milvus_schema import MilvusSchema
from .kuzu_schema import KuzuSchema
from .oxigraph_schema import OxigraphSchema

__all__ = [
    "SQLiteSchema",
    "MilvusSchema", 
    "KuzuSchema",
    "OxigraphSchema"
]
