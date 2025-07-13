"""Milvus schema definitions for MagicScroll."""

import logging
from pathlib import Path
from typing import Dict, List

from pymilvus import MilvusClient

logger = logging.getLogger(__name__)


class MilvusSchema:
    """Milvus vector database schema management."""
    
    @staticmethod
    def create_collections(milvus_path: Path) -> bool:
        """Create Milvus collections for vector storage."""
        try:
            # Ensure directory exists
            milvus_path.parent.mkdir(parents=True, exist_ok=True)
            
            client = MilvusClient(str(milvus_path))
            
            # Drop old collection if it exists (migration from old schema)
            collections = client.list_collections()
            if "conversations" in collections:
                client.drop_collection("conversations")
                logger.info("🗑️ Dropped old 'conversations' collection")
            
            # Create ms_entries collection (conversation chunks for semantic search)
            if "ms_entries" not in collections:
                client.create_collection(
                    collection_name="ms_entries",
                    dimension=384,  # sentence-transformers/all-MiniLM-L6-v2 dimension
                    metric_type="COSINE",
                    index_type="FLAT"
                )
                logger.info("✅ Created 'ms_entries' collection (384D vectors)")
            else:
                logger.info("ℹ️ 'ms_entries' collection already exists")
            
            # Future collections can be added here:
            # - ms_artifacts (for files, images, code)
            # - ms_summaries (for conversation summaries)
            # - ms_documents (for document chunks)
            
            client.close()
            logger.info("✅ Milvus collections created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Milvus collection creation failed: {e}")
            return False
    
    @staticmethod
    def drop_all_collections(milvus_path: Path) -> bool:
        """Drop all Milvus collections."""
        try:
            if not milvus_path.exists():
                logger.info("ℹ️ Milvus database does not exist")
                return True
                
            client = MilvusClient(str(milvus_path))
            collections = client.list_collections()
            
            for collection in collections:
                client.drop_collection(collection)
                logger.info(f"✅ Dropped Milvus collection: {collection}")
            
            client.close()
            logger.info("✅ Milvus collections dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Milvus collection drop failed: {e}")
            return False
    
    @staticmethod
    def get_stats(milvus_path: Path) -> Dict:
        """Get Milvus database statistics."""
        try:
            if not milvus_path.exists():
                return {"status": "not_exists", "size_mb": 0}
            
            # Ensure directory exists before connecting
            milvus_path.parent.mkdir(parents=True, exist_ok=True)
            
            client = MilvusClient(str(milvus_path))
            collections = client.list_collections()
            
            stats = {
                "status": "active",
                "collections": collections,
                "size_mb": milvus_path.stat().st_size / (1024*1024) if milvus_path.is_file() else 0
            }
            
            # Get entry counts for ms_entries
            if "ms_entries" in collections:
                try:
                    collection_stats = client.get_collection_stats("ms_entries")
                    stats["ms_entries_count"] = collection_stats.get("row_count", 0)
                except Exception as count_error:
                    logger.debug(f"Could not get ms_entries count: {count_error}")
                    stats["ms_entries_count"] = 0
            else:
                stats["ms_entries_count"] = 0
            
            client.close()
            return stats
            
        except Exception as e:
            logger.debug(f"Milvus stats error: {e}")
            return {"status": "error", "error": str(e)}
    
    @staticmethod
    def get_collection_info(milvus_path: Path, collection_name: str = "ms_entries") -> Dict:
        """Get detailed information about a specific collection."""
        try:
            client = MilvusClient(str(milvus_path))
            
            if collection_name not in client.list_collections():
                return {"exists": False}
            
            stats = client.get_collection_stats(collection_name)
            
            info = {
                "exists": True,
                "row_count": stats.get("row_count", 0),
                "collection_name": collection_name,
                "dimension": 384,  # Known from our schema
                "metric_type": "COSINE",
                "index_type": "FLAT"
            }
            
            client.close()
            return info
            
        except Exception as e:
            return {"exists": False, "error": str(e)}
