"""Kuzu schema for MagicScroll Anthropic data ingestion."""

import logging
from pathlib import Path
from typing import Dict, Any
import kuzu

logger = logging.getLogger(__name__)


class AnthropicKuzuSchema:
    """Kuzu schema specifically for Anthropic conversation data."""
    
    @staticmethod
    def create_anthropic_schema(db_path: Path) -> bool:
        """Create Kuzu schema for Anthropic conversations, attachments, and artifacts.
        
        Args:
            db_path: Path to the Kuzu database directory
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure database directory exists
            db_path.mkdir(parents=True, exist_ok=True)
            
            # Connect to Kuzu database
            db = kuzu.Database(str(db_path))
            conn = kuzu.Connection(db)
            
            # Create node tables
            logger.info("Creating MS_CONVERSATION node table...")
            conn.execute("""
                CREATE NODE TABLE MS_CONVERSATION(
                    uuid STRING PRIMARY KEY,
                    name STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    message_count INT
                )
            """)
            
            logger.info("Creating MS_ATTACHMENT node table...")
            conn.execute("""
                CREATE NODE TABLE MS_ATTACHMENT(
                    id STRING PRIMARY KEY,
                    file_name STRING,
                    file_type STRING,
                    file_size INT,
                    extracted_content STRING,
                    conversation_uuid STRING,
                    message_uuid STRING,
                    created_at TIMESTAMP
                )
            """)
            
            logger.info("Creating MS_ARTIFACT node table...")
            conn.execute("""
                CREATE NODE TABLE MS_ARTIFACT(
                    id STRING PRIMARY KEY,
                    identifier STRING,
                    title STRING,
                    artifact_type STRING,
                    language STRING,
                    content STRING,
                    conversation_uuid STRING,
                    message_uuid STRING,
                    created_at TIMESTAMP
                )
            """)
            
            # Create relationship tables
            logger.info("Creating relationship tables...")
            conn.execute("""
                CREATE REL TABLE HAS_ATTACHMENT(
                    FROM MS_CONVERSATION TO MS_ATTACHMENT,
                    attached_in_message STRING
                )
            """)
            
            conn.execute("""
                CREATE REL TABLE CREATES_ARTIFACT(
                    FROM MS_CONVERSATION TO MS_ARTIFACT,
                    created_in_message STRING
                )
            """)
            
            # Close connection
            conn.close()
            
            logger.info(f"✅ Anthropic Kuzu schema created successfully at {db_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Anthropic Kuzu schema: {e}")
            return False
    
    @staticmethod
    def drop_all_data(db_path: Path) -> bool:
        """Drop all data from the Kuzu database.
        
        Args:
            db_path: Path to the Kuzu database directory
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not db_path.exists():
                logger.info("Kuzu database directory doesn't exist, nothing to drop")
                return True
            
            # Connect to database
            db = kuzu.Database(str(db_path))
            conn = kuzu.Connection(db)
            
            # Drop relationships first (foreign key constraints)
            try:
                conn.execute("DROP TABLE HAS_ATTACHMENT")
                conn.execute("DROP TABLE CREATES_ARTIFACT")
            except:
                pass  # Tables might not exist
            
            # Drop node tables
            try:
                conn.execute("DROP TABLE MS_CONVERSATION")
                conn.execute("DROP TABLE MS_ATTACHMENT") 
                conn.execute("DROP TABLE MS_ARTIFACT")
            except:
                pass  # Tables might not exist
            
            conn.close()
            
            logger.info(f"✅ Cleared Anthropic data from Kuzu database at {db_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to clear Kuzu database: {e}")
            return False
    
    @staticmethod
    def get_stats(db_path: Path) -> Dict[str, Any]:
        """Get statistics about the Kuzu database.
        
        Args:
            db_path: Path to the Kuzu database directory
            
        Returns:
            Dict containing database statistics
        """
        try:
            if not db_path.exists():
                return {
                    "status": "not_found",
                    "path": str(db_path),
                    "error": "Database directory does not exist"
                }
            
            db = kuzu.Database(str(db_path))
            conn = kuzu.Connection(db)
            
            stats = {"status": "active", "path": str(db_path)}
            
            # Count conversations
            try:
                result = conn.execute("MATCH (c:MS_CONVERSATION) RETURN COUNT(*)")
                stats["conversations"] = result.get_next()[0]
            except:
                stats["conversations"] = 0
            
            # Count attachments  
            try:
                result = conn.execute("MATCH (a:MS_ATTACHMENT) RETURN COUNT(*)")
                stats["attachments"] = result.get_next()[0]
            except:
                stats["attachments"] = 0
            
            # Count artifacts
            try:
                result = conn.execute("MATCH (a:MS_ARTIFACT) RETURN COUNT(*)")
                stats["artifacts"] = result.get_next()[0]
            except:
                stats["artifacts"] = 0
            
            # Count relationships
            try:
                result = conn.execute("MATCH ()-[r:HAS_ATTACHMENT]->() RETURN COUNT(*)")
                stats["attachment_relationships"] = result.get_next()[0]
            except:
                stats["attachment_relationships"] = 0
            
            try:
                result = conn.execute("MATCH ()-[r:CREATES_ARTIFACT]->() RETURN COUNT(*)")
                stats["artifact_relationships"] = result.get_next()[0]
            except:
                stats["artifact_relationships"] = 0
            
            conn.close()
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get Kuzu stats: {e}")
            return {
                "status": "error",
                "path": str(db_path),
                "error": str(e)
            }
