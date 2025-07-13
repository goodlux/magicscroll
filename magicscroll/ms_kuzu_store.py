"""Kuzu graph database operations for conversations, attachments, and artifacts."""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import re
import hashlib

logger = logging.getLogger(__name__)


# === ANTHROPIC CONVERSATIONS & ARTIFACTS FUNCTIONS ===

def create_anthropic_kuzu_schema(kuzu_conn):
    """Create Kuzu schema for Anthropic conversations, attachments, and artifacts."""
    try:
        # Core conversation nodes
        kuzu_conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS MS_CONVERSATION(
                uuid STRING PRIMARY KEY,
                name STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                message_count INT
            )
        """)
        
        # User-uploaded attachments
        kuzu_conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS MS_ATTACHMENT(
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
        
        # Claude-created artifacts
        kuzu_conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS MS_ARTIFACT(
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
        
        # Relationships
        kuzu_conn.execute("""
            CREATE REL TABLE IF NOT EXISTS HAS_ATTACHMENT(
                FROM MS_CONVERSATION TO MS_ATTACHMENT,
                attached_in_message STRING
            )
        """)
        
        kuzu_conn.execute("""
            CREATE REL TABLE IF NOT EXISTS CREATES_ARTIFACT(
                FROM MS_CONVERSATION TO MS_ARTIFACT,
                created_in_message STRING
            )
        """)
        
        logger.info("✅ Anthropic Kuzu schema created successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create Anthropic schema: {e}")
        return False


def extract_artifacts_from_message(message_content: str) -> List[Dict[str, Any]]:
    """Extract Claude artifacts from message content."""
    artifacts = []
    
    # Pattern to match <antArtifact> tags
    artifact_pattern = r'<antArtifact identifier="([^"]+)" type="([^"]+)"(?:\s+language="([^"]+)")?(?:\s+title="([^"]+)")?>([\s\S]*?)</antArtifact>'
    
    for match in re.finditer(artifact_pattern, message_content):
        identifier = match.group(1)
        artifact_type = match.group(2)
        language = match.group(3) or ""
        title = match.group(4) or ""
        content = match.group(5).strip()
        
        artifacts.append({
            'identifier': identifier,
            'title': title,
            'artifact_type': artifact_type,
            'language': language,
            'content': content
        })
    
    return artifacts


def store_conversation_in_kuzu(conversation: Dict[str, Any]) -> Dict[str, int]:
    """Store conversation, attachments, and artifacts in Kuzu."""
    result = {
        "conversations": 0,
        "attachments": 0,
        "artifacts": 0,
        "errors": 0
    }
    
    try:
        from .config import settings
        import kuzu
        
        # Connect to Kuzu
        settings.ensure_data_dir()
        kuzu_db = kuzu.Database(str(settings.kuzu_path))
        kuzu_conn = kuzu.Connection(kuzu_db)
        
        # Ensure schema exists
        create_anthropic_kuzu_schema(kuzu_conn)
        
        # Store conversation
        conv_uuid = conversation.get('id', '')
        conv_name = conversation.get('title', 'Untitled')
        created_at = conversation.get('created_at', '')
        updated_at = conversation.get('updated_at', '')
        message_count = len(conversation.get('messages', []))
        
        # Parse timestamps
        try:
            created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00')) if created_at else datetime.now()
            updated_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00')) if updated_at else datetime.now()
        except:
            created_dt = updated_dt = datetime.now()
        
        # Insert conversation
        kuzu_conn.execute("""
            MERGE (c:MS_CONVERSATION {uuid: $uuid})
            ON CREATE SET
                c.name = $name,
                c.created_at = $created_at,
                c.updated_at = $updated_at,
                c.message_count = $msg_count
            ON MATCH SET
                c.name = $name,
                c.updated_at = $updated_at,
                c.message_count = $msg_count
        """, {
            "uuid": conv_uuid,
            "name": conv_name,
            "created_at": created_dt,
            "updated_at": updated_dt,
            "msg_count": message_count
        })
        
        result["conversations"] = 1
        
        # Process messages for attachments and artifacts
        for message in conversation.get('messages', []):
            msg_uuid = message.get('id', '')
            msg_content = message.get('content', '')
            msg_metadata = message.get('metadata', {})
            
            # Process attachments
            attachments = msg_metadata.get('attachments', [])
            for attachment in attachments:
                try:
                    attachment_id = f"{conv_uuid}_{msg_uuid}_{attachment.get('file_name', 'unknown')}"
                    
                    # Parse attachment created_at if available
                    try:
                        att_created_dt = datetime.fromisoformat(message.get('created_at', '').replace('Z', '+00:00')) if message.get('created_at') else datetime.now()
                    except:
                        att_created_dt = datetime.now()
                    
                    # Store attachment
                    kuzu_conn.execute("""
                        MERGE (a:MS_ATTACHMENT {id: $id})
                        ON CREATE SET
                            a.file_name = $file_name,
                            a.file_type = $file_type,
                            a.file_size = $file_size,
                            a.extracted_content = $content,
                            a.conversation_uuid = $conv_uuid,
                            a.message_uuid = $msg_uuid,
                            a.created_at = $created_at
                    """, {
                        "id": attachment_id,
                        "file_name": attachment.get('file_name', ''),
                        "file_type": attachment.get('file_type', ''),
                        "file_size": attachment.get('file_size', 0),
                        "content": attachment.get('extracted_content', ''),
                        "conv_uuid": conv_uuid,
                        "msg_uuid": msg_uuid,
                        "created_at": att_created_dt
                    })
                    
                    # Create relationship
                    kuzu_conn.execute("""
                        MATCH (c:MS_CONVERSATION {uuid: $conv_uuid}), (a:MS_ATTACHMENT {id: $att_id})
                        MERGE (c)-[r:HAS_ATTACHMENT]->(a)
                        ON CREATE SET r.attached_in_message = $msg_uuid
                    """, {
                        "conv_uuid": conv_uuid,
                        "att_id": attachment_id,
                        "msg_uuid": msg_uuid
                    })
                    
                    result["attachments"] += 1
                    
                except Exception as e:
                    logger.error(f"Error storing attachment: {e}")
                    result["errors"] += 1
            
            # Extract and store artifacts
            artifacts = extract_artifacts_from_message(msg_content)
            for artifact in artifacts:
                try:
                    artifact_id = f"{conv_uuid}_{artifact['identifier']}"
                    
                    # Parse artifact created_at
                    try:
                        art_created_dt = datetime.fromisoformat(message.get('created_at', '').replace('Z', '+00:00')) if message.get('created_at') else datetime.now()
                    except:
                        art_created_dt = datetime.now()
                    
                    # Store artifact
                    kuzu_conn.execute("""
                        MERGE (a:MS_ARTIFACT {id: $id})
                        ON CREATE SET
                            a.identifier = $identifier,
                            a.title = $title,
                            a.artifact_type = $artifact_type,
                            a.language = $language,
                            a.content = $content,
                            a.conversation_uuid = $conv_uuid,
                            a.message_uuid = $msg_uuid,
                            a.created_at = $created_at
                    """, {
                        "id": artifact_id,
                        "identifier": artifact['identifier'],
                        "title": artifact['title'],
                        "artifact_type": artifact['artifact_type'],
                        "language": artifact['language'],
                        "content": artifact['content'],
                        "conv_uuid": conv_uuid,
                        "msg_uuid": msg_uuid,
                        "created_at": art_created_dt
                    })
                    
                    # Create relationship
                    kuzu_conn.execute("""
                        MATCH (c:MS_CONVERSATION {uuid: $conv_uuid}), (a:MS_ARTIFACT {id: $art_id})
                        MERGE (c)-[r:CREATES_ARTIFACT]->(a)
                        ON CREATE SET r.created_in_message = $msg_uuid
                    """, {
                        "conv_uuid": conv_uuid,
                        "art_id": artifact_id,
                        "msg_uuid": msg_uuid
                    })
                    
                    result["artifacts"] += 1
                    
                except Exception as e:
                    logger.error(f"Error storing artifact: {e}")
                    result["errors"] += 1
        
        kuzu_conn.close()
        
        logger.info(f"📊 Stored in Kuzu: {result}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error storing conversation in Kuzu: {e}")
        result["errors"] += 1
        return result


def get_anthropic_kuzu_stats() -> Dict[str, Any]:
    """Get statistics for the Anthropic Kuzu data."""
    try:
        from .config import settings
        import kuzu
        
        kuzu_db = kuzu.Database(str(settings.kuzu_path))
        kuzu_conn = kuzu.Connection(kuzu_db)
        
        stats = {"status": "active"}
        
        # Count conversations
        try:
            result = kuzu_conn.execute("MATCH (c:MS_CONVERSATION) RETURN COUNT(*) as count")
            stats["conversations"] = result.get_next()[0]
        except:
            stats["conversations"] = 0
        
        # Count attachments
        try:
            result = kuzu_conn.execute("MATCH (a:MS_ATTACHMENT) RETURN COUNT(*) as count")
            stats["attachments"] = result.get_next()[0]
        except:
            stats["attachments"] = 0
        
        # Count artifacts
        try:
            result = kuzu_conn.execute("MATCH (a:MS_ARTIFACT) RETURN COUNT(*) as count")
            stats["artifacts"] = result.get_next()[0]
        except:
            stats["artifacts"] = 0
        
        kuzu_conn.close()
        return stats
        
    except Exception as e:
        logger.error(f"Error getting Anthropic Kuzu stats: {e}")
        return {"status": "error", "error": str(e)}
