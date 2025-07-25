"""Anthropic Claude export ingestor - ingests Claude conversation exports into MagicScroll."""

import json
import uuid
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging

from .base import BaseIngestor
from ..ms_kuzu_store import store_conversation_in_kuzu

logger = logging.getLogger(__name__)

class AnthropicIngestor(BaseIngestor):
    """Ingestor for Anthropic Claude conversation exports."""
    
    def __init__(self, magic_scroll=None, db_path: Optional[str] = None):
        """Initialize Anthropic ingestor."""
        super().__init__(magic_scroll, db_path)
        
        self.source_name = "anthropic_claude"
        self.supported_formats = [".json"]
    
    def parse_source_data(self, source_path: str) -> List[Dict[str, Any]]:
        """
        Parse Claude export JSON file into standardized format.
        
        Args:
            source_path: Path to Claude export JSON file
            
        Returns:
            List of standardized conversation dictionaries
        """
        try:
            with open(source_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                raise ValueError("Expected list of conversations at top level")
            
            logger.info(f"Loaded {len(data)} conversations from {source_path}")
            
            # Convert to standardized format
            standardized_conversations = []
            
            for claude_conv in data:
                standardized_conv = self._standardize_conversation(claude_conv)
                if standardized_conv:
                    standardized_conversations.append(standardized_conv)
            
            return standardized_conversations
            
        except Exception as e:
            logger.error(f"Error parsing Claude export: {e}")
            raise
    
    def _standardize_conversation(self, claude_conv: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Claude conversation to standardized format."""
        try:
            # Extract basic conversation info
            conversation = {
                'id': claude_conv.get('uuid', str(uuid.uuid4())),
                'title': claude_conv.get('name', 'Untitled Conversation'),
                'created_at': claude_conv.get('created_at', ''),
                'updated_at': claude_conv.get('updated_at', ''),
                'metadata': {
                    'source': 'anthropic_claude',
                    'account_uuid': claude_conv.get('account', {}).get('uuid', ''),
                    'has_attachments': False  # Will be updated if we find attachments
                },
                'messages': []
            }
            
            # Process messages
            claude_messages = claude_conv.get('chat_messages', [])
            
            # Sort messages by timestamp
            sorted_messages = sorted(
                claude_messages,
                key=lambda m: m.get('created_at', '1970-01-01T00:00:00Z')
            )
            
            for claude_msg in sorted_messages:
                standardized_msg = self._standardize_message(claude_msg)
                if standardized_msg:
                    conversation['messages'].append(standardized_msg)
                    
                    # Check for attachments
                    if (claude_msg.get('attachments') or claude_msg.get('files')):
                        conversation['metadata']['has_attachments'] = True
            
            return conversation
            
        except Exception as e:
            logger.warning(f"Error standardizing conversation {claude_conv.get('uuid', 'unknown')}: {e}")
            return None
    
    def _standardize_message(self, claude_msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Claude message to standardized format."""
        try:
            return {
                'id': claude_msg.get('uuid', str(uuid.uuid4())),
                'sender': claude_msg.get('sender', 'unknown'),
                'content': self.extract_message_content(claude_msg),
                'created_at': claude_msg.get('created_at', ''),
                'metadata': {
                    'updated_at': claude_msg.get('updated_at', ''),
                    'attachments': claude_msg.get('attachments', []),
                    'files': claude_msg.get('files', []),
                    'content_structure': claude_msg.get('content', []) if isinstance(claude_msg.get('content'), list) else None
                }
            }
        except Exception as e:
            logger.warning(f"Error standardizing message {claude_msg.get('uuid', 'unknown')}: {e}")
            return None
    
    def extract_message_content(self, message: Dict[str, Any]) -> str:
        """
        Extract clean text content from Claude message.
        
        Args:
            message: Claude message dictionary
            
        Returns:
            Extracted text content
        """
        message_id = message.get('uuid', 'unknown')
        
        # ENHANCED DEBUGGING: Print full message structure for first few messages
        if not hasattr(self, '_debug_count'):
            self._debug_count = 0
        
        if self._debug_count < 5:  # Show first 5 messages in detail
            logger.warning(f"\n=== DEBUGGING MESSAGE {self._debug_count + 1} ===")
            logger.warning(f"Message keys: {list(message.keys())}")
            logger.warning(f"Full message structure: {json.dumps(message, indent=2)[:1000]}...")
            self._debug_count += 1
        
        # Try text field first (direct text)
        text_field = message.get('text')
        if text_field and isinstance(text_field, str) and text_field.strip():
            logger.info(f"✅ Using 'text' field for message {message_id[:8]}...: {len(text_field)} chars")
            return text_field.strip()
        
        # Fallback to content array (structured content)
        content_field = message.get('content')
        if content_field and isinstance(content_field, list):
            text_parts = []
            for i, content_block in enumerate(content_field):
                if (isinstance(content_block, dict) and 
                    content_block.get('type') == 'text' and 
                    content_block.get('text')):
                    
                    block_text = content_block.get('text')
                    if isinstance(block_text, str) and block_text.strip():
                        text_parts.append(block_text.strip())
                        if self._debug_count <= 5:
                            logger.warning(f"  Found text in content[{i}]: {repr(block_text[:100])}")
            
            if text_parts:
                result = '\n'.join(text_parts)
                logger.info(f"✅ Using 'content' array for message {message_id[:8]}...: {len(result)} chars from {len(text_parts)} parts")
                return result
        
        # Enhanced debug logging for problematic messages
        logger.warning(f"❌ No content extracted for message {message_id[:8]}... (sender: {message.get('sender', 'unknown')})")
        if self._debug_count <= 5:
            logger.warning(f"  Available fields: {list(message.keys())}")
            logger.warning(f"  Text field: {type(text_field)} = {repr(text_field)}")
            logger.warning(f"  Content field: {type(content_field)} = {repr(content_field) if content_field else None}")
            if isinstance(content_field, list) and content_field:
                logger.warning(f"  Content[0] structure: {json.dumps(content_field[0], indent=2)[:200]}...")
        
        # Last resort - empty string
        return ""
    
    def standardize_sender(self, raw_sender: str) -> str:
        """
        Standardize Claude sender names.
        
        Args:
            raw_sender: Raw sender from Claude export
            
        Returns:
            Standardized sender name
        """
        sender_lower = raw_sender.lower()
        
        if sender_lower == 'human':
            return 'human'
        elif sender_lower == 'assistant':
            return 'assistant'
        else:
            # Keep original for specific Claude models
            return raw_sender
    
    def store_conversation_in_kuzu(self, conversation: Dict[str, Any]) -> Dict[str, int]:
        """Store conversation, attachments, and artifacts in Kuzu graph database."""
        try:
            result = store_conversation_in_kuzu(conversation)
            logger.info(f"✅ Stored conversation {conversation.get('id', 'unknown')[:8]}... in Kuzu: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ Failed to store conversation in Kuzu: {e}")
            return {"conversations": 0, "attachments": 0, "artifacts": 0, "errors": 1}


# Convenience function for backward compatibility
async def ingest_claude_export(
    export_path: str,
    magic_scroll=None,
    db_path: Optional[str] = None,
    create_ms_entries: bool = False,
    limit_conversations: Optional[int] = None
) -> Dict[str, Any]:
    """
    Convenience function to ingest Claude export using AnthropicIngestor.
    
    Args:
        export_path: Path to Claude export JSON file
        magic_scroll: Optional MagicScroll instance
        db_path: Optional database path
        create_ms_entries: Whether to create MSEntry objects
        limit_conversations: Optional limit on conversations to process
        
    Returns:
        Ingestion summary dictionary
    """
    ingestor = AnthropicIngestor(magic_scroll, db_path)
    
    try:
        result = await ingestor.ingest(
            export_path,
            create_ms_entries=create_ms_entries,
            limit_conversations=limit_conversations
        )
        return result
    finally:
        ingestor.close()


# Example usage for testing
async def test_anthropic_ingestor(export_path: str, limit: int = 3):
    """Test the Anthropic ingestor."""
    print(f"🧪 Testing Anthropic Ingestor with {export_path}")
    
    try:
        result = await ingest_claude_export(
            export_path,
            create_ms_entries=False,  # Don't create MSEntries for test
            limit_conversations=limit
        )
        
        print(f"✅ Ingestion completed!")
        print(f"📊 Results:")
        print(f"  - Source: {result['source']}")
        print(f"  - Conversations: {result['processed_conversations']}")
        print(f"  - Messages: {result['processed_messages']}")
        print(f"  - Errors: {result['errors']}")
        print(f"  - Success: {result['success']}")
        
        if result['errors'] > 0:
            print(f"\n⚠️ Error details:")
            for error in result['error_messages']:
                print(f"  - {error}")
        
        return result
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    import asyncio
    
    # Test with sample file
    test_file = "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/sample_conversations.json"
    asyncio.run(test_anthropic_ingestor(test_file))
