"""Claude Message Ingestor - Converts Claude exports to MagicScroll format."""
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging

# from .digital_trinity.fipa_acl import FIPAACLMessage  # Not needed for direct storage

logger = logging.getLogger(__name__)

class ClaudeMessageIngestor:
    """Ingests Claude conversation exports into MagicScroll using FIPA-ACL format."""
    
    def __init__(self):
        """Initialize the Claude message ingestor."""
        self.processed_conversations = 0
        self.processed_messages = 0
        self.errors = []
    
    def parse_claude_export(self, export_path: str) -> List[Dict[str, Any]]:
        """
        Parse Claude export JSON file.
        
        Args:
            export_path: Path to the Claude export JSON file
            
        Returns:
            List of conversation dictionaries
        """
        try:
            with open(export_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                raise ValueError("Expected list of conversations at top level")
                
            logger.info(f"Loaded {len(data)} conversations from {export_path}")
            return data
            
        except Exception as e:
            logger.error(f"Error parsing Claude export: {e}")
            raise
    
    def extract_message_content(self, message: Dict[str, Any]) -> str:
        """
        Extract the main text content from a Claude message.
        
        Args:
            message: Claude message dictionary
            
        Returns:
            Extracted text content
        """
        # Try text field first
        if 'text' in message and message['text']:
            return message['text']
        
        # Fallback to content array
        if 'content' in message and isinstance(message['content'], list):
            text_parts = []
            for content_block in message['content']:
                if content_block.get('type') == 'text' and content_block.get('text'):
                    text_parts.append(content_block['text'])
            
            if text_parts:
                return '\n'.join(text_parts)
        
        # Last resort
        return ""
    
    def extract_message_metadata(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metadata from Claude message including attachments and files.
        
        Args:
            message: Claude message dictionary
            
        Returns:
            Metadata dictionary
        """
        metadata = {
            'original_format': 'claude_export',
            'claude_message_uuid': message.get('uuid', ''),
            'updated_at': message.get('updated_at', ''),
        }
        
        # Add attachment information
        if 'attachments' in message and message['attachments']:
            metadata['attachments'] = message['attachments']
        
        # Add file information (with extracted content if available)
        if 'files' in message and message['files']:
            metadata['files'] = message['files']
        
        # Add content structure if complex
        if 'content' in message and isinstance(message['content'], list):
            metadata['content_structure'] = message['content']
        
        return metadata
    
    def convert_to_fipa_message(
        self, 
        claude_message: Dict[str, Any], 
        conversation_id: str,
        previous_message_id: Optional[str] = None
    ) -> FIPAACLMessage:
        """
        Convert Claude message to FIPA-ACL format.
        
        Args:
            claude_message: Claude message dictionary
            conversation_id: Conversation UUID
            previous_message_id: Previous message ID for threading
            
        Returns:
            FIPA-ACL message
        """
        # Get the actual speaker from Claude export
        speaker = claude_message.get('sender', 'unknown')
        
        # Map to FIPA performatives while preserving speaker identity
        if speaker == 'human':
            performative = 'REQUEST'
            sender_id = 'user'
            receiver_id = 'assistant'
        elif speaker == 'assistant':
            performative = 'INFORM'
            sender_id = 'assistant'
            receiver_id = 'user'
        else:
            performative = 'INFORM'
            sender_id = speaker
            receiver_id = None
        
        # Extract content and metadata
        content = self.extract_message_content(claude_message)
        metadata = self.extract_message_metadata(claude_message)
        
        # Create simplified message dict for storage
        fipa_message = {
            'message_id': claude_message.get('uuid', str(uuid.uuid4())),
            'conversation_id': conversation_id,
            'sender': sender_id,
            'receiver': receiver_id,
            'speaker': speaker,  # Actual speaker identity
            'content': content,
            'performative': performative,
            'timestamp': claude_message.get('created_at', ''),
            'reply_with': None,
            'in_reply_to': previous_message_id,
            'metadata': json.dumps(metadata)
        }
        
        return fipa_message
    
    def process_conversation(self, conversation: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process a single Claude conversation into FIPA-ACL messages.
        
        Args:
            conversation: Claude conversation dictionary
            
        Returns:
            List of FIPA message dictionaries
        """
        try:
            conversation_id = conversation.get('uuid', str(uuid.uuid4()))
            messages = conversation.get('chat_messages', [])
            
            fipa_messages = []
            previous_message_id = None
            
            # Sort messages by created_at to ensure proper order
            sorted_messages = sorted(
                messages, 
                key=lambda m: m.get('created_at', '1970-01-01T00:00:00Z')
            )
            
            for claude_message in sorted_messages:
                try:
                    fipa_message = self.convert_to_fipa_message(
                        claude_message, 
                        conversation_id,
                        previous_message_id
                    )
                    fipa_messages.append(fipa_message)
                    previous_message_id = fipa_message['message_id']
                    
                except Exception as e:
                    self.errors.append(f"Error processing message {claude_message.get('uuid', 'unknown')}: {e}")
                    logger.warning(f"Skipping message due to error: {e}")
                    continue
            
            self.processed_messages += len(fipa_messages)
            return fipa_messages
            
        except Exception as e:
            self.errors.append(f"Error processing conversation {conversation.get('uuid', 'unknown')}: {e}")
            logger.error(f"Error processing conversation: {e}")
            return []
    
    def create_conversation_record(self, conversation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create conversation record for storage.
        
        Args:
            conversation: Claude conversation dictionary
            
        Returns:
            Conversation record for database
        """
        return {
            'conversation_id': conversation.get('uuid', str(uuid.uuid4())),
            'title': conversation.get('name', 'Untitled Conversation'),
            'start_time': conversation.get('created_at', ''),
            'end_time': conversation.get('updated_at', ''),
            'account_uuid': conversation.get('account', {}).get('uuid', ''),
            'message_count': len(conversation.get('chat_messages', [])),
            'metadata': json.dumps({
                'original_format': 'claude_export',
                'has_attachments': any(
                    msg.get('attachments') or msg.get('files') 
                    for msg in conversation.get('chat_messages', [])
                )
            })
        }
    
    def store_conversation(self, conversation_record: Dict[str, Any]) -> bool:
        """Store a conversation record in the database."""
        try:
            # This would use MCP SQL tools in the actual implementation
            # For now, we'll just return success
            logger.info(f"Would store conversation: {conversation_record['conversation_id']}")
            return True
        except Exception as e:
            logger.error(f"Error storing conversation: {e}")
            return False
    
    def store_message(self, message_record: Dict[str, Any]) -> bool:
        """Store a message record in the database."""
        try:
            # This would use MCP SQL tools in the actual implementation  
            # For now, we'll just return success
            logger.info(f"Would store message: {message_record['message_id']}")
            return True
        except Exception as e:
            logger.error(f"Error storing message: {e}")
            return False
    
    async def ingest_claude_export(
        self, 
        export_path: str,
        store_conversations: bool = True,
        store_messages: bool = True,
        create_vectors: bool = True
    ) -> Dict[str, Any]:
        """
        Main ingestion method to process Claude export and store in MagicScroll.
        
        Args:
            export_path: Path to Claude export JSON file
            store_conversations: Whether to store conversation metadata
            store_messages: Whether to store individual messages  
            create_vectors: Whether to create vector embeddings
            
        Returns:
            Ingestion summary
        """
        logger.info(f"Starting Claude export ingestion from {export_path}")
        
        # Reset counters
        self.processed_conversations = 0
        self.processed_messages = 0
        self.errors = []
        
        try:
            # Parse the export file
            conversations = self.parse_claude_export(export_path)
            
            total_fipa_messages = []
            conversation_summaries = []
            
            # Process each conversation
            for conversation in conversations:
                try:
                    # Convert to FIPA messages
                    fipa_messages = self.process_conversation(conversation)
                    if fipa_messages:
                        total_fipa_messages.extend(fipa_messages)
                    
                    # Create conversation record
                    if store_conversations:
                        conv_record = self.create_conversation_record(conversation)
                        conversation_summaries.append(conv_record)
                    
                    self.processed_conversations += 1
                    
                    if self.processed_conversations % 100 == 0:
                        logger.info(f"Processed {self.processed_conversations} conversations...")
                        
                except Exception as e:
                    self.errors.append(f"Failed to process conversation: {e}")
                    logger.warning(f"Skipping conversation due to error: {e}")
                    continue
            
            # TODO: Store in MagicScroll databases
            # For now, just return the processed data
            
            summary = {
                'processed_conversations': self.processed_conversations,
                'processed_messages': self.processed_messages,
                'total_fipa_messages': len(total_fipa_messages),
                'conversation_summaries': len(conversation_summaries),
                'errors': len(self.errors),
                'error_messages': self.errors[:10]  # First 10 errors
            }
            
            logger.info(f"Ingestion complete: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Fatal error during ingestion: {e}")
            raise


# Example usage function for testing
async def test_claude_ingestor(export_path: str):
    """Test the Claude ingestor with a sample export."""
    ingestor = ClaudeMessageIngestor()
    
    try:
        result = await ingestor.ingest_claude_export(export_path)
        print(f"Ingestion Result: {json.dumps(result, indent=2)}")
        
        if ingestor.errors:
            print(f"\nErrors encountered:")
            for error in ingestor.errors[:5]:  # Show first 5 errors
                print(f"  - {error}")
                
    except Exception as e:
        print(f"Ingestion failed: {e}")

if __name__ == "__main__":
    import asyncio
    # Test with the sample file
    asyncio.run(test_claude_ingestor("/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/sample_conversations.json"))
