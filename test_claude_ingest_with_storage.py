"""Test Claude ingestor with actual database storage."""
import json
import sys
import asyncio
from pathlib import Path

# Add the magicscroll module to path
sys.path.append('/Users/rob/repos/magicscroll')

async def test_claude_ingest_with_storage():
    """Test ingesting Claude messages and storing in the database."""
    
    from magicscroll.claude_ingestor import ClaudeMessageIngestor
    
    # Initialize ingestor
    ingestor = ClaudeMessageIngestor()
    
    # Test file path
    test_file = "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/sample_conversations.json"
    
    print("🦹‍♂️ Starting Claude Message Ingestion with Database Storage...")
    
    try:
        # Parse and process the export
        conversations = ingestor.parse_claude_export(test_file)
        print(f"📂 Loaded {len(conversations)} conversations")
        
        # Process first conversation as a test
        test_conversation = conversations[0]
        print(f"\n📋 Testing with conversation: '{test_conversation.get('name', 'Untitled')}'")
        
        # Convert to FIPA messages
        fipa_messages = ingestor.process_conversation(test_conversation)
        print(f"💬 Converted {len(fipa_messages)} messages")
        
        # Create conversation record
        conv_record = ingestor.create_conversation_record(test_conversation)
        print(f"📝 Created conversation record")
        
        # Show what we're about to store
        print(f"\n🔍 Sample Message Structure:")
        if fipa_messages:
            sample_msg = fipa_messages[0]
            print(f"  Message ID: {sample_msg['message_id']}")
            print(f"  Speaker: {sample_msg['speaker']}")
            print(f"  Performative: {sample_msg['performative']}")
            print(f"  Content preview: {sample_msg['content'][:100]}...")
        
        print(f"\n🔍 Conversation Record:")
        print(f"  Conversation ID: {conv_record['conversation_id']}")
        print(f"  Title: {conv_record['title']}")
        print(f"  Message Count: {conv_record['message_count']}")
        
        return {
            'conversation_record': conv_record,
            'fipa_messages': fipa_messages,
            'sample_message': fipa_messages[0] if fipa_messages else None
        }
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        raise

if __name__ == "__main__":
    result = asyncio.run(test_claude_ingest_with_storage())
    print(f"\n✅ Test completed successfully!")
    print(f"📊 Processed {len(result['fipa_messages'])} messages")
