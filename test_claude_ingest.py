"""Test the Claude ingestor with MCP SQL storage."""
import json
import asyncio
from pathlib import Path

async def test_claude_ingest_and_store():
    """Test ingesting Claude messages and storing in SQL."""
    
    # Import after we've set up the file
    import sys
    sys.path.append('/Users/rob/repos/magicscroll')
    
    from magicscroll.claude_ingestor import ClaudeMessageIngestor
    
    # Initialize ingestor
    ingestor = ClaudeMessageIngestor()
    
    # Test file path
    test_file = "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/sample_conversations.json"
    
    print("🦹‍♂️ Starting Claude Message Ingestion...")
    
    try:
        # Parse the export
        conversations = ingestor.parse_claude_export(test_file)
        print(f"📂 Loaded {len(conversations)} conversations")
        
        # Process each conversation
        all_fipa_messages = []
        conversation_summaries = []
        
        for i, conversation in enumerate(conversations):
            print(f"\n📋 Processing conversation {i+1}: '{conversation.get('name', 'Untitled')}'")
            
            # Convert to FIPA messages
            fipa_messages = ingestor.process_conversation(conversation)
            print(f"  💬 Converted {len(fipa_messages)} messages")
            
            # Show sample message
            if fipa_messages:
                sample = fipa_messages[0]
                print(f"  🔍 Sample: speaker='{sample.metadata.get('speaker')}', performative='{sample.performative}'")
                print(f"      Content preview: {sample.content[:100]}...")
            
            all_fipa_messages.extend(fipa_messages)
            
            # Create conversation summary
            conv_summary = ingestor.create_conversation_summary(conversation)
            conversation_summaries.append(conv_summary)
        
        print(f"\n📊 Processed {len(all_fipa_messages)} total messages from {len(conversation_summaries)} conversations")
        
        # Now let's store them using MCP tools (we'll call these manually)
        return {
            'fipa_messages': all_fipa_messages,
            'conversations': conversation_summaries,
            'summary': {
                'total_conversations': len(conversation_summaries),
                'total_messages': len(all_fipa_messages),
                'errors': len(ingestor.errors)
            }
        }
        
    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_claude_ingest_and_store())
