#!/usr/bin/env python3
"""Test Kuzu ingestion with Anthropic conversations."""

import sys
import asyncio
from pathlib import Path

# Add magicscroll to path
sys.path.insert(0, str(Path(__file__).parent))

from magicscroll.ingestor.anthropic import AnthropicIngestor
from magicscroll.ms_kuzu_store import get_anthropic_kuzu_stats

async def test_kuzu_ingestion():
    """Test ingesting a few conversations into Kuzu."""
    
    print("🧪 Testing Kuzu Ingestion with Anthropic Data")
    print("=" * 50)
    
    # Path to your conversations.json
    conversations_json = "/Users/rob/repos/anthropic/data-2025-07-13-09-08-12-batch-0000/conversations.json"
    
    try:
        # Create ingestor (no MagicScroll instance needed for Kuzu testing)
        ingestor = AnthropicIngestor()
        
        # Parse a few conversations
        print("📖 Parsing conversations...")
        conversations = ingestor.parse_source_data(conversations_json)
        print(f"✅ Loaded {len(conversations)} conversations")
        
        # Test with just the first 3 conversations
        test_conversations = conversations[:3]
        print(f"🎯 Testing with {len(test_conversations)} conversations")
        
        # Store each conversation in Kuzu
        total_results = {"conversations": 0, "attachments": 0, "artifacts": 0, "errors": 0}
        
        for i, conversation in enumerate(test_conversations, 1):
            print(f"\n📊 Processing conversation {i}: {conversation.get('title', 'Untitled')[:50]}...")
            
            result = ingestor.store_conversation_in_kuzu(conversation)
            
            # Accumulate results
            for key in total_results:
                total_results[key] += result.get(key, 0)
            
            print(f"   Result: {result}")
        
        print(f"\n📈 Total Results: {total_results}")
        
        # Get Kuzu stats
        print("\n📊 Kuzu Database Stats:")
        stats = get_anthropic_kuzu_stats()
        print(f"   Status: {stats.get('status', 'unknown')}")
        print(f"   Conversations: {stats.get('conversations', 0)}")
        print(f"   Attachments: {stats.get('attachments', 0)}")
        print(f"   Artifacts: {stats.get('artifacts', 0)}")
        
        print("\n✅ Kuzu ingestion test completed!")
        
        # Clean up
        ingestor.close()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_kuzu_ingestion())
