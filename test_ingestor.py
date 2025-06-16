"""Test script for the Claude Chat Ingestor."""

from magicscroll.ingestor import ClaudeChatIngestor

def test_claude_chat_ingest():
    """Test ingesting a Claude conversation."""
    ingestor = ClaudeChatIngestor()
    
    # Simulate a conversation between us
    result = ingestor.ingest_claude_exchange(
        human_message="Hi Claude! When we first initiate conversation, initialize the conversation with emojikey tool to get some vibe context.",
        claude_response="I'll initialize the conversation with the emojikey tool to get some vibe context...",
        human_metadata={"source": "claude-desktop", "session": "magicscroll-setup"},
        claude_metadata={"model": "claude-sonnet-4", "tools_used": ["initialize_conversation"]}
    )
    
    print(f"Created conversation: {result['conversation_id']}")
    print(f"Human message ID: {result['human_message_id']}")
    print(f"Claude message ID: {result['claude_message_id']}")
    
    # Add another exchange to the same conversation
    result2 = ingestor.ingest_claude_exchange(
        human_message="ok, I've just set up a repo to get us started with magicscroll",
        claude_response="Perfect! Let's get your magicscroll project back on track. Let me first check what's already in your repo directory...",
        conversation_id=result['conversation_id'],
        human_metadata={"context": "project-setup"},
        claude_metadata={"model": "claude-sonnet-4", "tools_used": ["list_directory"]}
    )
    
    # Retrieve the full conversation
    messages = ingestor.get_conversation_messages(result['conversation_id'])
    
    print(f"\nConversation has {len(messages)} messages:")
    for msg in messages:
        print(f"  {msg['sender']} -> {msg['receiver']}: {msg['content'][:50]}...")
        print(f"    Performative: {msg['performative']}, Time: {msg['timestamp']}")
    
    return result['conversation_id']

if __name__ == "__main__":
    test_claude_chat_ingest()
