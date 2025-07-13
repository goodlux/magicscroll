#!/bin/bash
# Explore the structure of conversations.json

CONV_FILE="/Users/rob/repos/anthropic/data-2025-07-13-09-08-12-batch-0000/conversations.json"

echo "🔍 Exploring conversations.json structure"
echo "=" * 50

echo "📊 Basic Stats:"
echo "Total conversations:"
jq '. | length' "$CONV_FILE"

echo -e "\n🏗️ Top-level structure of first conversation:"
jq '.[0] | keys' "$CONV_FILE"

echo -e "\n💬 Sample conversation metadata:"
jq '.[0] | {uuid, name, created_at, updated_at}' "$CONV_FILE"

echo -e "\n📝 Chat messages structure in first conversation:"
jq '.[0].chat_messages | length' "$CONV_FILE"
echo "First message keys:"
jq '.[0].chat_messages[0] | keys' "$CONV_FILE"

echo -e "\n👤 Sample message:"
jq '.[0].chat_messages[0] | {uuid, sender, text, created_at}' "$CONV_FILE"

echo -e "\n📎 Looking for attachments/artifacts:"
echo "Conversations with attachments:"
jq '[.[] | select(.chat_messages[]?.attachments? | length > 0)] | length' "$CONV_FILE"

echo -e "\nFirst conversation with attachments:"
jq '[.[] | select(.chat_messages[]?.attachments? | length > 0)][0] | {name, uuid}' "$CONV_FILE"

echo -e "\n📄 Attachment structure:"
jq '[.[] | select(.chat_messages[]?.attachments? | length > 0)][0].chat_messages[] | select(.attachments? | length > 0) | .attachments[0] | keys' "$CONV_FILE"

echo -e "\n📋 Sample attachment:"
jq '[.[] | select(.chat_messages[]?.attachments? | length > 0)][0].chat_messages[] | select(.attachments? | length > 0) | .attachments[0]' "$CONV_FILE" | head -20

echo -e "\n🎨 Looking for artifacts:"
echo "Messages with artifacts:"
jq '[.[] | .chat_messages[] | select(.text? | contains("artifact"))] | length' "$CONV_FILE"

echo -e "\n📊 Message sender distribution:"
jq '[.[] | .chat_messages[] | .sender] | group_by(.) | map({sender: .[0], count: length})' "$CONV_FILE"

echo -e "\n🗓️ Date range:"
echo "Earliest conversation:"
jq '[.[] | .created_at] | min' "$CONV_FILE"
echo "Latest conversation:"
jq '[.[] | .created_at] | max' "$CONV_FILE"
