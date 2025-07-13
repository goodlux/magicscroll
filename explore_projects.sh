#!/bin/bash
# Explore projects.json structure

PROJ_FILE="/Users/rob/repos/anthropic/data-2025-07-13-09-08-12-batch-0000/projects.json"

echo "🔍 Exploring projects.json structure"
echo "=" * 50

if [ ! -f "$PROJ_FILE" ]; then
    echo "❌ projects.json not found!"
    exit 1
fi

echo "📊 Basic Stats:"
echo "Total projects:"
jq '. | length' "$PROJ_FILE"

echo -e "\n🏗️ Top-level structure of first project:"
jq '.[0] | keys' "$PROJ_FILE"

echo -e "\n📝 Sample project metadata:"
jq '.[0] | {uuid, name, created_at, updated_at}' "$PROJ_FILE"

echo -e "\n🎨 Looking for artifacts:"
echo "Projects with artifacts:"
jq '[.[] | select(.artifacts? | length > 0)] | length' "$PROJ_FILE"

if [ $(jq '[.[] | select(.artifacts? | length > 0)] | length' "$PROJ_FILE") -gt 0 ]; then
    echo -e "\nFirst project with artifacts:"
    jq '[.[] | select(.artifacts? | length > 0)][0] | {name, uuid}' "$PROJ_FILE"
    
    echo -e "\n🎨 Artifact structure:"
    jq '[.[] | select(.artifacts? | length > 0)][0].artifacts[0] | keys' "$PROJ_FILE"
    
    echo -e "\n📋 Sample artifact:"
    jq '[.[] | select(.artifacts? | length > 0)][0].artifacts[0]' "$PROJ_FILE" | head -20
fi

echo -e "\n🗓️ Date range:"
echo "Earliest project:"
jq '[.[] | .created_at] | min' "$PROJ_FILE"
echo "Latest project:"
jq '[.[] | .created_at] | max' "$PROJ_FILE"

echo -e "\n📈 Artifact count distribution:"
jq '[.[] | .artifacts? | length // 0] | group_by(.) | map({artifact_count: .[0], projects_with_count: length})' "$PROJ_FILE"
