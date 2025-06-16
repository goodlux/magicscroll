# MagicScroll

A multi-modal data backend for AI chat applications and personal information management.

## Overview

MagicScroll provides a unified storage layer that combines three complementary database technologies to handle different aspects of personal AI data:

- **Milvus-lite** - Vector embeddings for semantic search across documents, conversations, and media
- **Oxigraph** - RDF triple store for knowledge graphs, relationships, and structured metadata  
- **SQLite** - Relational data for user preferences, chat history, and application state

## Architecture

The system stores all data locally in `~/.magicscroll/` with separate storage backends optimized for different data types:

```
~/.magicscroll/
├── sqlite/magicscroll-sqlite.db    # Relational data
├── oxigraph/                       # RDF triple store  
└── milvus/milvus.db               # Vector embeddings
```

## Features

- **Semantic Search** - Find relevant information across all your personal data using vector similarity
- **Knowledge Graphs** - Model complex relationships between entities, concepts, and conversations
- **Multi-Modal** - Handle text, images, documents, and structured data in a unified system
- **Privacy-First** - All data stored locally with no external dependencies
- **FastAPI Backend** - RESTful API for integration with AI chat interfaces

## Use Cases

- Personal AI assistants with long-term memory
- Conversational interfaces over personal document collections  
- Multi-modal search across photos, documents, and chat history
- Knowledge management and relationship mapping
- Context-aware AI applications

Built with Python, designed for developers building AI-powered personal productivity tools.
