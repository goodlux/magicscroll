"""Domain types for MagicScroll."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
import uuid

class EntryType(Enum):
    """Types of entries in MagicScroll."""
    CONVERSATION = "conversation"
    DOCUMENT = "document"  # For PDFs, text files, etc
    IMAGE = "image"        # For image files
    CODE = "code"         # For code snippets/files

@dataclass
class MSEntry:
    """Base class for MagicScroll entries."""
    content: str
    entry_type: EntryType
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata dictionary without content."""
        return {
            "id": self.id,
            "type": self.entry_type.value,
            "created_at": self.created_at.isoformat(),
            **self.metadata  # spread any additional metadata
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert entry to dictionary format."""
        return {
            "id": self.id,
            "content": self.content,
            "type": self.entry_type.value,
            "created_at": self.created_at.isoformat(),
            **self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MSEntry':
        """Create entry from dictionary format."""
        # Convert created_at from ISO string to datetime
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        else:
            created_at = datetime.utcnow()

        # Extract core fields
        entry_type = data.get("type", "conversation")
        
        # Extract metadata (excluding core fields)
        metadata = {k: v for k, v in data.items() 
                if k not in ['id', 'content', 'type', 'created_at']}

        return cls(
            id=data["id"],
            content=data["content"],
            entry_type=EntryType(entry_type),
            metadata=metadata,
            created_at=created_at
        )
    
    def to_dict_with_vector(self, vector=None) -> Dict[str, Any]:
        """Convert entry to dictionary format with vector embedding."""
        result = self.to_dict()
        if vector is not None:
            result["vector"] = vector
        return result

class MSConversation(MSEntry):
    """A conversation entry - fully implemented."""
    def __init__(
        self,
        content: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            content=content,
            entry_type=EntryType.CONVERSATION,
            metadata={
                **(metadata or {}),
                "speaker_count": content.count("Assistant:") + content.count("User:")
            }
        )

class MSDocument(MSEntry):
    """
    A document entry (PDF, text, etc) - NOT YET IMPLEMENTED.
    Will require appropriate document reader to convert to text before storage.
    """
    def __init__(self):
        raise NotImplementedError(
            "Document handling not yet implemented. "
            "Will require document processing setup."
        )

class MSImage(MSEntry):
    """
    An image entry - NOT YET IMPLEMENTED.
    Will require image processing to extract/generate 
    text content before storage.
    """
    def __init__(self):
        raise NotImplementedError(
            "Image handling not yet implemented. "
            "Will require image processing setup."
        )

class MSCode(MSEntry):
    """
    A code entry - NOT YET IMPLEMENTED.
    May require special handling for language-specific parsing
    or documentation extraction.
    """
    def __init__(self):
        raise NotImplementedError(
            "Code handling not yet implemented. "
            "Will require code parsing setup."
        )
