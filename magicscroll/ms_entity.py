"""Entity extraction using GLiNER for MagicScroll conversations."""

import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ExtractedEntity:
    """Represents an extracted entity from text."""
    text: str
    label: str
    confidence: float
    start: int
    end: int


class EntityExtractor:
    """Local entity extractor using GLiNER for conversation processing."""
    
    # Domain-specific entity types for MagicScroll conversations
    DEFAULT_ENTITY_TYPES = [
        "person",
        "organization", 
        "project_name",
        "technology",
        "protocol",
        "programming_language",
        "conversation_topic",
        "temporal_reference",
        "tool",
        "framework"
    ]
    
    def __init__(self, model_name: str = "gliner-community/gliner_medium-v2.5", preload: bool = True):
        """Initialize the entity extractor.
        
        Args:
            model_name: GLiNER model to use for extraction
            preload: Whether to load the model immediately
        """
        self.model_name = model_name
        self.model = None
        self._entity_types = self.DEFAULT_ENTITY_TYPES
        self._gliner_available = None
        
        if preload:
            self._check_gliner_availability()
            if self._gliner_available:
                self._load_model()
    
    def _check_gliner_availability(self) -> bool:
        """Check if GLiNER is available and cache the result."""
        if self._gliner_available is None:
            try:
                import gliner
                self._gliner_available = True
                logger.info(f"GLiNER available (version: {getattr(gliner, '__version__', 'unknown')})")
            except ImportError:
                self._gliner_available = False
                logger.warning("GLiNER not installed. Entity extraction will be disabled. Run: pip install gliner")
        return self._gliner_available
        
    def _load_model(self):
        """Load the GLiNER model once at startup."""
        if not self._check_gliner_availability():
            return
            
        if self.model is None:
            try:
                from gliner import GLiNER
                logger.info(f"Loading GLiNER model: {self.model_name}")
                self.model = GLiNER.from_pretrained(self.model_name)
                logger.info("GLiNER model loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load GLiNER model: {e}")
                self.model = None
                self._gliner_available = False
    
    def extract_entities(
        self, 
        text: str, 
        entity_types: Optional[List[str]] = None,
        confidence_threshold: float = 0.3
    ) -> List[ExtractedEntity]:
        """Extract entities from text using GLiNER.
        
        Args:
            text: Text to extract entities from
            entity_types: List of entity types to extract (uses defaults if None)
            confidence_threshold: Minimum confidence score for entities
            
        Returns:
            List of extracted entities
        """
        if not text or not text.strip():
            return []
        
        # Check if GLiNER is available and model is loaded
        if not self._gliner_available or self.model is None:
            logger.debug("GLiNER not available or model not loaded - returning empty entities")
            return []
            
        if entity_types is None:
            entity_types = self._entity_types
            
        try:
            # GLiNER prediction
            predictions = self.model.predict_entities(text, entity_types)
            
            # Convert to our format
            entities = []
            for pred in predictions:
                if pred.get("score", 0) >= confidence_threshold:
                    entity = ExtractedEntity(
                        text=pred["text"],
                        label=pred["label"], 
                        confidence=pred["score"],
                        start=pred["start"],
                        end=pred["end"]
                    )
                    entities.append(entity)
            
            logger.debug(f"Extracted {len(entities)} entities from text of length {len(text)}")
            return entities
            
        except Exception as e:
            logger.error(f"Entity extraction failed: {e}")
            return []
    
    def extract_for_conversation(self, conversation_text: str) -> Dict[str, Any]:
        """Extract entities specifically for conversation storage.
        
        Args:
            conversation_text: Full conversation text
            
        Returns:
            Dictionary with extracted entities and metadata
        """
        entities = self.extract_entities(conversation_text)
        
        # Group entities by type for easier processing
        entities_by_type = {}
        for entity in entities:
            if entity.label not in entities_by_type:
                entities_by_type[entity.label] = []
            entities_by_type[entity.label].append({
                "text": entity.text,
                "confidence": entity.confidence,
                "start": entity.start,
                "end": entity.end
            })
        
        # Extract unique entity texts per type (deduplicate)
        unique_entities = {}
        for entity_type, entity_list in entities_by_type.items():
            # Keep highest confidence for each unique text
            seen_texts = {}
            for entity in entity_list:
                text = entity["text"].lower().strip()
                if text not in seen_texts or entity["confidence"] > seen_texts[text]["confidence"]:
                    seen_texts[text] = entity
            unique_entities[entity_type] = list(seen_texts.values())
        
        return {
            "entities": entities,
            "entities_by_type": unique_entities,
            "entity_count": len(entities),
            "total_confidence": sum(e.confidence for e in entities) / len(entities) if entities else 0
        }
    
    def get_entity_summary(self, extraction_result: Dict[str, Any]) -> str:
        """Generate a summary string of extracted entities.
        
        Args:
            extraction_result: Result from extract_for_conversation
            
        Returns:
            Human-readable summary of entities
        """
        entities_by_type = extraction_result.get("entities_by_type", {})
        
        summary_parts = []
        for entity_type, entities in entities_by_type.items():
            if entities:
                entity_texts = [e["text"] for e in entities]
                summary_parts.append(f"{entity_type}: {', '.join(entity_texts)}")
        
        return "; ".join(summary_parts) if summary_parts else "No entities extracted"


# Global instance for convenience
_entity_extractor = None

def get_entity_extractor() -> EntityExtractor:
    """Get global entity extractor instance (singleton pattern)."""
    global _entity_extractor
    if _entity_extractor is None:
        _entity_extractor = EntityExtractor(preload=True)
    return _entity_extractor
