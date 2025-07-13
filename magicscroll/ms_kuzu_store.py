"""Kuzu graph database operations with updated entity-focused schema."""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def normalize_entity_name(name: str) -> str:
    """Normalize entity name for deduplication."""
    return name.strip().lower()


def categorize_entity(entity_text: str, entity_type: str) -> str:
    """Categorize entities for better organization."""
    entity_lower = entity_text.lower()
    
    if entity_type == 'technology':
        # Basic tech categorization
        if any(term in entity_lower for term in ['python', 'javascript', 'rust', 'go', 'java', 'cpp', 'c++']):
            return 'programming_language'
        elif any(term in entity_lower for term in ['react', 'vue', 'angular', 'svelte']):
            return 'frontend_framework'
        elif any(term in entity_lower for term in ['django', 'flask', 'fastapi', 'express']):
            return 'backend_framework'
        elif any(term in entity_lower for term in ['postgres', 'mysql', 'mongodb', 'redis']):
            return 'database'
        elif any(term in entity_lower for term in ['aws', 'gcp', 'azure', 'docker', 'kubernetes']):
            return 'infrastructure'
        else:
            return 'general_tech'
    
    elif entity_type == 'topic':
        # Basic topic categorization
        if any(term in entity_lower for term in ['machine learning', 'ai', 'neural', 'model']):
            return 'ai_ml'
        elif any(term in entity_lower for term in ['web', 'frontend', 'backend', 'api']):
            return 'web_development'
        elif any(term in entity_lower for term in ['business', 'strategy', 'market']):
            return 'business'
        elif any(term in entity_lower for term in ['design', 'ux', 'ui']):
            return 'design'
        else:
            return 'general'
    
    return 'general'


def store_entities_in_graph(entities: List[Dict[str, Any]], conversation_id: str, 
                           entry_id: str, conversation_title: str = "") -> Dict[str, int]:
    """Store entities and their relationships to MSEntry in Kuzu.
    
    Updated to use the new entity-focused schema with MSEntry nodes.
    
    Args:
        entities: List of GLiNER entity dictionaries
        conversation_id: UUID of the conversation
        entry_id: UUID of the MSEntry
        conversation_title: Title/summary of the conversation
        
    Returns:
        Dictionary with counts of entities stored by type
    """
    counts = {"person": 0, "organization": 0, "technology": 0, "topic": 0}
    
    try:
        # Import here to avoid circular imports
        from .stores import storage
        kuzu_conn = storage.kuzu
        
        # First, ensure MSEntry exists
        _store_ms_entry(kuzu_conn, entry_id, conversation_id, conversation_title)
        
        for entity in entities:
            entity_type = entity.get('label', '').lower()
            entity_text = entity.get('text', '').strip()
            confidence = entity.get('score', 0.0)
            
            if not entity_text:
                continue
            
            normalized_name = normalize_entity_name(entity_text)
            
            if entity_type == 'person':
                _store_person_entity(kuzu_conn, entity_text, normalized_name, confidence)
                _store_person_relationship(kuzu_conn, normalized_name, entry_id, confidence)
                counts["person"] += 1
                
            elif entity_type == 'organization':
                _store_organization_entity(kuzu_conn, entity_text, normalized_name, confidence)
                _store_org_relationship(kuzu_conn, normalized_name, entry_id, confidence)
                counts["organization"] += 1
                
            # Enhanced: Map location to technology for now (can be refined)
            elif entity_type in ['location', 'technology', 'misc']:
                # Classify as either technology or topic based on content
                if _is_technology_term(entity_text):
                    category = categorize_entity(entity_text, 'technology')
                    _store_technology_entity(kuzu_conn, entity_text, normalized_name, confidence, category)
                    _store_tech_relationship(kuzu_conn, normalized_name, entry_id, confidence)
                    counts["technology"] += 1
                else:
                    category = categorize_entity(entity_text, 'topic')
                    _store_topic_entity(kuzu_conn, entity_text, normalized_name, confidence, category)
                    _store_topic_relationship(kuzu_conn, normalized_name, entry_id, confidence)
                    counts["topic"] += 1
        
        logger.info(f"Stored entities in graph for MSEntry {entry_id}: {counts}")
        return counts
        
    except Exception as e:
        logger.error(f"Error storing entities in graph: {e}")
        return {"person": 0, "organization": 0, "technology": 0, "topic": 0}


def _is_technology_term(text: str) -> bool:
    """Determine if a term is technology-related."""
    tech_indicators = [
        # Programming languages
        'python', 'javascript', 'typescript', 'rust', 'go', 'java', 'cpp', 'c++', 'ruby', 'php',
        # Frameworks
        'react', 'vue', 'angular', 'django', 'flask', 'fastapi', 'express', 'next.js', 'nuxt',
        # Databases
        'postgres', 'postgresql', 'mysql', 'mongodb', 'redis', 'sqlite', 'elasticsearch',
        # Infrastructure
        'aws', 'gcp', 'azure', 'docker', 'kubernetes', 'terraform', 'ansible',
        # Tools
        'git', 'github', 'gitlab', 'jenkins', 'circleci', 'webpack', 'vite', 'babel',
        # AI/ML
        'pytorch', 'tensorflow', 'transformers', 'openai', 'anthropic', 'huggingface'
    ]
    
    text_lower = text.lower()
    return any(indicator in text_lower for indicator in tech_indicators)


def _store_ms_entry(kuzu_conn, entry_id: str, conversation_id: str, title: str):
    """Store or update MSEntry node."""
    try:
        current_time = datetime.now()  # Use datetime object, not ISO string
        kuzu_conn.execute("""
            MERGE (e:MSEntry {entry_id: $entry_id})
            ON CREATE SET 
                e.conversation_id = $conv_id,
                e.entry_type = 'conversation',
                e.title = $title,
                e.created_at = $created_at,
                e.token_count = 0
            ON MATCH SET
                e.title = $title
        """, {
            "entry_id": entry_id,
            "conv_id": conversation_id,
            "title": title,
            "created_at": current_time
        })
    except Exception as e:
        logger.error(f"Error storing MSEntry: {e}")


def _store_person_entity(kuzu_conn, name: str, normalized_name: str, confidence: float):
    """Store or update person entity with enhanced tracking."""
    try:
        current_time = datetime.now()  # Use datetime object, not ISO string
        kuzu_conn.execute("""
            MERGE (p:Person {normalized_name: $norm_name})
            ON CREATE SET 
                p.name = $name,
                p.confidence = $conf,
                p.first_seen = $time,
                p.last_seen = $time,
                p.mention_count = 1
            ON MATCH SET
                p.confidence = CASE WHEN $conf > p.confidence THEN $conf ELSE p.confidence END,
                p.last_seen = $time,
                p.mention_count = p.mention_count + 1
        """, {
            "norm_name": normalized_name,
            "name": name,
            "conf": confidence,
            "time": current_time
        })
    except Exception as e:
        logger.error(f"Error storing person {name}: {e}")


def _store_organization_entity(kuzu_conn, name: str, normalized_name: str, confidence: float):
    """Store or update organization entity with enhanced tracking."""
    try:
        current_time = datetime.now()  # Use datetime object, not ISO string
        kuzu_conn.execute("""
            MERGE (o:Organization {normalized_name: $norm_name})
            ON CREATE SET 
                o.name = $name,
                o.confidence = $conf,
                o.first_seen = $time,
                o.last_seen = $time,
                o.mention_count = 1
            ON MATCH SET
                o.confidence = CASE WHEN $conf > o.confidence THEN $conf ELSE o.confidence END,
                o.last_seen = $time,
                o.mention_count = o.mention_count + 1
        """, {
            "norm_name": normalized_name,
            "name": name,
            "conf": confidence,
            "time": current_time
        })
    except Exception as e:
        logger.error(f"Error storing organization {name}: {e}")


def _store_technology_entity(kuzu_conn, name: str, normalized_name: str, confidence: float, category: str):
    """Store or update technology entity with category."""
    try:
        current_time = datetime.now()  # Use datetime object, not ISO string
        kuzu_conn.execute("""
            MERGE (t:Technology {normalized_name: $norm_name})
            ON CREATE SET 
                t.name = $name,
                t.category = $category,
                t.confidence = $conf,
                t.first_seen = $time,
                t.last_seen = $time,
                t.mention_count = 1
            ON MATCH SET
                t.confidence = CASE WHEN $conf > t.confidence THEN $conf ELSE t.confidence END,
                t.last_seen = $time,
                t.mention_count = t.mention_count + 1
        """, {
            "norm_name": normalized_name,
            "name": name,
            "category": category,
            "conf": confidence,
            "time": current_time
        })
    except Exception as e:
        logger.error(f"Error storing technology {name}: {e}")


def _store_topic_entity(kuzu_conn, name: str, normalized_name: str, confidence: float, category: str):
    """Store or update topic entity with category."""
    try:
        current_time = datetime.now()  # Use datetime object, not ISO string
        kuzu_conn.execute("""
            MERGE (t:Topic {normalized_name: $norm_name})
            ON CREATE SET 
                t.name = $name,
                t.category = $category,
                t.confidence = $conf,
                t.first_seen = $time,
                t.last_seen = $time,
                t.mention_count = 1
            ON MATCH SET
                t.confidence = CASE WHEN $conf > t.confidence THEN $conf ELSE t.confidence END,
                t.last_seen = $time,
                t.mention_count = t.mention_count + 1
        """, {
            "norm_name": normalized_name,
            "name": name,
            "category": category,
            "conf": confidence,
            "time": current_time
        })
    except Exception as e:
        logger.error(f"Error storing topic {name}: {e}")


def _store_person_relationship(kuzu_conn, person_normalized: str, entry_id: str, confidence: float):
    """Store person-MSEntry relationship."""
    try:
        kuzu_conn.execute("""
            MATCH (p:Person {normalized_name: $person}), (e:MSEntry {entry_id: $entry})
            MERGE (p)-[r:DISCUSSED_IN]->(e)
            ON CREATE SET 
                r.confidence = $conf,
                r.context = 'conversation',
                r.sentiment = 'neutral',
                r.mentioned_count = 1
            ON MATCH SET
                r.mentioned_count = r.mentioned_count + 1,
                r.confidence = CASE WHEN $conf > r.confidence THEN $conf ELSE r.confidence END
        """, {
            "person": person_normalized,
            "entry": entry_id,
            "conf": confidence
        })
    except Exception as e:
        logger.debug(f"Person relationship creation: {e}")


def _store_org_relationship(kuzu_conn, org_normalized: str, entry_id: str, confidence: float):
    """Store organization-MSEntry relationship."""
    try:
        kuzu_conn.execute("""
            MATCH (o:Organization {normalized_name: $org}), (e:MSEntry {entry_id: $entry})
            MERGE (o)-[r:ORG_IN]->(e)
            ON CREATE SET 
                r.confidence = $conf,
                r.context = 'conversation',
                r.relationship_type = 'mentioned'
            ON MATCH SET
                r.confidence = CASE WHEN $conf > r.confidence THEN $conf ELSE r.confidence END
        """, {
            "org": org_normalized,
            "entry": entry_id,
            "conf": confidence
        })
    except Exception as e:
        logger.debug(f"Org relationship creation: {e}")


def _store_tech_relationship(kuzu_conn, tech_normalized: str, entry_id: str, confidence: float):
    """Store technology-MSEntry relationship."""
    try:
        kuzu_conn.execute("""
            MATCH (t:Technology {normalized_name: $tech}), (e:MSEntry {entry_id: $entry})
            MERGE (t)-[r:TECH_IN]->(e)
            ON CREATE SET 
                r.confidence = $conf,
                r.usage_context = 'discussion',
                r.proficiency_level = 'unknown'
            ON MATCH SET
                r.confidence = CASE WHEN $conf > r.confidence THEN $conf ELSE r.confidence END
        """, {
            "tech": tech_normalized,
            "entry": entry_id,
            "conf": confidence
        })
    except Exception as e:
        logger.debug(f"Tech relationship creation: {e}")


def _store_topic_relationship(kuzu_conn, topic_normalized: str, entry_id: str, confidence: float):
    """Store topic-MSEntry relationship."""
    try:
        kuzu_conn.execute("""
            MATCH (t:Topic {normalized_name: $topic}), (e:MSEntry {entry_id: $entry})
            MERGE (t)-[r:TOPIC_IN]->(e)
            ON CREATE SET 
                r.confidence = $conf,
                r.importance_level = 'medium',
                r.discussion_depth = 'surface'
            ON MATCH SET
                r.confidence = CASE WHEN $conf > r.confidence THEN $conf ELSE r.confidence END
        """, {
            "topic": topic_normalized,
            "entry": entry_id,
            "conf": confidence
        })
    except Exception as e:
        logger.debug(f"Topic relationship creation: {e}")


def search_entities_by_entry(entry_id: str) -> Dict[str, List[Dict]]:
    """Find all entities mentioned in a specific MSEntry."""
    try:
        from .stores import storage
        kuzu_conn = storage.kuzu
        result = {"people": [], "organizations": [], "technologies": [], "topics": []}
        
        # Get people
        try:
            people_result = kuzu_conn.execute("""
                MATCH (p:Person)-[r:DISCUSSED_IN]->(e:MSEntry {entry_id: $entry_id})
                RETURN p.name, p.confidence, r.confidence as mention_confidence, r.sentiment
            """, {"entry_id": entry_id})
            result["people"] = [dict(row) for row in people_result.get_as_df().to_dict('records')]
        except Exception as e:
            logger.error(f"Error getting people for entry: {e}")
        
        # Get organizations  
        try:
            orgs_result = kuzu_conn.execute("""
                MATCH (o:Organization)-[r:ORG_IN]->(e:MSEntry {entry_id: $entry_id})
                RETURN o.name, o.confidence, r.confidence as mention_confidence, r.relationship_type
            """, {"entry_id": entry_id})
            result["organizations"] = [dict(row) for row in orgs_result.get_as_df().to_dict('records')]
        except Exception as e:
            logger.error(f"Error getting organizations for entry: {e}")
        
        # Get technologies
        try:
            tech_result = kuzu_conn.execute("""
                MATCH (t:Technology)-[r:TECH_IN]->(e:MSEntry {entry_id: $entry_id})
                RETURN t.name, t.category, t.confidence, r.confidence as mention_confidence, r.usage_context
            """, {"entry_id": entry_id})
            result["technologies"] = [dict(row) for row in tech_result.get_as_df().to_dict('records')]
        except Exception as e:
            logger.error(f"Error getting technologies for entry: {e}")
        
        # Get topics
        try:
            topics_result = kuzu_conn.execute("""
                MATCH (t:Topic)-[r:TOPIC_IN]->(e:MSEntry {entry_id: $entry_id})
                RETURN t.name, t.category, t.confidence, r.confidence as mention_confidence, r.importance_level
            """, {"entry_id": entry_id})
            result["topics"] = [dict(row) for row in topics_result.get_as_df().to_dict('records')]
        except Exception as e:
            logger.error(f"Error getting topics for entry: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error searching entities by entry: {e}")
        return {"people": [], "organizations": [], "technologies": [], "topics": []}


def get_entity_stats() -> Dict[str, int]:
    """Get counts of entities by type using new schema."""
    try:
        from .stores import storage
        kuzu_conn = storage.kuzu
        stats = {}
        
        try:
            people_count = kuzu_conn.execute("MATCH (p:Person) RETURN count(p) as count").get_as_df().iloc[0]['count']
            stats["people"] = people_count
        except:
            stats["people"] = 0
            
        try:
            orgs_count = kuzu_conn.execute("MATCH (o:Organization) RETURN count(o) as count").get_as_df().iloc[0]['count']
            stats["organizations"] = orgs_count
        except:
            stats["organizations"] = 0
            
        try:
            tech_count = kuzu_conn.execute("MATCH (t:Technology) RETURN count(t) as count").get_as_df().iloc[0]['count']
            stats["technologies"] = tech_count
        except:
            stats["technologies"] = 0
            
        try:
            topics_count = kuzu_conn.execute("MATCH (t:Topic) RETURN count(t) as count").get_as_df().iloc[0]['count']
            stats["topics"] = topics_count
        except:
            stats["topics"] = 0
            
        try:
            entries_count = kuzu_conn.execute("MATCH (e:MSEntry) RETURN count(e) as count").get_as_df().iloc[0]['count']
            stats["ms_entries"] = entries_count
        except:
            stats["ms_entries"] = 0
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting entity stats: {e}")
        return {"people": 0, "organizations": 0, "technologies": 0, "topics": 0, "ms_entries": 0}


def search_related_entries_by_entity(entity_name: str, entity_type: str, limit: int = 10) -> List[Dict]:
    """Find MSEntries related to a specific entity."""
    try:
        from .stores import storage
        kuzu_conn = storage.kuzu
        
        normalized_name = normalize_entity_name(entity_name)
        
        if entity_type.lower() == 'person':
            query = """
                MATCH (p:Person {normalized_name: $name})-[r:DISCUSSED_IN]->(e:MSEntry)
                RETURN e.entry_id, e.conversation_id, e.title, e.created_at, r.confidence
                ORDER BY r.confidence DESC
                LIMIT $limit
            """
        elif entity_type.lower() == 'organization':
            query = """
                MATCH (o:Organization {normalized_name: $name})-[r:ORG_IN]->(e:MSEntry)
                RETURN e.entry_id, e.conversation_id, e.title, e.created_at, r.confidence
                ORDER BY r.confidence DESC
                LIMIT $limit
            """
        elif entity_type.lower() == 'technology':
            query = """
                MATCH (t:Technology {normalized_name: $name})-[r:TECH_IN]->(e:MSEntry)
                RETURN e.entry_id, e.conversation_id, e.title, e.created_at, r.confidence
                ORDER BY r.confidence DESC
                LIMIT $limit
            """
        elif entity_type.lower() == 'topic':
            query = """
                MATCH (t:Topic {normalized_name: $name})-[r:TOPIC_IN]->(e:MSEntry)
                RETURN e.entry_id, e.conversation_id, e.title, e.created_at, r.confidence
                ORDER BY r.confidence DESC
                LIMIT $limit
            """
        else:
            return []
        
        result = kuzu_conn.execute(query, {"name": normalized_name, "limit": limit})
        return [dict(row) for row in result.get_as_df().to_dict('records')]
        
    except Exception as e:
        logger.error(f"Error searching related entries: {e}")
        return []
