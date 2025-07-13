"""
MagicScroll Message System based on FIPA ACL Standard

This module implements messages following the FIPA Agent Communication 
Language (ACL) standard for MagicScroll conversations.

References:
- FIPA ACL: http://www.fipa.org/specs/fipa00061/SC00061G.html
"""

import json
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class MSMessage:
    """MagicScroll message based on FIPA ACL standard."""
    
    # FIPA ACL Performatives as defined in the standard
    PERFORMATIVES = [
        'ACCEPT_PROPOSAL', 'AGREE', 'CANCEL', 'CFP', 'CONFIRM',
        'DISCONFIRM', 'FAILURE', 'INFORM', 'INFORM_IF', 'INFORM_REF',
        'NOT_UNDERSTOOD', 'PROPOSE', 'QUERY_IF', 'QUERY_REF',
        'REFUSE', 'REJECT_PROPOSAL', 'REQUEST', 'REQUEST_WHEN',
        'REQUEST_WHENEVER', 'SUBSCRIBE'
    ]
    
    def __init__(self, 
                 performative: str, 
                 sender: str, 
                 receiver: Optional[str] = None, 
                 content: Optional[str] = None, 
                 conversation_id: Optional[str] = None, 
                 reply_to: Optional[str] = None, 
                 language: Optional[str] = None, 
                 encoding: Optional[str] = None, 
                 ontology: Optional[str] = None, 
                 protocol: Optional[str] = None, 
                 reply_with: Optional[str] = None, 
                 in_reply_to: Optional[str] = None, 
                 reply_by: Optional[str] = None,
                 message_id: Optional[str] = None):
        """
        Initialize a MagicScroll message following FIPA ACL standard.
        
        Args:
            performative: The performative (type of communicative act)
            sender: The identity of the sender
            receiver: The identity of the intended recipient(s)
            content: The content of the message
            conversation_id: The conversation identifier
            reply_to: The identity of the agent to which replies should be sent
            language: The language in which the content is expressed
            encoding: The specific encoding of the content language expression
            ontology: The ontology(s) used to give meaning to symbols in content
            protocol: The interaction protocol used
            reply_with: An expression used by the sending agent to identify this message
            in_reply_to: The expression referenced in a previous message's reply_with
            reply_by: A time/date expression indicating when a reply should be received
            message_id: Optional ID for the message (will be generated if None)
        """
        
        if performative not in self.PERFORMATIVES:
            raise ValueError(f"Invalid performative: {performative}")
        
        self.id = message_id or str(uuid.uuid4())
        self.performative = performative
        self.sender = sender
        self.receiver = receiver
        self.content = content
        self.conversation_id = conversation_id or str(uuid.uuid4())
        self.reply_to = reply_to
        self.language = language
        self.encoding = encoding
        self.ontology = ontology
        self.protocol = protocol
        self.reply_with = reply_with
        self.in_reply_to = in_reply_to
        self.reply_by = reply_by
        self.created_at = datetime.now().isoformat()
        self.metadata = {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary using schema format"""
        return {
            'message_id': self.id,
            'conversation_id': self.conversation_id,
            'sender': self.sender,
            'receiver': self.receiver,
            'content': self.content,
            'performative': self.performative,
            'created_at': self.created_at,
            'reply_to': self.reply_to,
            'in_reply_to': self.in_reply_to,
            'reply_with': self.reply_with,
            'reply_by': self.reply_by if hasattr(self, 'reply_by') else None,
            'language': self.language if hasattr(self, 'language') else 'en',
            'ontology': self.ontology if hasattr(self, 'ontology') else None,
            'protocol': self.protocol if hasattr(self, 'protocol') else None,
            'conversation_state': getattr(self, 'conversation_state', None),
            'encoding': getattr(self, 'encoding', 'utf-8'),
            'content_length': len(self.content) if self.content else 0
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MSMessage':
        """Create message from dictionary using schema format"""
        # Extract core parameters for the constructor
        msg = cls(
            performative=data['performative'],
            sender=data['sender'],
            receiver=data.get('receiver'),
            content=data.get('content'),
            conversation_id=data.get('conversation_id'),
            reply_with=data.get('reply_with'),
            in_reply_to=data.get('in_reply_to'),
            message_id=data.get('message_id')
        )
        
        # Handle timestamp field 
        if 'created_at' in data:
            msg.created_at = data['created_at']
        elif 'timestamp' in data:
            msg.created_at = data['timestamp']
            
        # Handle metadata if present
        if 'metadata' in data and data['metadata']:
            try:
                msg.metadata = json.loads(data['metadata'])
            except json.JSONDecodeError:
                msg.metadata = {}
                
        return msg
    
    def __str__(self) -> str:
        """String representation of the message."""
        return f"MSMessage(id={self.id[:8]}..., performative={self.performative}, sender={self.sender})"
    
    def __repr__(self) -> str:
        """Detailed representation of the message."""
        return self.__str__()
