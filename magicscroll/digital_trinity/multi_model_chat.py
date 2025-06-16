"""
Multi-model chat manager using FIPA ACL for message coordination.

This module implements a chat system that can work with multiple AI models
simultaneously, coordinating their interactions through FIPA ACL messaging.
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple, Callable, Awaitable

from ..config import Config
import logging
from .fipa_acl import FIPAACLMessage, FIPAACLDatabase, get_fipa_acl_db
from .message_adapter import MessageAdapter

logger = logging.getLogger(__name__)

class AgentProfile:
    """Profile for an AI agent/model in the multi-model system."""
    
    def __init__(
        self,
        name: str,
        agent_type: str,
        capabilities: Optional[Dict[str, Any]] = None,
        agent_id: Optional[str] = None
    ):
        """
        Initialize an agent profile.
        
        Args:
            name: Human-readable name for the agent
            agent_type: Type of agent (e.g., 'openai', 'anthropic', 'local')
            capabilities: Dictionary of capabilities this agent has
            agent_id: Optional ID for the agent (will be generated if None)
        """
        self.id = agent_id or str(uuid.uuid4())
        self.name = name
        self.agent_type = agent_type
        self.capabilities = capabilities or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'type': self.agent_type,
            'capabilities': self.capabilities
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentProfile':
        """Create profile from dictionary."""
        return cls(
            name=data['name'],
            agent_type=data['type'],
            capabilities=data.get('capabilities', {}),
            agent_id=data.get('id')
        )


class ModelHandler:
    """Base class for model-specific handlers."""
    
    def __init__(self, agent_profile: AgentProfile):
        """
        Initialize with agent profile.
        
        Args:
            agent_profile: Profile for the agent this handler manages
        """
        self.agent_profile = agent_profile
    
    async def process_message(
        self,
        message: FIPAACLMessage,
        conversation_history: List[FIPAACLMessage]
    ) -> FIPAACLMessage:
        """
        Process a message and generate a response.
        
        Args:
            message: The FIPA ACL message to process
            conversation_history: Previous messages in the conversation
            
        Returns:
            Response message in FIPA ACL format
        """
        raise NotImplementedError("Subclasses must implement process_message")


class OpenAIModelHandler(ModelHandler):
    """Handler for OpenAI models."""
    
    async def process_message(
        self,
        message: FIPAACLMessage,
        conversation_history: List[FIPAACLMessage]
    ) -> FIPAACLMessage:
        """
        Process a message using an OpenAI model and generate a response.
        
        Args:
            message: The FIPA ACL message to process
            conversation_history: Previous messages in the conversation
            
        Returns:
            Response message in FIPA ACL format
        """
        # Convert conversation history to OpenAI format
        openai_messages = [MessageAdapter.fipa_to_openai(msg) for msg in conversation_history]
        
        # Add the current message
        openai_messages.append(MessageAdapter.fipa_to_openai(message))
        
        # This is where we would make the OpenAI API call
        # For now, we'll just return a placeholder response
        openai_response = {
            'role': 'assistant',
            'content': f"OpenAI model {self.agent_profile.name} would process: {message.content}"
        }
        
        # Convert back to FIPA ACL
        response = MessageAdapter.openai_to_fipa(
            openai_response,
            conversation_id=message.conversation_id,
            sender=self.agent_profile.id,
            receiver=message.sender
        )
        response.in_reply_to = message.id
        
        return response


class AnthropicModelHandler(ModelHandler):
    """Handler for Anthropic models."""
    
    async def process_message(
        self,
        message: FIPAACLMessage,
        conversation_history: List[FIPAACLMessage]
    ) -> FIPAACLMessage:
        """
        Process a message using an Anthropic model and generate a response.
        
        Args:
            message: The FIPA ACL message to process
            conversation_history: Previous messages in the conversation
            
        Returns:
            Response message in FIPA ACL format
        """
        # Convert conversation history to Anthropic format
        anthropic_messages = []
        for msg in conversation_history:
            try:
                anthropic_messages.append(MessageAdapter.fipa_to_anthropic(msg))
            except Exception as e:
                logger.warning(f"Error converting message to Anthropic format: {e}")
                # Skip problematic messages or create simplified version
                continue
        
        # Add the current message
        try:
            anthropic_messages.append(MessageAdapter.fipa_to_anthropic(message))
        except Exception as e:
            logger.error(f"Error converting current message to Anthropic format: {e}")
            # Create a simplified fallback message
            anthropic_messages.append({
                'role': 'user',
                'content': [{'type': 'text', 'text': message.content}]
            })
        
        # This is where we would make the Anthropic API call
        # For now, we'll just return a placeholder response
        anthropic_response = {
            'role': 'assistant',
            'content': [{'type': 'text', 'text': f"Anthropic model {self.agent_profile.name} would process: {message.content}"}]
        }
        
        # Convert back to FIPA ACL
        response = MessageAdapter.anthropic_to_fipa(
            anthropic_response,
            conversation_id=message.conversation_id,
            sender=self.agent_profile.id,
            receiver=message.sender
        )
        response.in_reply_to = message.id
        
        return response


class MultiModelChatManager:
    """Manages multi-model conversations using FIPA ACL."""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the chat manager.
        
        Args:
            db_path: Optional path to FIPA ACL database
        """
        self.db = FIPAACLDatabase(db_path) if db_path else get_fipa_acl_db()
        self.handlers: Dict[str, ModelHandler] = {}
        self.active_conversations: Dict[str, List[FIPAACLMessage]] = {}
        
    def register_model(self, handler: ModelHandler) -> str:
        """
        Register a model handler.
        
        Args:
            handler: The model handler to register
            
        Returns:
            The agent ID
        """
        agent_id = handler.agent_profile.id
        self.handlers[agent_id] = handler
        
        # Register agent in database
        self.db.register_agent(
            name=handler.agent_profile.name,
            agent_type=handler.agent_profile.agent_type,
            capabilities=handler.agent_profile.capabilities,
            agent_id=agent_id
        )
        
        logger.info(f"Registered model handler: {handler.agent_profile.name} ({agent_id})")
        return agent_id
    
    def create_conversation(self, title: Optional[str] = None) -> str:
        """
        Create a new conversation.
        
        Args:
            title: Optional title for the conversation
            
        Returns:
            The conversation ID
        """
        conversation_id = self.db.create_conversation(title)
        self.active_conversations[conversation_id] = []
        logger.info(f"Created conversation: {conversation_id}")
        return conversation_id
    
    async def send_message(
        self,
        content: str,
        conversation_id: str,
        sender_id: Optional[str] = None,
        receiver_id: Optional[str] = None,
        performative: str = 'REQUEST'
    ) -> FIPAACLMessage:
        """
        Send a message in a conversation.
        
        Args:
            content: The message content
            conversation_id: The conversation ID
            sender_id: Optional sender ID (defaults to 'user')
            receiver_id: Optional receiver ID
            performative: The FIPA ACL performative
            
        Returns:
            The sent message
        """
        sender_id = sender_id or 'user'
        
        # Create FIPA ACL message
        message = FIPAACLMessage(
            performative=performative,
            sender=sender_id,
            receiver=receiver_id,
            content=content,
            conversation_id=conversation_id
        )
        
        # Save message to database
        self.db.save_message(message)
        
        # Add to active conversation
        if conversation_id not in self.active_conversations:
            self.active_conversations[conversation_id] = []
        self.active_conversations[conversation_id].append(message)
        
        logger.info(f"Sent message in conversation {conversation_id}: {content[:50]}...")
        return message
    
    async def process_message(
        self,
        message: FIPAACLMessage,
        target_agent_id: Optional[str] = None
    ) -> Optional[FIPAACLMessage]:
        """
        Process a message through the appropriate model handler.
        
        Args:
            message: The message to process
            target_agent_id: Optional specific agent to target
            
        Returns:
            The response message if successful
        """
        # Determine which handler to use
        if target_agent_id and target_agent_id in self.handlers:
            handler = self.handlers[target_agent_id]
        elif message.receiver and message.receiver in self.handlers:
            handler = self.handlers[message.receiver]
        else:
            # Default to first available handler
            if not self.handlers:
                logger.error("No model handlers available")
                return None
            handler = next(iter(self.handlers.values()))
        
        # Get conversation history
        conversation_history = self.db.get_conversation_messages(message.conversation_id)
        
        try:
            # Process message through handler
            response = await handler.process_message(message, conversation_history)
            
            # Save response to database
            self.db.save_message(response)
            
            # Add to active conversation
            if message.conversation_id in self.active_conversations:
                self.active_conversations[message.conversation_id].append(response)
            
            logger.info(f"Processed message and generated response: {response.content[:50]}...")
            return response
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return None
    
    def get_conversation_history(self, conversation_id: str) -> List[FIPAACLMessage]:
        """
        Get the full conversation history.
        
        Args:
            conversation_id: The conversation ID
            
        Returns:
            List of messages in the conversation
        """
        return self.db.get_conversation_messages(conversation_id)
    
    def close(self):
        """Close the chat manager and cleanup resources."""
        self.db.close()
        logger.info("Chat manager closed")
