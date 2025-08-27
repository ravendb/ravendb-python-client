# RavenDB AI Agent Python Client

A comprehensive Python client for RavenDB's AI Agent functionality, enabling intelligent database interactions through conversational AI interfaces.

## Overview

The RavenDB AI Agent Python client allows you to create intelligent agents that can:

- **Query databases** using natural language through AI-powered tools
- **Execute actions** and handle complex workflows
- **Maintain conversation state** across multiple interactions
- **Integrate with various AI providers** (OpenAI, Azure OpenAI, etc.)
- **Handle tool usage** for database queries and external actions
- **Manage conversation persistence** and token optimization

### Key Features

- 🤖 **Conversational AI**: Natural language database interactions
- 🔧 **Tool Integration**: Query and action tools for database operations
- 💬 **Conversation Management**: Stateful conversations with continuation support
- 🎯 **Type Safety**: Full type hints and dataclass support
- 🐍 **Pythonic API**: Context managers, properties, and idiomatic Python patterns
- 🔒 **Enterprise Ready**: Production-ready with comprehensive error handling

## Installation & Setup

### Prerequisites

1. **RavenDB Server** (v7.1+) with AI Agent support
2. **Python** 3.8 or higher
3. **AI Connection String** configured in RavenDB

### Install RavenDB Python Client

```bash
pip install ravendb
```

### Configure AI Connection String

In RavenDB Studio, configure your AI connection string (e.g., "openai-gpt4"):

```json
{
  "Name": "openai-gpt4",
  "ModelType": "Chat",
  "OpenAiSettings": {
    "ApiKey": "your-openai-api-key",
    "Model": "gpt-4",
    "Endpoint": "https://api.openai.com/v1"
  }
}
```

## Quick Start Guide

Here's a simple example to get you started:

```python
from ravendb import DocumentStore, AiAgentConfiguration

# Initialize document store
with DocumentStore(["http://localhost:8080"], "YourDatabase") as store:
    store.initialize()
    
    # Create a simple AI agent
    config = AiAgentConfiguration(
        name="DatabaseAssistant",
        connection_string_name="openai-gpt4",
        system_prompt="You are a helpful database assistant."
    )
    config.sample_object = '{"response": "Your helpful response"}'
    
    # Add the agent
    result = store.ai.add_or_update_agent(config)
    agent_id = result.identifier
    
    # Start a conversation
    with store.ai.conversation(agent_id) as conversation:
        conversation.set_user_prompt("Hello! Can you help me?")
        result = conversation.run()
        print(f"AI Response: {result.response}")
    
    # Cleanup
    store.ai.delete_agent(agent_id)
```

## Detailed Usage Examples

### Creating and Configuring AI Agents

#### Basic Agent Configuration

```python
from ravendb import (
    AiAgentConfiguration,
    AiAgentToolQuery,
    AiAgentPersistenceConfiguration,
    AiAgentChatTrimmingConfiguration,
    AiAgentSummarizationByTokens
)

# Create comprehensive agent configuration
config = AiAgentConfiguration(
    name="ComprehensiveAgent",
    connection_string_name="openai-gpt4",
    system_prompt="""You are a database expert assistant.
    
You can query the database using available tools. When users ask for data,
use the appropriate tools to retrieve information and explain the results."""
)

# Set output format
config.sample_object = '{"response": "Your response", "action_taken": "What you did"}'

# Add parameters
config.parameters.add("user_id")
config.parameters.add("max_results")

# Add query tool
categories_query = AiAgentToolQuery(
    name="GetCategories",
    description="Retrieves category information from the database",
    query="from Categories select Name, Description limit $limit"
)
categories_query.parameters_sample_object = '{"limit": 10}'
config.queries.append(categories_query)

# Configure persistence
config.persistence = AiAgentPersistenceConfiguration(
    persist_conversation=True,
    persist_user_prompts=True,
    persist_ai_responses=True
)

# Configure chat trimming
trimming = AiAgentChatTrimmingConfiguration()
trimming.tokens = AiAgentSummarizationByTokens()
trimming.tokens.max_tokens_before_summarization = 16000
trimming.tokens.max_tokens_after_summarization = 4000
config.chat_trimming = trimming

# Create the agent
result = store.ai.add_or_update_agent(config)
agent_id = result.identifier
```

#### Adding Action Tools

```python
from ravendb import AiAgentToolAction

# Create action tool for external operations
email_action = AiAgentToolAction(
    name="SendEmail",
    description="Sends an email notification to users"
)
email_action.parameters_sample_object = '{"to": "user@example.com", "subject": "Subject", "body": "Message"}'
config.actions.append(email_action)
```

### Starting Conversations and Handling Responses

#### Basic Conversation

```python
# Start a new conversation
conversation = store.ai.conversation(agent_id, {
    "user_id": "user123",
    "max_results": 10
})

conversation.set_user_prompt("Show me some product categories")
result = conversation.run()

print(f"Response: {result.response}")
print(f"Conversation ID: {result.conversation_id}")
print(f"Token Usage: {result.usage.total_tokens if result.usage else 'N/A'}")
```

#### Handling Action Requests

```python
conversation.set_user_prompt("Send me an email with the category list")
result = conversation.run()

# Check if AI requested actions
if result.has_action_requests:
    print("AI requested actions:")
    for action in result.action_requests:
        print(f"- Tool: {action.name}")
        print(f"- Arguments: {action.arguments}")

        # Handle the action (implement your logic here)
        if action.name == "SendEmail":
            # Process email sending
            email_result = send_email_implementation(action.arguments)

            # Provide response back to AI
            conversation.add_action_response(action.tool_id, email_result)

    # Continue conversation with action results
    result = conversation.run()
    print(f"Final response: {result.response}")
```

### Conversation Continuation and State Management

#### Continuing Existing Conversations

```python
# Continue an existing conversation
existing_conversation = store.ai.conversation_with_id(
    conversation_id="conversations/12345-A",
    change_vector="A:123-xyz"
)

existing_conversation.set_user_prompt("What else can you tell me?")
result = existing_conversation.run()

print(f"Continued response: {result.response}")
print(f"Updated change vector: {result.change_vector}")
```

#### Using Context Managers for Automatic Cleanup

```python
# Context manager automatically handles cleanup
with store.ai.conversation(agent_id) as conversation:
    conversation.set_user_prompt("Hello!")
    result = conversation.run()

    # Check required actions as property
    if conversation.required_actions:
        print(f"Actions needed: {len(conversation.required_actions)}")

    # Conversation is automatically cleaned up on exit
```

### Error Handling and Best Practices

#### Comprehensive Error Handling

```python
from ravendb.exceptions import RavenException

try:
    # Create conversation
    conversation = store.ai.conversation(agent_id)

    # Validate input
    user_input = input("Enter your question: ").strip()
    if not user_input:
        raise ValueError("Question cannot be empty")

    conversation.set_user_prompt(user_input)
    result = conversation.run()

    # Handle response
    if result.response:
        print(f"AI: {result.response}")
    else:
        print("No response received from AI")

except ValueError as e:
    print(f"Input error: {e}")
except RavenException as e:
    print(f"RavenDB error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

#### Production Best Practices

```python
import logging
from typing import Optional

class ProductionAiAgent:
    def __init__(self, store: DocumentStore, agent_id: str):
        self.store = store
        self.agent_id = agent_id
        self.logger = logging.getLogger(__name__)

    def safe_conversation(self, prompt: str, parameters: Optional[dict] = None) -> Optional[dict]:
        """Safely execute a conversation with comprehensive error handling."""
        try:
            with self.store.ai.conversation(self.agent_id, parameters) as conversation:
                conversation.set_user_prompt(prompt)
                result = conversation.run()

                # Log usage for monitoring
                if result.usage:
                    self.logger.info(f"Token usage: {result.usage.total_tokens}")

                return {
                    "response": result.response,
                    "conversation_id": result.conversation_id,
                    "success": True
                }

        except Exception as e:
            self.logger.error(f"Conversation failed: {e}")
            return {
                "error": str(e),
                "success": False
            }

# Usage
agent = ProductionAiAgent(store, agent_id)
result = agent.safe_conversation("What categories do we have?")
if result["success"]:
    print(result["response"])
else:
    print(f"Error: {result['error']}")
```

## API Reference

### Core Classes

#### `AiOperations`

Main entry point for AI agent operations.

```python
# Access via document store
ai_ops = store.ai

# Methods
ai_ops.add_or_update_agent(configuration, schema_type=None) -> AiAgentConfigurationResult
ai_ops.delete_agent(identifier) -> AiAgentConfigurationResult
ai_ops.get_agents(agent_id=None) -> GetAiAgentsResponse
ai_ops.conversation(agent_id, parameters=None) -> AiConversation
ai_ops.conversation_with_id(conversation_id, change_vector=None) -> AiConversation
```

#### `AiConversation`

Manages individual conversations with AI agents.

```python
# Properties
conversation.required_actions -> List[AiAgentActionRequest]  # Read-only property

# Methods
conversation.set_user_prompt(prompt: str) -> None
conversation.add_action_response(action_id: str, response: Union[str, Any]) -> None
conversation.run() -> AiConversationResult

# Context manager support
with store.ai.conversation(agent_id) as conversation:
    # Automatic cleanup on exit
    pass
```

#### `AiConversationResult`

Contains the result of a conversation turn.

```python
# Properties
result.conversation_id: Optional[str]
result.change_vector: Optional[str]
result.response: Optional[Any]
result.usage: Optional[AiUsage]
result.action_requests: List[AiAgentActionRequest]

# Methods
result.has_action_requests -> bool  # Property
result.get_action_request_by_id(action_id: str) -> Optional[AiAgentActionRequest]

# String representations
str(result)   # Brief summary
repr(result)  # Detailed representation
```

#### `AiAgentConfiguration`

Defines agent configuration and capabilities.

```python
config = AiAgentConfiguration(
    name="AgentName",
    connection_string_name="ai-connection",
    system_prompt="Your system prompt"
)

# Properties
config.identifier: Optional[str]
config.sample_object: Optional[str]
config.output_schema: Optional[str]
config.queries: List[AiAgentToolQuery]
config.actions: List[AiAgentToolAction]
config.parameters: Set[str]
config.persistence: Optional[AiAgentPersistenceConfiguration]
config.chat_trimming: Optional[AiAgentChatTrimmingConfiguration]
config.max_model_iterations_per_call: Optional[int]
```

### Data Classes

#### `AiUsage` (Dataclass)

Token usage statistics.

```python
@dataclass
class AiUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cached_tokens: int = 0
```

#### `AiAgentActionRequest` (Dataclass)

Represents an action request from the AI.

```python
@dataclass
class AiAgentActionRequest:
    name: Optional[str] = None
    tool_id: Optional[str] = None
    arguments: Optional[str] = None
```

#### `AiAgentActionResponse` (Dataclass)

Response to an AI action request.

```python
@dataclass
class AiAgentActionResponse:
    tool_id: Optional[str] = None
    content: Optional[str] = None
```

## Configuration Options

### Agent Configuration

#### Basic Settings

```python
config = AiAgentConfiguration(
    name="MyAgent",                          # Agent display name
    connection_string_name="ai-provider",    # AI connection string name
    system_prompt="Your system prompt"       # Instructions for the AI
)
```

#### Output Configuration

```python
# JSON sample for response format
config.sample_object = '{"response": "text", "confidence": 0.95}'

# JSON schema for strict typing
config.output_schema = '''
{
  "type": "object",
  "properties": {
    "response": {"type": "string"},
    "confidence": {"type": "number"}
  }
}
'''
```

#### Tool Configuration

```python
# Query tools for database operations
query_tool = AiAgentToolQuery(
    name="GetProducts",
    description="Retrieves product information",
    query="from Products where Category == $category select Name, Price"
)
query_tool.parameters_sample_object = '{"category": "Electronics"}'
config.queries.append(query_tool)

# Action tools for external operations
action_tool = AiAgentToolAction(
    name="SendNotification",
    description="Sends notifications to users"
)
action_tool.parameters_sample_object = '{"message": "text", "recipients": ["email"]}'
config.actions.append(action_tool)
```

#### Persistence Configuration

```python
from ravendb import AiAgentPersistenceConfiguration

config.persistence = AiAgentPersistenceConfiguration(
    persist_conversation=True,      # Save conversation history
    persist_user_prompts=True,      # Save user inputs
    persist_ai_responses=True       # Save AI responses
)
```

#### Chat Trimming Configuration

```python
from ravendb import (
    AiAgentChatTrimmingConfiguration,
    AiAgentSummarizationByTokens,
    AiAgentTruncateChat,
    AiAgentHistoryConfiguration
)

# Token-based summarization
trimming = AiAgentChatTrimmingConfiguration()
trimming.tokens = AiAgentSummarizationByTokens()
trimming.tokens.max_tokens_before_summarization = 32000
trimming.tokens.max_tokens_after_summarization = 8000
trimming.tokens.summarization_task_beginning_prompt = "Summarize the conversation:"

# Simple truncation
trimming.truncate = AiAgentTruncateChat()
trimming.truncate.max_messages = 50

# History-based trimming
trimming.history = AiAgentHistoryConfiguration()
trimming.history.max_age_in_minutes = 1440  # 24 hours

config.chat_trimming = trimming
```

## Best Practices

### 1. Agent Design

#### Clear System Prompts

```python
# Good: Specific and actionable
system_prompt = """You are a customer service assistant for an e-commerce platform.

You can:
- Query product information using GetProducts tool
- Check order status using GetOrders tool
- Send notifications using SendEmail tool

Always be helpful and provide specific information. When using tools,
explain what you're doing and what the results mean."""

# Avoid: Vague or overly broad prompts
system_prompt = "You are a helpful assistant."
```

#### Structured Output Formats

```python
# Good: Structured response format
config.sample_object = '''
{
  "response": "Your helpful response to the user",
  "action_taken": "Description of what you did",
  "confidence": 0.95,
  "follow_up_needed": false
}
'''

# This helps ensure consistent, parseable responses
```

### 2. Conversation Management

#### Use Context Managers

```python
# Good: Automatic cleanup
with store.ai.conversation(agent_id) as conversation:
    conversation.set_user_prompt(user_input)
    result = conversation.run()
    return result.response

# Avoid: Manual resource management
conversation = store.ai.conversation(agent_id)
# ... (easy to forget cleanup)
```

#### Handle Long Conversations

```python
# Configure appropriate chat trimming
trimming = AiAgentChatTrimmingConfiguration()
trimming.tokens = AiAgentSummarizationByTokens()
trimming.tokens.max_tokens_before_summarization = 16000  # Adjust based on model
trimming.tokens.max_tokens_after_summarization = 4000

config.chat_trimming = trimming
```

### 3. Error Handling

#### Validate Inputs

```python
def safe_prompt(conversation, user_input: str):
    # Validate input
    if not user_input or user_input.isspace():
        raise ValueError("Input cannot be empty")

    if len(user_input) > 10000:  # Reasonable limit
        raise ValueError("Input too long")

    conversation.set_user_prompt(user_input)
    return conversation.run()
```

#### Handle Action Failures

```python
def handle_actions(conversation, result):
    for action in result.action_requests:
        try:
            # Implement your action logic
            action_result = execute_action(action)
            conversation.add_action_response(action.tool_id, action_result)
        except Exception as e:
            # Provide error feedback to AI
            error_response = f"Action failed: {str(e)}"
            conversation.add_action_response(action.tool_id, error_response)

    return conversation.run()
```

### 4. Production Deployment

#### Monitor Token Usage

```python
def monitor_conversation(conversation, prompt):
    result = conversation.run()

    if result.usage:
        # Log for monitoring
        logger.info(f"Tokens used: {result.usage.total_tokens}")

        # Alert on high usage
        if result.usage.total_tokens > 10000:
            logger.warning(f"High token usage: {result.usage.total_tokens}")

    return result
```

#### Implement Rate Limiting

```python
from time import time
from collections import defaultdict

class RateLimiter:
    def __init__(self, max_requests_per_minute=60):
        self.max_requests = max_requests_per_minute
        self.requests = defaultdict(list)

    def allow_request(self, user_id: str) -> bool:
        now = time()
        minute_ago = now - 60

        # Clean old requests
        self.requests[user_id] = [
            req_time for req_time in self.requests[user_id]
            if req_time > minute_ago
        ]

        # Check limit
        if len(self.requests[user_id]) >= self.max_requests:
            return False

        self.requests[user_id].append(now)
        return True

# Usage
rate_limiter = RateLimiter(max_requests_per_minute=30)

if not rate_limiter.allow_request(user_id):
    raise Exception("Rate limit exceeded")
```

#### Secure Configuration

```python
# Store sensitive configuration securely
import os

config = AiAgentConfiguration(
    name="ProductionAgent",
    connection_string_name=os.getenv("AI_CONNECTION_NAME"),
    system_prompt=load_prompt_from_secure_storage()
)

# Don't hardcode API keys or sensitive prompts in code
```

### 5. Testing

#### Unit Testing

```python
import unittest
from unittest.mock import Mock, patch

class TestAiAgent(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.agent_id = "test-agent"

    def test_conversation_creation(self):
        # Mock the conversation
        mock_conversation = Mock()
        self.store.ai.conversation.return_value = mock_conversation

        # Test conversation creation
        conversation = self.store.ai.conversation(self.agent_id)
        self.store.ai.conversation.assert_called_once_with(self.agent_id)

    @patch('your_module.store')
    def test_agent_response(self, mock_store):
        # Test actual agent responses
        # ... implementation
        pass
```

## Troubleshooting

### Common Issues

1. **"Agent ID is required"**: Ensure you're providing a valid agent ID when creating conversations
2. **"User prompt cannot be empty"**: Validate user input before setting prompts
3. **Token limit exceeded**: Configure appropriate chat trimming settings
4. **Action tool not found**: Verify tool names match exactly between configuration and usage
5. **Connection string not found**: Ensure AI connection string is properly configured in RavenDB


For more information, visit the [RavenDB Documentation](https://docs.ravendb.net) or check the [Python Client GitHub Repository](https://github.com/ravendb/ravendb-python-client).
