# chainlit_app.py - ChainLit chat application
import chainlit as cl
from typing import Optional

@cl.on_chat_start
async def start():
    """Initialize the chat session"""
    await cl.Message(
        content="Hello! I'm your AI assistant. How can I help you today?",
        author="Assistant"
    ).send()
    
    # Store user session data
    cl.user_session.set("message_count", 0)

@cl.on_message
async def main(message: cl.Message):
    """Handle incoming messages"""
    
    # Get current message count
    count = cl.user_session.get("message_count", 0)
    count += 1
    cl.user_session.set("message_count", count)
    
    # Show typing indicator
    async with cl.Step(name="thinking") as step:
        step.output = "Processing your message..."
        
        # Simple response logic (you can replace this with actual AI/LLM integration)
        user_message = message.content.lower()
        
        if "hello" in user_message or "hi" in user_message:
            response = f"Hello! Nice to meet you. This is message #{count} in our conversation."
        elif "how are you" in user_message:
            response = "I'm doing great, thank you for asking! How can I assist you today?"
        elif "help" in user_message:
            response = """I can help you with various tasks:
            
• Answer questions
• Provide information
• Have conversations
• Assist with problem-solving

What would you like help with?"""
        elif "count" in user_message:
            response = f"We've exchanged {count} messages so far in this conversation."
        else:
            response = f"I understand you said: '{message.content}'. That's interesting! Is there anything specific you'd like to know or discuss about this topic?"
    
    # Send the response
    await cl.Message(
        content=response,
        author="Assistant"
    ).send()

@cl.on_stop
async def stop():
    """Handle session end"""
    print("Chat session ended")

@cl.author_rename
def rename(orig_author: str):
    """Rename message authors"""
    rename_dict = {
        "Assistant": "🤖 AI Assistant",
        "User": "👤 You"
    }
    return rename_dict.get(orig_author, orig_author)

# Optional: Add custom styling
@cl.on_settings_update
async def setup_agent(settings):
    """Handle settings updates"""
    print("Settings updated:", settings)
