import streamlit as st
import pandas as pd
from datetime import datetime

# Fixed bottom chatbar styles (fallback) and layout script
st.markdown(
    """
    <style>
    :root { --chatbar-max-width: 900px; }
    /* Fix the row containing st.chat_input to the bottom */
    div[data-testid="stHorizontalBlock"]:has([data-testid="stChatInput"]) {
      position: fixed !important;
      left: 50%;
      transform: translateX(-50%);
      width: min(var(--chatbar-max-width), 100vw);
      bottom: 0;
      z-index: 1001;
      border-top: 1px solid rgba(0,0,0,0.06);
      padding: 0.6rem 1rem;
      margin: 0;
      box-sizing: border-box;
      display: flex;
      align-items: center;
      column-gap: 14px;
      background: transparent;
      padding-left: 100px; /* nudge the whole row slightly to the right */
    }
    div[data-testid="stHorizontalBlock"]:has([data-testid="stChatInput"]) [data-testid="column"]:first-child {
      flex: 1 1 auto !important; min-width: 0 !important; max-width: 100% !important;
    }
    div[data-testid="stHorizontalBlock"]:has([data-testid="stChatInput"]) [data-testid="column"]:nth-child(2),
    div[data-testid="stHorizontalBlock"]:has([data-testid="stChatInput"]) [data-testid="column"]:nth-child(3) {
      flex: 0 0 auto !important; min-width: 48px !important;
    }
    div[data-testid="stHorizontalBlock"]:has([data-testid="stChatInput"]) [data-testid="column"]:nth-child(3) {
      padding-left: 8px !important;
    }
    /* Give main content room above the fixed bar */
    .main { padding-bottom: 160px; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <script>
    (function() {
      function updateContentBounds() {
        try {
          const root = document.documentElement;
          const sidebar = document.querySelector('aside[data-testid="stSidebar"]');
          const main = document.querySelector('section.main');
          let left = 0;
          let width = window.innerWidth;
          if (sidebar && main) {
            const mainRect = main.getBoundingClientRect();
            left = Math.max(0, mainRect.left);
            width = Math.max(320, mainRect.width);
          }
          root.style.setProperty('--content-left', left + 'px');
          root.style.setProperty('--content-width', width + 'px');
        } catch (e) {}
      }
      new ResizeObserver(updateContentBounds).observe(document.body);
      window.addEventListener('resize', updateContentBounds);
      setTimeout(updateContentBounds, 50);
      setTimeout(updateContentBounds, 250);
      setTimeout(updateContentBounds, 500);
    })();
    </script>
    """,
    unsafe_allow_html=True,
)

# Init state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []
if 'sample_size' not in st.session_state:
    st.session_state.sample_size = 5
if 'first_message_sent' not in st.session_state:
    st.session_state.first_message_sent = False
if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []

# Lightweight responder (kept from previous app)
def generate_ai_response(user_input, uploaded_files):
    text = user_input.lower()
    if 'table' in text:
        return "Showing a sample table below:"
    if 'json' in text:
        return "Showing a sample JSON below:"
    if 'graph' in text or 'visualiz' in text:
        return "Showing a sample graph below:"
    if 'code' in text:
        return "Showing a sample ETL code block below:"
    return f"You said: {user_input}"

# Sample prompts (shown once)
if not st.session_state.first_message_sent:
    st.markdown("### Sample Prompts")
    st.markdown("*Click any prompt below to get started:*")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("📊 Create Table", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": "Create a sample data table"})
            st.session_state.messages.append({"role": "assistant", "content": generate_ai_response("table", [])})
            st.session_state.first_message_sent = True
            st.rerun()
    with c2:
        if st.button("💻 Insert Code", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": "Insert ETL pipeline code"})
            st.session_state.messages.append({"role": "assistant", "content": generate_ai_response("code", [])})
            st.session_state.first_message_sent = True
            st.rerun()
    with c3:
        if st.button("📋 Generate JSON", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": "Generate JSON configuration"})
            st.session_state.messages.append({"role": "assistant", "content": generate_ai_response("json", [])})
            st.session_state.first_message_sent = True
            st.rerun()
    with c4:
        if st.button("📈 Show Graph", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": "Show data visualization"})
            st.session_state.messages.append({"role": "assistant", "content": generate_ai_response("graph", [])})
            st.session_state.first_message_sent = True
            st.rerun()
    st.markdown("---")

# Welcome message if empty
if not st.session_state.messages:
    with st.chat_message("assistant", avatar="nexuslogo.png"):
        st.write("Hello! Upload files or ask me anything.")

# Render chat
for i, msg in enumerate(st.session_state.messages):
    avatar = "logo.png" if msg["role"] == "assistant" else None
    with st.chat_message(msg["role"], avatar=avatar):
        st.write(msg["content"])
        if msg["role"] == "assistant" and i > 0:
            prev = st.session_state.messages[i-1]["content"].lower()
            if 'table' in prev:
                df = pd.DataFrame({
                    'Employee_ID': list(range(1001, 1001 + st.session_state.sample_size)),
                    'Salary': [95000 + i*1000 for i in range(st.session_state.sample_size)],
                })
                st.dataframe(df, use_container_width=True)
            elif 'json' in prev:
                st.json({"pipeline": {"steps": st.session_state.sample_size}})
            elif 'graph' in prev:
                df = pd.DataFrame({
                    'Month': [f'M{i+1}' for i in range(st.session_state.sample_size)],
                    'Revenue': [120000 + i*5000 for i in range(st.session_state.sample_size)],
                })
                st.line_chart(df.set_index('Month'))
            elif 'code' in prev:
                st.code("print('ETL pipeline...')", language='python')

# Input row with Settings and Clear controls
col1, col2, col3 = st.columns([18, 1.7, 1])
with col1:
    user_input = st.chat_input("Message AI Assistant...")

with col2:
    with st.popover("⚙️", help="Settings", use_container_width=False):
        st.markdown("### Settings")
        st.markdown("---")

        # File uploads
        st.markdown("#### Upload Files")
        uploaded_files = st.file_uploader(
            "Choose files to analyze",
            accept_multiple_files=True,
            type=["csv", "xlsx", "json", "txt", "py", "sql", "md", "pdf"],
            help="Upload files for AI analysis",
            key="file_uploader",
        )
        if uploaded_files:
            st.session_state.uploaded_files = uploaded_files
            st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
            with st.expander("View uploaded files", expanded=False):
                for f in uploaded_files:
                    st.write(f"• **{f.name}** ({f.size:,} bytes)")

        st.markdown("---")
        # Sample size
        st.markdown("#### Sample Data Size")
        new_sample_size = st.slider(
            "Adjust sample data rows",
            min_value=1,
            max_value=10,
            value=st.session_state.sample_size,
            key="sample_size_slider",
            help="Controls number of rows/items in sample displays",
        )
        if new_sample_size != st.session_state.sample_size:
            st.session_state.sample_size = new_sample_size
            st.caption(f"Sample size set to {new_sample_size} rows")

with col3:
    if st.button("🗑️", help="Clear chat history", key="clear_btn"):
        st.session_state.messages = []
        st.session_state.first_message_sent = False
        st.session_state.uploaded_files = []
        st.rerun()

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    ai_response = generate_ai_response(user_input, st.session_state.uploaded_files)
    st.session_state.messages.append({"role": "assistant", "content": ai_response})
    st.session_state.first_message_sent = True
    st.session_state.conversation_history.append({
        "timestamp": datetime.now(),
        "user_input": user_input,
        "response_length": len(ai_response),
    })
    st.rerun()
