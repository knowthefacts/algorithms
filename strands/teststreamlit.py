import streamlit as st
import pandas as pd
from datetime import datetime
import io

# Page configuration
st.set_page_config(
    page_title="Nexus",
    page_icon="nexuslogo.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar width: set min and preferred width before navigation runs
st.markdown(
    """
    <style>
    :root { --sidebar-width: 180px; }
    /* Force the grid to allocate the desired sidebar width */
    [data-testid="stAppViewContainer"] {
      grid-template-columns: var(--sidebar-width) auto !important;
    }
    /* Apply width to both possible sidebar tags (aside/section) */
    aside[data-testid="stSidebar"],
    section[data-testid="stSidebar"] {
      width: var(--sidebar-width) !important;
      min-width: var(--sidebar-width) !important;
      max-width: var(--sidebar-width) !important;
    }
    /* Ensure the inner content respects the width */
    div[data-testid="stSidebarContent"] {
      width: var(--sidebar-width) !important;
      max-width: var(--sidebar-width) !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Prevent sidebar from being closed/collapsed via UI button
st.markdown(
    """
    <style>
    /* Hide the collapse (hamburger/chevron) control */
    [data-testid="stSidebarCollapseButton"],
    button[data-testid="baseButton-headerNoPadding"],
    div[data-testid="collapsedControl"] { display: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)
# Prudential logo above navigation (sidebar header)
try:
    st.logo("prulogo.png")
except Exception:
    pass
st.markdown("""
        <style>
            [alt=Logo] {
                height: 100%; /* Adjust as needed */
                width: 100% !important;
            }
        </style>
    """, unsafe_allow_html=True)
# Nexus logo below Prudential logo (still above navigation)


# Native multipage navigation (simple, no custom buttons)
pages = [
    st.Page(
        "home.py",
        title="Home",
        icon=":material/home:",
    ),
    st.Page(
        "chat.py",
        title="Chat",
        icon=":material/chat:",
    ),
]

## (logo is already placed via st.logo in the sidebar header above navigation)


# Sidebar footer pinned flush to the bottom using flex/sticky layout
st.markdown(
    """
    <style>
    /* Sidebar content as flex column so footer can push to bottom */
    aside[data-testid="stSidebar"] div[data-testid="stSidebarContent"] {
      display: flex; flex-direction: column; min-height: 100vh;
    }
    /* Footer sticks to bottom of sidebar viewport */
    aside[data-testid="stSidebar"] .custom-sidebar-footer {
      margin-top: auto; position: sticky; bottom: 0; padding: 12px 12px 16px;
      text-align: center; opacity: 0.85; font-size: 0.8rem; color: #b3b3b3;
      border-top: 1px solid rgba(255,255,255,0.08);
      backdrop-filter: saturate(120%);
    }
    </style>
    """,
    unsafe_allow_html=True,
)
page = st.navigation(pages)
with st.sidebar:
    st.markdown(
        '<div class="custom-sidebar-footer">&copy; 2025 Data Engineering Team. All rights reserved</div>',
        unsafe_allow_html=True,
    )
page.run()
st.stop()

# Custom CSS for beautiful gradients and styling
def inject_stylesheet(path: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            css = f.read()
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    except Exception:
        # Fallback to inline minimal styles if file missing
        st.markdown(
            """
            <style>
    :root { --chatbar-max-width: 900px; }
    /* Minimal fallback: fixed chat bar only (colors from Streamlit theme) */
    div[data-testid=\"stHorizontalBlock\"]:has([data-testid=\"stChatInput\"]) { position: fixed !important; left: 50%; transform: translateX(-50%); width: min(var(--chatbar-max-width), 100vw); bottom: 0; z-index: 1001; border-top: 1px solid rgba(0,0,0,0.06); padding: 0.6rem 1rem; margin: 0; box-sizing: border-box; display: flex; align-items: center; column-gap: 14px; background: transparent; }
    div[data-testid=\"stHorizontalBlock\"]:has([data-testid=\"stChatInput\"]) [data-testid=\"column\"]:first-child { flex: 1 1 auto !important; min-width: 0 !important; max-width: 100% !important; }
    div[data-testid=\"stHorizontalBlock\"]:has([data-testid=\"stChatInput\"]) [data-testid=\"column\"]:nth-child(2), div[data-testid=\"stHorizontalBlock\"]:has([data-testid=\"stChatInput\"]) [data-testid=\"column\"]:nth-child(3) { flex: 0 0 auto !important; min-width: 48px !important; }
    div[data-testid=\"stHorizontalBlock\"]:has([data-testid=\"stChatInput\"]) [data-testid=\"column\"]:nth-child(3) { padding-left: 8px !important; }
    .main { padding-bottom: 160px; }
    </style>
            """,
            unsafe_allow_html=True,
        )

# Load custom typography (Poppins) first, then minimal chatbar styles
inject_stylesheet("styles/typography.css")
inject_stylesheet("styles/chatbar.css")

# Compute dynamic content area bounds so the fixed bottom bar respects sidebar width
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

# Sidebar nav styling: fixed min width, flat list, no borders and no hover/active background
st.markdown(
    """
    <style>
    /* Sidebar minimum width */
    aside[data-testid=\"stSidebar\"] { min-width: 250px !important; }
    /* Make nav buttons flat list items */
    aside[data-testid=\"stSidebar\"] div.stButton > button,
    aside[data-testid=\"stSidebar\"] div.stButton > button:hover,
    aside[data-testid=\"stSidebar\"] div.stButton > button:active,
    aside[data-testid=\"stSidebar\"] div.stButton > button:focus,
    aside[data-testid=\"stSidebar\"] div.stButton > button:focus-visible {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        outline: none !important;
        justify-content: flex-start !important;
        gap: 10px !important;
        padding: 8px 12px !important;
        border-radius: 0 !important;
    }
    aside[data-testid=\"stSidebar\"] div.stButton > button svg { margin-right: 6px !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Initialize session state for chat
if 'messages' not in st.session_state:
    st.session_state.messages = []

if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []

# Removed chat_mode state; single-mode assistant

if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []

if 'sample_size' not in st.session_state:
    st.session_state.sample_size = 5

if 'first_message_sent' not in st.session_state:
    st.session_state.first_message_sent = False

# Ensure current page exists before rendering sidebar
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'home'

def generate_ai_response(user_input, uploaded_files):
    """
    Generate AI response based on user input and uploaded files (single-mode assistant)
    """
    user_input_lower = user_input.lower()

    if 'create table' in user_input_lower or 'table' in user_input_lower:
        response = """**Creating Employee Data Table:**

I'll help you create a data table. Here's a sample implementation:

**📊 Table Structure:**
• **Table Name:** employees
• **Columns:** 5 (ID, Name, Department, Salary, Experience)
• **Primary Key:** Employee_ID
• **Indexes:** Department, Salary

The table has been created with sample data below:"""

    elif 'generate json' in user_input_lower or 'json' in user_input_lower:
        response = """**Generating JSON Configuration:**

I've created a JSON configuration for your data pipeline:

**📝 JSON Features:**
• **Pipeline Configuration** with scheduling
• **Source Connections** for multiple databases
• **Transformation Rules** for data processing
• **Output Destinations** with error handling

The configuration is displayed below:"""

    elif 'show graph' in user_input_lower or 'visualize' in user_input_lower or 'graph' in user_input_lower:
        response = """**Creating Data Visualization:**

I'll generate an interactive graph for your data:

**📈 Visualization Features:**
• **Interactive controls** for data exploration
• **Multiple metrics** on single chart
• **Responsive design** for all screen sizes
• **Export options** for sharing

The graph is rendered below:"""

    elif 'insert code' in user_input_lower or 'code' in user_input_lower:
        response = """**Generating ETL Pipeline Code:**

Here's a complete ETL pipeline implementation:

**💻 Code Features:**
• **Modular design** with reusable functions
• **Error handling** and retry logic
• **Logging** for monitoring
• **Configuration-driven** approach

The code is displayed below:"""

    else:
        response = "**AI Assistant Response** 🤖\n\n"
        response += f"I understand you're asking about: *'{user_input}'*\n\n"

        if uploaded_files:
            file_names = [f.name for f in uploaded_files]
            response += f"📁 **Analyzing Files:** {', '.join(file_names)}\n\n"

        response += "I'm here to help with:\n"
        response += "• Data pipeline architecture and design\n"
        response += "• SQL optimization and query tuning\n"
        response += "• ETL/ELT best practices\n"
        response += "• Data quality and validation\n"
        response += "• Performance optimization\n\n"
        response += "*Try the sample prompts above for specific examples!*"

    return response

# Sidebar navigation
with st.sidebar:
    # Theme controlled by Streamlit (Settings → Theme). No custom toggle.
    
    # Simple vertical navigation (Spotify-style)
    active_page = st.session_state.current_page
    home_clicked = st.button(
        "Home",
        key="nav_home",
        icon=":material/home:",
        use_container_width=True,
    )
    chat_clicked = st.button(
        "Chat",
        key="nav_chat",
        icon=":material/chat:",
        use_container_width=True,
    )
    if home_clicked:
        st.session_state.current_page = "home"
        st.rerun()
    if chat_clicked:
        st.session_state.current_page = "chat"
        st.rerun()
    
    # Sticky footer copyright at the very bottom
    st.markdown(
        """
        <div style="position: fixed; bottom: 12px; left: 16px; right: 16px; text-align: center; opacity: 0.8; font-size: 0.8rem;">
            &copy; 2025 Data Engineering Team. All rights reserved
        </div>
        """,
        unsafe_allow_html=True,
    )

page = st.session_state.current_page

# Home Page
if page == 'home':
    # Header
    st.markdown("""
    <div class="custom-header">
        <h1>Data Engineering Team Hub</h1>
        <p>Empowering data-driven decisions through robust engineering solutions</p>
    </div>
    """, unsafe_allow_html=True)
    
  

# AI Chat Page
elif page == 'chat':
    # Display sample prompts if no messages yet
    if not st.session_state.first_message_sent:
        st.markdown("### Sample Prompts")
        st.markdown("*Click any prompt below to get started:*")
        
        # Create columns for sample prompt cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📊 Create Table", key="prompt_table", help="Generate a sample data table", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": "Create a sample data table"})
                ai_response = generate_ai_response("create table", st.session_state.uploaded_files)
                st.session_state.messages.append({"role": "assistant", "content": ai_response})
                st.session_state.first_message_sent = True
                st.rerun()
        
        with col2:
            if st.button("💻 Insert Code", key="prompt_code", help="Generate ETL pipeline code", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": "Insert ETL pipeline code"})
                ai_response = generate_ai_response("insert code", st.session_state.uploaded_files)
                st.session_state.messages.append({"role": "assistant", "content": ai_response})
                st.session_state.first_message_sent = True
                st.rerun()
        
        with col3:
            if st.button("📋 Generate JSON", key="prompt_json", help="Create JSON configuration", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": "Generate JSON configuration"})
                ai_response = generate_ai_response("generate json", st.session_state.uploaded_files)
                st.session_state.messages.append({"role": "assistant", "content": ai_response})
                st.session_state.first_message_sent = True
                st.rerun()
        
        with col4:
            if st.button("📈 Show Graph", key="prompt_graph", help="Create data visualization", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": "Show data visualization"})
                ai_response = generate_ai_response("show graph", st.session_state.uploaded_files)
                st.session_state.messages.append({"role": "assistant", "content": ai_response})
                st.session_state.first_message_sent = True
                st.rerun()
        
        # Removed "More Prompts" section as requested
        st.markdown("---")
    
    # Welcome message if no chat history
    if not st.session_state.messages:
        with st.chat_message("assistant"):
            st.write("""
            Hello! I'm your AI assistant for data engineering tasks. I can help you with:
            • Data pipeline design and optimization
            • Code review and best practices  
            • File analysis and data insights
            • SQL query optimization
            • Architecture recommendations
            
            Upload files or ask me anything!
            """)
    
    # Display chat messages
    for i, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.write(message["content"])
            
            # Display additional content for assistant messages
            if message["role"] == "assistant":
                # Check what type of content this message should display
                user_message_content = ""
                if i > 0:  # Get the previous user message
                    user_message_content = st.session_state.messages[i-1]["content"].lower()
                
                # Display table if user asked for table
                if 'table' in user_message_content:
                    sample_data = {
                        'Employee_ID': list(range(1001, 1001 + st.session_state.sample_size)),
                        'Name': ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eva Brown', 
                                'Frank Miller', 'Grace Lee', 'Henry Chen', 'Iris Wang', 'Jack Turner'][:st.session_state.sample_size],
                        'Department': ['Data Engineering', 'Analytics', 'Data Science', 'Engineering', 'Data Engineering',
                                      'Analytics', 'Data Science', 'Engineering', 'Data Engineering', 'Analytics'][:st.session_state.sample_size],
                        'Salary': [95000, 87000, 102000, 78000, 91000, 89000, 98000, 82000, 93000, 86000][:st.session_state.sample_size],
                        'Experience_Years': list(range(2, 2 + st.session_state.sample_size))
                    }
                    df = pd.DataFrame(sample_data)
                    st.dataframe(df, use_container_width=True)
                
                # Display JSON if user asked for JSON
                elif 'json' in user_message_content:
                    json_config = {
                        "pipeline_config": {
                            "name": "sales_data_pipeline",
                            "version": "1.2.0",
                            "schedule": "0 2 * * *",
                            "timeout_minutes": 120,
                            "max_records": st.session_state.sample_size * 1000
                        },
                        "data_sources": [
                            {
                                "name": f"source_{i}",
                                "type": ["postgresql", "mysql", "mongodb", "s3", "api"][i % 5],
                                "active": True
                            } for i in range(st.session_state.sample_size)
                        ],
                        "transformations": {
                            "steps": st.session_state.sample_size,
                            "parallel_processing": True,
                            "error_handling": "retry_with_backoff"
                        }
                    }
                    st.json(json_config)
                
                # Display code if user asked for code
                elif 'code' in user_message_content:
                    sample_code = f"""import pandas as pd
from sqlalchemy import create_engine
import logging

# Configuration
BATCH_SIZE = {st.session_state.sample_size * 1000}
MAX_RETRIES = 3

def extract_data(connection_string, query):
    \"\"\"Extract data from database\"\"\"
    engine = create_engine(connection_string)
    try:
        df = pd.read_sql(query, engine)
        logging.info(f"Extracted {{len(df)}} records")
        return df
    except Exception as e:
        logging.error(f"Data extraction failed: {{e}}")
        return None

def transform_data(df):
    \"\"\"Clean and transform data\"\"\"
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Fill missing values
    df = df.fillna(method='forward')
    
    # Add calculated columns
    df['total_revenue'] = df['quantity'] * df['price']
    df['profit_margin'] = (df['revenue'] - df['cost']) / df['revenue']
    
    return df

def load_data(df, target_table, connection_string):
    \"\"\"Load data to target destination\"\"\"
    engine = create_engine(connection_string)
    try:
        df.to_sql(target_table, engine, if_exists='replace', index=False)
        logging.info(f"Loaded {{len(df)}} records to {{target_table}}")
        return True
    except Exception as e:
        logging.error(f"Data loading failed: {{e}}")
        return False

# Main ETL pipeline
if __name__ == "__main__":
    # Extract
    data = extract_data(SOURCE_CONN, "SELECT * FROM transactions")
    
    # Transform
    if data is not None:
        data = transform_data(data)
    
    # Load
    if data is not None:
        success = load_data(data, "processed_transactions", TARGET_CONN)"""
                    st.code(sample_code, language='python')
                
                # Display graph if user asked for graph
                elif 'graph' in user_message_content or 'visualiz' in user_message_content:
                    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][:st.session_state.sample_size]
                    graph_data = {
                        'Month': months,
                        'Revenue': [120000 + i*5000 for i in range(len(months))],
                        'Orders': [450 + i*30 for i in range(len(months))],
                        'Customers': [380 + i*25 for i in range(len(months))]
                    }
                    df = pd.DataFrame(graph_data)
                    
                    # Create tabs for different chart types
                    tab1, tab2, tab3 = st.tabs(["📈 Line Chart", "📊 Bar Chart", "🥧 Metrics"])
                    
                    with tab1:
                        st.line_chart(df.set_index('Month')[['Revenue']], use_container_width=True)
                    
                    with tab2:
                        st.bar_chart(df.set_index('Month')[['Orders', 'Customers']], use_container_width=True)
                    
                    with tab3:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Revenue", f"${sum(graph_data['Revenue']):,}")
                        with col2:
                            st.metric("Total Orders", f"{sum(graph_data['Orders']):,}")
                        with col3:
                            st.metric("Total Customers", f"{sum(graph_data['Customers']):,}")
    
    # Chat input and controls (fixed at the bottom)

    # Give buttons slightly more fixed room to avoid overlap
    col1, col2, col3 = st.columns([18, 1.5, 1])
    with col1:
        user_input = st.chat_input("Message AI Assistant...")

    with col2:
        # Settings popover using st.popover
        with st.popover("⚙️", help="Settings", use_container_width=False):
            st.markdown("### ⚙️ Settings")
            st.markdown("---")

            # File Upload Section
            st.markdown("#### 📎 Upload Files")
            uploaded_files = st.file_uploader(
                "Choose files to analyze",
                accept_multiple_files=True,
                type=['csv', 'xlsx', 'json', 'txt', 'py', 'sql', 'md', 'pdf'],
                help="Upload files for AI analysis",
                key="file_uploader"
            )

            if uploaded_files:
                st.session_state.uploaded_files = uploaded_files
                st.success(f"✅ {len(uploaded_files)} file(s) uploaded")

                # Show uploaded files
                with st.expander("View uploaded files", expanded=False):
                    for uploaded_file in uploaded_files:
                        st.write(f"• **{uploaded_file.name}** ({uploaded_file.size:,} bytes)")

            st.markdown("---")
            # Sample Data Size Control
            st.markdown("#### 📊 Sample Data Size")
            new_sample_size = st.slider(
                "Adjust sample data rows", 
                min_value=1, 
                max_value=10, 
                value=st.session_state.sample_size,
                help="Controls the number of rows/items in sample data displays",
                key="sample_size_slider"
            )

            if new_sample_size != st.session_state.sample_size:
                st.session_state.sample_size = new_sample_size
                st.caption(f"Sample size set to {new_sample_size} rows")
            st.markdown("---")

    with col3:
        # Clear button
        if st.button("🗑️", help="Clear chat history", key="clear_btn"):
            st.session_state.messages = []
            st.session_state.first_message_sent = False
            st.session_state.uploaded_files = []
            st.rerun()
    
    # Entire row is sticky via CSS targeting the block that contains the chat input
    
    # Handle chat input
    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        ai_response = generate_ai_response(user_input, st.session_state.uploaded_files)
        st.session_state.messages.append({"role": "assistant", "content": ai_response})
        st.session_state.first_message_sent = True
        
        # Store conversation history
        st.session_state.conversation_history.append({
            "timestamp": datetime.now(),
            "user_input": user_input,
            "response_length": len(ai_response)
        })
        st.rerun()