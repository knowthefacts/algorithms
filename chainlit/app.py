# main.py - Main Streamlit application
import streamlit as st
import subprocess
import threading
import time
import requests
from streamlit.components.v1 import html

# Set page config
st.set_page_config(
    page_title="My App",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for navigation bar
st.markdown("""
<style>
    .navbar {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .nav-button {
        background-color: #4CAF50;
        border: none;
        color: white;
        padding: 10px 20px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
        margin: 4px 8px;
        cursor: pointer;
        border-radius: 5px;
        transition: background-color 0.3s;
    }
    
    .nav-button:hover {
        background-color: #45a049;
    }
    
    .nav-button.active {
        background-color: #2196F3;
    }
    
    .chainlit-container {
        width: 100%;
        height: 600px;
        border: 1px solid #ddd;
        border-radius: 5px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

def check_chainlit_running(port=8000):
    """Check if ChainLit is running on the specified port"""
    try:
        response = requests.get(f"http://localhost:{port}", timeout=2)
        return response.status_code == 200
    except:
        return False

def start_chainlit():
    """Start ChainLit application in a separate process"""
    try:
        # Start ChainLit process
        subprocess.Popen([
            "chainlit", "run", "chainlit_app.py", 
            "--port", "8000", 
            "--headless"
        ])
        return True
    except Exception as e:
        st.error(f"Failed to start ChainLit: {e}")
        return False

def create_navigation():
    """Create navigation bar"""
    nav_html = """
    <div class="navbar">
        <h3>My Application Dashboard</h3>
    </div>
    """
    st.markdown(nav_html, unsafe_allow_html=True)
    
    # Create navigation buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        home_btn = st.button("🏠 Home", key="home", use_container_width=True)
    with col2:
        analytics_btn = st.button("📊 Analytics", key="analytics", use_container_width=True)
    with col3:
        chat_btn = st.button("💬 Chat (ChainLit)", key="chat", use_container_width=True)
    with col4:
        settings_btn = st.button("⚙️ Settings", key="settings", use_container_width=True)
    
    return home_btn, analytics_btn, chat_btn, settings_btn

def main():
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'home'
    if 'chainlit_started' not in st.session_state:
        st.session_state.chainlit_started = False
    
    # Create navigation
    home_btn, analytics_btn, chat_btn, settings_btn = create_navigation()
    
    # Handle navigation
    if home_btn:
        st.session_state.current_page = 'home'
    elif analytics_btn:
        st.session_state.current_page = 'analytics'
    elif chat_btn:
        st.session_state.current_page = 'chat'
    elif settings_btn:
        st.session_state.current_page = 'settings'
    
    # Display content based on current page
    if st.session_state.current_page == 'home':
        show_home_page()
    elif st.session_state.current_page == 'analytics':
        show_analytics_page()
    elif st.session_state.current_page == 'chat':
        show_chat_page()
    elif st.session_state.current_page == 'settings':
        show_settings_page()

def show_home_page():
    st.title("🏠 Welcome to My App")
    st.write("This is the home page of your application.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Quick Stats")
        st.metric("Active Users", "1,234", "12%")
        st.metric("Revenue", "$56,789", "8%")
        st.metric("Conversion Rate", "12.3%", "-2%")
    
    with col2:
        st.subheader("Recent Activity")
        st.info("✅ System backup completed")
        st.info("📊 Weekly report generated")
        st.info("🔄 Database updated")

def show_analytics_page():
    st.title("📊 Analytics Dashboard")
    st.write("Here you can view your application analytics.")
    
    # Sample chart
    import pandas as pd
    import numpy as np
    
    chart_data = pd.DataFrame(
        np.random.randn(20, 3),
        columns=['Series A', 'Series B', 'Series C']
    )
    
    st.line_chart(chart_data)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Traffic Sources")
        source_data = pd.DataFrame({
            'Source': ['Direct', 'Social', 'Email', 'Ads'],
            'Visits': [45, 30, 15, 10]
        })
        st.bar_chart(source_data.set_index('Source'))
    
    with col2:
        st.subheader("User Engagement")
        st.write("Average session duration: 4:32")
        st.write("Bounce rate: 34%")
        st.write("Pages per session: 2.8")

def show_chat_page():
    st.title("💬 Chat Interface (ChainLit)")
    
    # Check if ChainLit is running
    if not st.session_state.chainlit_started:
        st.info("Starting ChainLit application...")
        
        if start_chainlit():
            st.session_state.chainlit_started = True
            time.sleep(3)  # Give ChainLit time to start
            st.rerun()
        else:
            st.error("Failed to start ChainLit application")
            return
    
    if check_chainlit_running():
        st.success("ChainLit is running! 🎉")
        
        # Embed ChainLit using iframe
        chainlit_url = "http://localhost:8000"
        iframe_html = f"""
        <div class="chainlit-container">
            <iframe src="{chainlit_url}" width="100%" height="600px" frameborder="0"></iframe>
        </div>
        """
        html(iframe_html, height=620)
        
    else:
        st.warning("ChainLit is starting up... Please wait.")
        time.sleep(2)
        st.rerun()

def show_settings_page():
    st.title("⚙️ Settings")
    st.write("Configure your application settings here.")
    
    with st.form("settings_form"):
        st.subheader("General Settings")
        app_name = st.text_input("Application Name", value="My App")
        theme = st.selectbox("Theme", ["Light", "Dark", "Auto"])
        
        st.subheader("Notifications")
        email_notifications = st.checkbox("Email Notifications", value=True)
        push_notifications = st.checkbox("Push Notifications", value=False)
        
        st.subheader("ChainLit Settings")
        chainlit_port = st.number_input("ChainLit Port", value=8000, min_value=3000, max_value=9999)
        
        submitted = st.form_submit_button("Save Settings")
        if submitted:
            st.success("Settings saved successfully!")

if __name__ == "__main__":
    main()
