import streamlit as st
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import io
from datetime import datetime
import hashlib

# Configure page
st.set_page_config(
    page_title="Data Management Portal",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# AWS clients
@st.cache_resource
def get_aws_clients():
    return {
        's3': boto3.client('s3'),
        'secrets': boto3.client('secretsmanager')
    }

# Authentication functions
def get_credentials_from_secrets():
    """Retrieve credentials from AWS Secrets Manager"""
    try:
        clients = get_aws_clients()
        response = clients['secrets'].get_secret_value(SecretId='streamlit-app-credentials')
        secrets = json.loads(response['SecretString'])
        return secrets
    except ClientError as e:
        st.error(f"Error retrieving credentials: {e}")
        return None

def verify_credentials(username, password):
    """Verify username and password against stored credentials"""
    credentials = get_credentials_from_secrets()
    if not credentials:
        return False
    
    stored_username = credentials.get('username')
    stored_password_hash = credentials.get('password_hash')
    
    # Hash the provided password
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    return username == stored_username and password_hash == stored_password_hash

def login_form():
    """Display login form"""
    st.title("🔐 Login Required")
    st.write("Please enter your credentials to access the NPS Data Editor")
    
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")
        
        if submit:
            if verify_credentials(username, password):
                st.session_state.authenticated = True
                st.session_state.username = username
                st.rerun()
            else:
                st.error("Invalid credentials. Please try again.")

# Data management functions
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data_from_s3(bucket_name, key):
    """Load CSV data from S3"""
    try:
        clients = get_aws_clients()
        response = clients['s3'].get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        return df
    except ClientError as e:
        st.error(f"Error loading data from S3: {e}")
        return None

def save_data_to_s3(df, bucket_name, key):
    """Save DataFrame to S3 as CSV"""
    try:
        clients = get_aws_clients()
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        clients['s3'].put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        return True
    except ClientError as e:
        st.error(f"Error saving data to S3: {e}")
        return False

# Page functions
def home_page():
    """Home page content"""
    st.title("🏠 Welcome to Data Management Portal")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ## Available Applications
        
        ### 📊 NPS Data Editor
        Access the Net Promoter Score data management system. This application allows you to:
        - View and edit NPS survey data
        - Manage customer feedback records
        - Export updated datasets
        - Maintain data quality and consistency
        
        **Note:** Authentication required for access.
        
        ### 🚧 EDP Application
        **Status:** Under Development
        
        The Employee Development Program (EDP) application is currently being built and will include:
        - Employee performance tracking
        - Development goal management
        - Progress monitoring tools
        - Reporting capabilities
        
        *Expected completion: Q3 2025*
        """)
    
    with col2:
        st.info("💡 **Quick Tips**\n\n• Use the sidebar to navigate\n• NPS app requires login\n• Data changes are saved to S3\n• Contact admin for access issues")
        
        st.success("🔧 **System Status**\n\nAll systems operational")

def edp_page():
    """EDP page - under development"""
    st.title("🚧 EDP - Employee Development Program")
    
    st.warning("⚠️ This application is currently under development")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown("""
        ## Coming Soon!
        
        The Employee Development Program (EDP) platform will provide comprehensive tools for:
        
        **📈 Performance Management**
        - Individual performance tracking
        - Goal setting and monitoring
        - 360-degree feedback collection
        
        **🎯 Development Planning**
        - Skill gap analysis
        - Learning path recommendations
        - Career progression mapping
        
        **📊 Analytics & Reporting**
        - Performance dashboards
        - Development ROI metrics
        - Organizational insights
        
        **🤝 Collaboration Tools**
        - Manager-employee check-ins
        - Peer feedback systems
        - Team development activities
        """)
    
    with col2:
        st.info("📅 **Development Timeline**\n\n• Q2 2025: Core features\n• Q3 2025: Beta testing\n• Q4 2025: Full release")
        
        st.markdown("---")
        
        st.markdown("**💬 Feedback Welcome**\n\nHave suggestions for the EDP platform? Contact the development team!")

def nps_data_editor():
    """NPS Data Editor with authentication"""
    if not st.session_state.get('authenticated', False):
        login_form()
        return
    
    st.title("📊 NPS Data Editor")
    st.write(f"Welcome, {st.session_state.get('username', 'User')}!")
    
    # Logout button
    if st.button("🚪 Logout", type="secondary"):
        st.session_state.authenticated = False
        st.session_state.username = None
        st.rerun()
    
    # Configuration - Update these with your actual S3 details
    BUCKET_NAME = "your-data-bucket"  # Replace with your bucket name
    DATASET_1_KEY = "data/nps_dataset_1.csv"  # Replace with your file path
    DATASET_2_KEY = "data/nps_dataset_2.csv"  # Replace with your file path
    
    tab1, tab2 = st.tabs(["📋 Dataset 1", "📋 Dataset 2"])
    
    with tab1:
        st.subheader("Dataset 1 - NPS Survey Data")
        handle_dataset_editing(BUCKET_NAME, DATASET_1_KEY, "dataset_1")
    
    with tab2:
        st.subheader("Dataset 2 - Customer Feedback")
        handle_dataset_editing(BUCKET_NAME, DATASET_2_KEY, "dataset_2")

def handle_dataset_editing(bucket_name, key, dataset_id):
    """Handle editing for a specific dataset"""
    # Load data
    if f"{dataset_id}_data" not in st.session_state:
        with st.spinner(f"Loading {dataset_id}..."):
            df = load_data_from_s3(bucket_name, key)
            if df is not None:
                st.session_state[f"{dataset_id}_data"] = df
                st.session_state[f"{dataset_id}_original"] = df.copy()
    
    if f"{dataset_id}_data" in st.session_state:
        df = st.session_state[f"{dataset_id}_data"]
        
        # Display data info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            st.metric("Columns", len(df.columns))
        with col3:
            last_modified = datetime.now().strftime("%Y-%m-%d %H:%M")
            st.metric("Last Loaded", last_modified)
        
        # Data editor
        st.subheader("📝 Edit Data")
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            key=f"{dataset_id}_editor"
        )
        
        # Update session state
        st.session_state[f"{dataset_id}_data"] = edited_df
        
        # Show changes
        if not edited_df.equals(st.session_state[f"{dataset_id}_original"]):
            st.info("📝 You have unsaved changes!")
            
            # Save button
            col1, col2 = st.columns([1, 4])
            with col1:
                if st.button(f"💾 Save Changes", key=f"save_{dataset_id}", type="primary"):
                    with st.spinner("Saving to S3..."):
                        if save_data_to_s3(edited_df, bucket_name, key):
                            st.success("✅ Data saved successfully!")
                            st.session_state[f"{dataset_id}_original"] = edited_df.copy()
                            # Clear cache to reload fresh data
                            st.cache_data.clear()
                            st.rerun()
            
            with col2:
                if st.button(f"🔄 Reset Changes", key=f"reset_{dataset_id}"):
                    st.session_state[f"{dataset_id}_data"] = st.session_state[f"{dataset_id}_original"].copy()
                    st.rerun()
        
        # Data preview
        with st.expander("📊 Data Preview & Statistics"):
            st.write("**Data Types:**")
            st.write(edited_df.dtypes)
            
            st.write("**Summary Statistics:**")
            st.write(edited_df.describe())
    else:
        st.error("Failed to load dataset. Please check your S3 configuration.")

# Health check endpoint
def health_check():
    """Health check for ALB target group"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Main application
def main():
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    # Sidebar navigation
    st.sidebar.title("🚀 Navigation")
    
    page = st.sidebar.radio(
        "Select Page:",
        ["🏠 Home", "🚧 EDP", "📊 NPS App"],
        index=0
    )
    
    # Page routing
    if page == "🏠 Home":
        home_page()
    elif page == "🚧 EDP":
        edp_page()
    elif page == "📊 NPS App":
        nps_data_editor()
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("*Data Management Portal v1.0*")

# Health check route (for ALB)
if __name__ == "__main__":
    # Simple health check endpoint
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "health":
        print(json.dumps(health_check()))
    else:
        main()
