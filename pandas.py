import streamlit as st
import pandas as pd
import os
import io # For handling CSV string for S3

# --- AWS SDK ---
# Only import boto3 if you intend to use it (e.g., when deployed)
# For local development, we can mock these interactions.
USE_AWS_SERVICES = os.environ.get("USE_AWS_SERVICES", "false").lower() == "true"
if USE_AWS_SERVICES:
    import boto3
    secrets_manager = boto3.client('secretsmanager')
    s3_client = boto3.client('s3')

# --- Configuration ---
# For AWS Secrets Manager (replace with your actual secret name)
SECRET_NAME = os.environ.get("APP_SECRET_NAME", "myapp/credentials") # e.g., "your-app/ecs/credentials"
# For S3 (replace with your actual bucket and prefixes/keys)
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "your-edp-data-bucket")
CSV_FILES_S3_KEYS = {
    "NPS Raw Data": "data/nps_raw_data.csv",
    "Customer Feedback": "data/customer_feedback.csv",
    "Service Metrics": "data/service_metrics.csv"
}

# For local testing (paths relative to app.py)
LOCAL_CSV_PATHS = {
    "NPS Raw Data": "datapoint1.csv",
    "Customer Feedback": "datapoint2.csv",
    "Service Metrics": "datapoint3.csv"
}

# --- Authentication ---
def check_credentials(username, password_attempt):
    """
    Checks username and password.
    In ECS, fetches credentials from AWS Secrets Manager.
    For local dev, uses environment variables or hardcoded (less secure).
    """
    if USE_AWS_SERVICES:
        try:
            get_secret_value_response = secrets_manager.get_secret_value(SecretId=SECRET_NAME)
            secret = json.loads(get_secret_value_response['SecretString'])
            valid_username = secret.get('USERNAME')
            valid_password = secret.get('PASSWORD')
        except Exception as e:
            st.error(f"Error fetching credentials from Secrets Manager: {e}")
            return False
    else:
        # Local development: Use environment variables or hardcode (for demo only)
        valid_username = os.environ.get("STREAMLIT_USERNAME", "admin")
        valid_password = os.environ.get("STREAMLIT_PASSWORD", "password")
        if not (valid_username and valid_password):
            st.warning("Local credentials (STREAMLIT_USERNAME, STREAMLIT_PASSWORD) not set. Using defaults: admin/password")
            valid_username = "admin"
            valid_password = "password"


    if username == valid_username and password_attempt == valid_password:
        return True
    return False

# --- S3 Data Operations ---
def load_data_from_s3(tab_name):
    """Loads a CSV from S3 or local path into a pandas DataFrame."""
    if USE_AWS_SERVICES:
        key = CSV_FILES_S3_KEYS[tab_name]
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            st.error(f"Error loading '{key}' from S3 bucket '{S3_BUCKET}': {e}")
            return pd.DataFrame() # Return empty dataframe on error
    else:
        # Local development
        filepath = LOCAL_CSV_PATHS[tab_name]
        try:
            if not os.path.exists(filepath):
                st.warning(f"Local file {filepath} not found. Creating a dummy one for {tab_name}.")
                # Create dummy files if they don't exist for local testing
                if tab_name == "NPS Raw Data":
                    pd.DataFrame({'ID':[1,2], 'Name':['A','B'], 'Score':[9,10]}).to_csv(filepath, index=False)
                elif tab_name == "Customer Feedback":
                    pd.DataFrame({'TicketID':[101,102], 'Comment':['Good','Okay'], 'Sentiment':['Positive','Neutral']}).to_csv(filepath, index=False)
                else: # Service Metrics
                    pd.DataFrame({'Month':['Jan','Feb'], 'Uptime': [99.9, 99.8], 'Tickets':[50, 60]}).to_csv(filepath, index=False)

            return pd.read_csv(filepath)
        except Exception as e:
            st.error(f"Error loading local file '{filepath}': {e}")
            return pd.DataFrame()

def save_data_to_s3(df, tab_name):
    """Saves a pandas DataFrame to a CSV in S3 or local path."""
    if USE_AWS_SERVICES:
        key = CSV_FILES_S3_KEYS[tab_name]
        try:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
            st.success(f"Data for '{tab_name}' successfully saved to S3 bucket '{S3_BUCKET}' at '{key}'.")
            return True
        except Exception as e:
            st.error(f"Error saving '{key}' to S3 bucket '{S3_BUCKET}': {e}")
            return False
    else:
        # Local development
        filepath = LOCAL_CSV_PATHS[tab_name]
        try:
            df.to_csv(filepath, index=False)
            st.success(f"Data for '{tab_name}' successfully saved locally to '{filepath}'.")
            return True
        except Exception as e:
            st.error(f"Error saving local file '{filepath}': {e}")
            return False

# --- Initialize session state ---
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'show_login_form' not in st.session_state:
    st.session_state.show_login_form = False # To control visibility of login form

# For storing original and edited dataframes for each tab
for tab_key_name in CSV_FILES_S3_KEYS.keys():
    sanitized_key = tab_key_name.replace(" ", "_") # for valid session state keys
    if f'df_original_{sanitized_key}' not in st.session_state:
        st.session_state[f'df_original_{sanitized_key}'] = None
    if f'df_edited_{sanitized_key}' not in st.session_state:
        st.session_state[f'df_edited_{sanitized_key}'] = None
    if f'review_mode_{sanitized_key}' not in st.session_state:
        st.session_state[f'review_mode_{sanitized_key}'] = False # Are we reviewing changes for this tab?

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
menu_selection = st.sidebar.radio("Go to", ["Home", "NPS Update"])

# --- Page Content ---
if menu_selection == "Home":
    st.title("🏠 Welcome to EDP Dashboard")
    st.write("This dashboard is currently under development.")
    st.info("Select 'NPS Update' from the sidebar to manage data points (requires login).")

elif menu_selection == "NPS Update":
    st.title("🔄 NPS Update")

    if not st.session_state.authenticated:
        st.session_state.show_login_form = True # Ensure form is shown if not authenticated

    if st.session_state.show_login_form:
        with st.form("login_form"):
            st.subheader("Login Required")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            login_button = st.form_submit_button("Login")

            if login_button:
                if check_credentials(username, password):
                    st.session_state.authenticated = True
                    st.session_state.show_login_form = False # Hide form on success
                    st.success("Login successful!")
                    st.rerun() # Rerun to reflect authenticated state
                else:
                    st.error("Invalid username or password.")
    
    if st.session_state.authenticated:
        if st.sidebar.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.show_login_form = True # Show login form again
            # Clear sensitive data from session state on logout
            for tab_key_name in CSV_FILES_S3_KEYS.keys():
                sanitized_key = tab_key_name.replace(" ", "_")
                st.session_state[f'df_original_{sanitized_key}'] = None
                st.session_state[f'df_edited_{sanitized_key}'] = None
                st.session_state[f'review_mode_{sanitized_key}'] = False
            st.rerun()

        # Create tabs for each data point
        tab_names = list(CSV_FILES_S3_KEYS.keys())
        tabs = st.tabs(tab_names)

        for i, tab_widget in enumerate(tabs):
            tab_name = tab_names[i]
            sanitized_tab_key = tab_name.replace(" ", "_")
            df_original_key = f'df_original_{sanitized_tab_key}'
            df_edited_key = f'df_edited_{sanitized_tab_key}'
            review_mode_key = f'review_mode_{sanitized_tab_key}'

            with tab_widget:
                st.subheader(f"Manage: {tab_name}")

                # Load data if not already loaded or if original is None (e.g., after logout/login)
                if st.session_state[df_original_key] is None:
                    st.session_state[df_original_key] = load_data_from_s3(tab_name)
                    # Initialize edited DF as a copy of original
                    if st.session_state[df_original_key] is not None:
                         st.session_state[df_edited_key] = st.session_state[df_original_key].copy()
                    else: # If loading failed
                        st.session_state[df_edited_key] = pd.DataFrame()


                if st.session_state[df_original_key] is None or st.session_state[df_original_key].empty:
                    st.warning(f"Could not load data for {tab_name}. Check S3/local configuration or file existence.")
                    continue # Skip to next tab if data loading failed

                # --- Review Mode ---
                if st.session_state[review_mode_key]:
                    st.markdown("#### Review Changes")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Original Data**")
                        st.dataframe(st.session_state[df_original_key], use_container_width=True)
                    with col2:
                        st.markdown("**Modified Data**")
                        st.dataframe(st.session_state[df_edited_key], use_container_width=True)
                    
                    st.markdown("**Differences**")
                    try:
                        # Pandas compare needs identical indexes for proper comparison
                        original_df_reset = st.session_state[df_original_key].reset_index(drop=True)
                        edited_df_reset = st.session_state[df_edited_key].reset_index(drop=True)
                        
                        # Align columns before compare, in case columns were added/dropped
                        common_cols = original_df_reset.columns.intersection(edited_df_reset.columns)
                        
                        diff_df = original_df_reset[common_cols].compare(
                            edited_df_reset[common_cols],
                            align_axis=1, # compare row by row
                            keep_equal=False, # show only differences
                            keep_shape=False # don't keep original shape if rows are identical
                        )
                        if not diff_df.empty:
                             # The output of compare has multi-index columns ('self', 'other')
                            diff_df.columns = pd.MultiIndex.from_tuples([(col[0], f"{col[1]}_orig" if col[1]=='self' else f"{col[1]}_new") for col in diff_df.columns])
                            st.dataframe(diff_df, use_container_width=True)
                        else:
                            st.info("No cell-value changes detected in common columns and rows.")

                        # Check for added/dropped rows (simple check based on index length)
                        if len(original_df_reset) > len(edited_df_reset):
                            st.write(f"Rows dropped: {len(original_df_reset) - len(edited_df_reset)}")
                        elif len(edited_df_reset) > len(original_df_reset):
                            st.write(f"Rows added: {len(edited_df_reset) - len(original_df_reset)}")
                        
                        # Check for added/dropped columns
                        added_cols = edited_df_reset.columns.difference(original_df_reset.columns)
                        dropped_cols = original_df_reset.columns.difference(edited_df_reset.columns)
                        if len(added_cols) > 0:
                            st.write(f"Columns added: {list(added_cols)}")
                        if len(dropped_cols) > 0:
                            st.write(f"Columns dropped: {list(dropped_cols)}")


                    except Exception as e:
                        st.error(f"Error generating diff: {e}")
                        st.write("Original and modified data might have shapes that are too different for a simple comparison.")


                    save_col, cancel_col = st.columns(2)
                    if save_col.button("Confirm and Save to Production", key=f"save_prod_{sanitized_tab_key}", type="primary"):
                        if save_data_to_s3(st.session_state[df_edited_key], tab_name):
                            # Update original to reflect the saved state
                            st.session_state[df_original_key] = st.session_state[df_edited_key].copy()
                            st.session_state[review_mode_key] = False
                            st.success(f"Changes for {tab_name} saved!")
                            st.rerun() # Rerun to go back to edit mode and reflect changes
                        else:
                            st.error(f"Failed to save changes for {tab_name}.")
                    
                    if cancel_col.button("Cancel and Go Back to Editing", key=f"cancel_review_{sanitized_tab_key}"):
                        st.session_state[review_mode_key] = False
                        st.rerun()

                # --- Edit Mode ---
                else:
                    st.markdown("#### Edit Data")
                    if st.session_state[df_edited_key] is not None:
                        # The data editor modifies the DataFrame in place if it's already a session state object
                        # So we pass a copy to data_editor for editing, then update session_state
                        # Or, directly bind to st.session_state for convenience if careful
                        edited_df_from_editor = st.data_editor(
                            st.session_state[df_edited_key], 
                            num_rows="dynamic", # allow adding/deleting rows
                            key=f"editor_{sanitized_tab_key}",
                            use_container_width=True
                        )
                        # Important: Update the session state with the result from data_editor
                        st.session_state[df_edited_key] = edited_df_from_editor 

                        if st.button("Save for Review", key=f"review_changes_{sanitized_tab_key}"):
                            st.session_state[review_mode_key] = True
                            st.rerun() # Rerun to enter review mode
                        
                        if st.button("Reload Data from Source", key=f"reload_data_{sanitized_tab_key}"):
                            st.session_state[df_original_key] = load_data_from_s3(tab_name)
                            if st.session_state[df_original_key] is not None:
                                st.session_state[df_edited_key] = st.session_state[df_original_key].copy()
                                st.info(f"Data for {tab_name} reloaded.")
                            else:
                                st.error(f"Failed to reload data for {tab_name}.")
                            st.rerun()

    elif st.session_state.authenticated is False and st.session_state.show_login_form is False:
        # This case should ideally not be hit if logic is correct, but as a fallback
        st.info("Please login to access NPS Update features.")
        if st.button("Show Login Form"):
            st.session_state.show_login_form = True
            st.rerun()
