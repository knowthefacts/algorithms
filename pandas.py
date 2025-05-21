import streamlit as st
import pandas as pd
import os
import io # For handling CSV string for S3
import json # For parsing Secrets Manager secret

# --- AWS SDK ---
USE_AWS_SERVICES = os.environ.get("USE_AWS_SERVICES", "false").lower() == "true"
if USE_AWS_SERVICES:
    import boto3
    secrets_manager = boto3.client('secretsmanager')
    s3_client = boto3.client('s3')

# --- Configuration ---
SECRET_NAME = os.environ.get("APP_SECRET_NAME", "myapp/credentials")
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "your-edp-data-bucket")

# Updated CSV file configuration
CSV_FILES_S3_KEYS = {
    "Survey Weights": f"data/survey_weights.csv",
    "BU Allocation Target": f"data/bu_allocation_target.csv",
}

LOCAL_CSV_PATHS = {
    "Survey Weights": "survey_weights.csv",
    "BU Allocation Target": "bu_allocation_target.csv",
}

# --- Authentication ---
def check_credentials(username, password_attempt):
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
        valid_username = os.environ.get("STREAMLIT_USERNAME", "admin")
        valid_password = os.environ.get("STREAMLIT_PASSWORD", "password")
        if not (valid_username and valid_password) and not (st.secrets.get("streamlit_username") and st.secrets.get("streamlit_password")):
            st.warning("Local credentials not set via env vars (STREAMLIT_USERNAME, STREAMLIT_PASSWORD) or st.secrets. Using defaults: admin/password")
            valid_username = "admin"
            valid_password = "password"
        elif st.secrets.get("streamlit_username") and st.secrets.get("streamlit_password"): # Check Streamlit secrets file
            valid_username = st.secrets["streamlit_username"]
            valid_password = st.secrets["streamlit_password"]


    if username == valid_username and password_attempt == valid_password:
        return True
    return False

# --- S3 Data Operations ---
def load_data_from_s3(tab_name):
    if USE_AWS_SERVICES:
        key = CSV_FILES_S3_KEYS[tab_name]
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            st.error(f"Error loading '{key}' from S3 bucket '{S3_BUCKET}': {e}")
            return pd.DataFrame()
    else:
        filepath = LOCAL_CSV_PATHS[tab_name]
        try:
            if not os.path.exists(filepath):
                st.warning(f"Local file {filepath} not found. Creating a dummy one for {tab_name}.")
                if tab_name == "Survey Weights":
                    pd.DataFrame({'Factor':['Age','Income'], 'Weight':[0.3,0.4], 'Category':['Demo','Financial']}).to_csv(filepath, index=False)
                elif tab_name == "BU Allocation Target":
                    pd.DataFrame({'BusinessUnit':['Marketing','Sales'], 'TargetPercentage':[30,40], 'Region':['NA','EMEA']}).to_csv(filepath, index=False)
                else:
                    pd.DataFrame().to_csv(filepath, index=False) # Empty for any other
            return pd.read_csv(filepath)
        except FileNotFoundError:
            st.error(f"Local file '{filepath}' not found and could not create dummy.")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading local file '{filepath}': {e}")
            return pd.DataFrame()

def save_data_to_s3(df, tab_name):
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
    st.session_state.show_login_form = False

for tab_key_name in CSV_FILES_S3_KEYS.keys():
    sanitized_key = tab_key_name.replace(" ", "_").lower()
    if f'df_original_{sanitized_key}' not in st.session_state:
        st.session_state[f'df_original_{sanitized_key}'] = None
    if f'df_edited_{sanitized_key}' not in st.session_state:
        st.session_state[f'df_edited_{sanitized_key}'] = None
    if f'review_mode_{sanitized_key}' not in st.session_state:
        st.session_state[f'review_mode_{sanitized_key}'] = False

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
        st.session_state.show_login_form = True

    if st.session_state.show_login_form:
        with st.form("login_form"):
            st.subheader("Login Required")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            login_button = st.form_submit_button("Login")

            if login_button:
                if check_credentials(username, password):
                    st.session_state.authenticated = True
                    st.session_state.show_login_form = False
                    st.success("Login successful!")
                    st.experimental_rerun()
                else:
                    st.error("Invalid username or password.")
    
    if st.session_state.authenticated:
        if st.sidebar.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.show_login_form = True
            for tab_key_name in CSV_FILES_S3_KEYS.keys():
                sanitized_key = tab_key_name.replace(" ", "_").lower()
                st.session_state[f'df_original_{sanitized_key}'] = None
                st.session_state[f'df_edited_{sanitized_key}'] = None
                st.session_state[f'review_mode_{sanitized_key}'] = False
            st.experimental_rerun()

        tab_names = list(CSV_FILES_S3_KEYS.keys())
        tabs = st.tabs(tab_names)

        for i, tab_widget in enumerate(tabs):
            tab_name = tab_names[i]
            sanitized_tab_key = tab_name.replace(" ", "_").lower()
            df_original_key = f'df_original_{sanitized_tab_key}'
            df_edited_key = f'df_edited_{sanitized_tab_key}'
            review_mode_key = f'review_mode_{sanitized_tab_key}'

            with tab_widget:
                st.subheader(f"Manage: {tab_name}")

                if st.session_state[df_original_key] is None:
                    loaded_df = load_data_from_s3(tab_name)
                    if loaded_df is not None and not loaded_df.empty:
                        st.session_state[df_original_key] = loaded_df.copy(deep=True)
                        st.session_state[df_edited_key] = loaded_df.copy(deep=True)
                    else:
                        st.session_state[df_original_key] = pd.DataFrame()
                        st.session_state[df_edited_key] = pd.DataFrame()
                        st.warning(f"No data loaded for {tab_name}. Displaying empty editor.")


                if st.session_state[df_original_key] is None: # Should be caught by above, but as a safeguard
                    st.error(f"Critical error: Original data for {tab_name} is None. Please reload the page or contact support.")
                    continue

                # --- Review Mode ---
                if st.session_state[review_mode_key]:
                    st.markdown("#### Review Changes")
                    
                    original_df = st.session_state[df_original_key]
                    edited_df = st.session_state[df_edited_key]

                    # For debugging, uncomment below:
                    # st.write("--- DEBUG: Data in Review Mode ---")
                    # st.write("Original DF:")
                    # st.dataframe(original_df)
                    # st.write("Edited DF:")
                    # st.dataframe(edited_df)
                    # if original_df is not None and edited_df is not None:
                    #     st.write(f"Are they identical? {original_df.equals(edited_df)}")
                    # st.write("--- END DEBUG ---")

                    col1_disp, col2_disp = st.columns(2)
                    with col1_disp:
                        st.markdown("**Original Data**")
                        st.dataframe(original_df, use_container_width=True, height=300)
                    with col2_disp:
                        st.markdown("**Modified Data**")
                        st.dataframe(edited_df, use_container_width=True, height=300)
                    
                    st.markdown("---")
                    st.markdown("#### Summary of Structural Changes")

                    original_cols = set(original_df.columns)
                    edited_cols = set(edited_df.columns)

                    added_cols = list(edited_cols - original_cols)
                    if added_cols:
                        st.write(f"**Columns Added:** ` {', '.join(added_cols)} `")
                    
                    deleted_cols = list(original_cols - edited_cols)
                    if deleted_cols:
                        st.write(f"**Columns Deleted:** ` {', '.join(deleted_cols)} `")
                    
                    if len(original_df) != len(edited_df):
                        st.write(f"**Number of Rows Changed:** From {len(original_df)} to {len(edited_df)}")
                    
                    if not added_cols and not deleted_cols and len(original_df) == len(edited_df) and original_df.equals(edited_df):
                        st.info("No structural or value changes detected.")
                    
                    st.markdown("---")
                    st.markdown("#### Cell Value Differences (within common columns & rows by position)")
                    
                    common_cols_list = list(original_cols.intersection(edited_cols))
                    if not common_cols_list:
                        st.info("No common columns to compare for cell values.")
                    elif original_df.empty and edited_df.empty:
                         st.info("Both original and edited data are empty.")
                    else:
                        # Compare based on reset indexes to see positional differences
                        # Make copies to avoid modifying the session state DFs directly with reset_index
                        orig_compare_df = original_df[common_cols_list].copy().reset_index(drop=True)
                        edit_compare_df = edited_df[common_cols_list].copy().reset_index(drop=True)
                        
                        try:
                            # Align number of rows for comparison if they differ after reset_index
                            # This is a basic alignment; pandas compare is sensitive to shape
                            max_rows = max(len(orig_compare_df), len(edit_compare_df))
                            orig_compare_df = orig_compare_df.reindex(range(max_rows))
                            edit_compare_df = edit_compare_df.reindex(range(max_rows))

                            diff_df = orig_compare_df.compare(
                                edit_compare_df,
                                align_axis=1, 
                                keep_equal=False, 
                                keep_shape=False # True might be better to see context
                            )
                            if not diff_df.empty:
                                diff_df.columns = pd.MultiIndex.from_tuples(
                                    [(col[0], "Original" if col[1]=='self' else "New Value") for col in diff_df.columns]
                                )
                                st.dataframe(diff_df, use_container_width=True)
                            else:
                                if not original_df.equals(edited_df): # If structurally different but no cell diffs in common parts
                                     st.info("No differing cell values found in common columns at corresponding row positions. Structural changes are noted above.")
                                elif not added_cols and not deleted_cols and len(original_df) == len(edited_df): # If no structural changes and no value changes
                                    pass # Already covered by "No structural or value changes detected."
                                else: # Default if diff_df is empty but other changes occurred.
                                    st.info("No differing cell values found in common columns at corresponding row positions.")
                        except Exception as e:
                            st.error(f"Error generating cell value diff: {e}")
                            st.caption("This can happen if data structures are too dissimilar for a simple comparison.")


                    save_col, cancel_col = st.columns(2)
                    if save_col.button("Confirm and Save to Production", key=f"save_prod_{sanitized_tab_key}", type="primary"):
                        if save_data_to_s3(st.session_state[df_edited_key], tab_name):
                            st.session_state[df_original_key] = st.session_state[df_edited_key].copy(deep=True)
                            st.session_state[review_mode_key] = False
                            st.success(f"Changes for {tab_name} saved!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Failed to save changes for {tab_name}.")
                    
                    if cancel_col.button("Cancel and Go Back to Editing", key=f"cancel_review_{sanitized_tab_key}"):
                        st.session_state[review_mode_key] = False
                        st.experimental_rerun()

                # --- Edit Mode ---
                else:
                    st.markdown("#### Edit Data")
                    if st.session_state[df_edited_key] is not None:
                        edited_df_from_editor = st.data_editor(
                            st.session_state[df_edited_key], 
                            num_rows="dynamic",
                            key=f"editor_{sanitized_tab_key}",
                            use_container_width=True,
                            height=400 # Optional: set a height for the editor
                        )
                        # Update session state with the returned (potentially modified) dataframe
                        st.session_state[df_edited_key] = edited_df_from_editor 

                        col_b1, col_b2 = st.columns(2)
                        with col_b1:
                            if st.button("Save for Review", key=f"review_changes_{sanitized_tab_key}"):
                                # Ensure df_edited_key in session state has the latest from editor
                                # This should already be true due to the assignment above
                                st.session_state[review_mode_key] = True
                                st.experimental_rerun()
                        with col_b2:
                            if st.button("Reload Data from Source", key=f"reload_data_{sanitized_tab_key}"):
                                loaded_df = load_data_from_s3(tab_name)
                                if loaded_df is not None and not loaded_df.empty:
                                    st.session_state[df_original_key] = loaded_df.copy(deep=True)
                                    st.session_state[df_edited_key] = loaded_df.copy(deep=True)
                                    st.info(f"Data for {tab_name} reloaded.")
                                else:
                                    st.session_state[df_original_key] = pd.DataFrame() # Reset to empty
                                    st.session_state[df_edited_key] = pd.DataFrame()
                                    st.error(f"Failed to reload data for {tab_name}, or data source is empty.")
                                st.experimental_rerun()
                    else:
                        st.error(f"Edited data for {tab_name} is None. This should not happen.")


    elif st.session_state.authenticated is False and st.session_state.show_login_form is False:
        st.info("Please login to access NPS Update features.")
        if st.button("Show Login Form"):
            st.session_state.show_login_form = True
            st.experimental_rerun()
