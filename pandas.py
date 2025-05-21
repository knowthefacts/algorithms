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
        elif st.secrets.get("streamlit_username") and st.secrets.get("streamlit_password"):
            valid_username = st.secrets["streamlit_username"]
            valid_password = st.secrets["streamlit_password"]

    return username == valid_username and password_attempt == valid_password

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
                    pd.DataFrame().to_csv(filepath, index=False)
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
                # print(f"--- Tab: {tab_name} ---") # Console log

                if st.session_state.get(df_original_key) is None: # Use .get for safety initial check
                    # print(f"Tab {tab_name}: Initializing DFs. df_original is None.") # Console log
                    loaded_df = load_data_from_s3(tab_name)
                    if loaded_df is not None and not loaded_df.empty:
                        st.session_state[df_original_key] = loaded_df.copy(deep=True)
                        st.session_state[df_edited_key] = loaded_df.copy(deep=True)
                    else: # loaded_df is None or empty
                        st.session_state[df_original_key] = pd.DataFrame()
                        st.session_state[df_edited_key] = pd.DataFrame()
                        st.warning(f"No data loaded for {tab_name} or data source is empty. Displaying empty editor.")
                
                # Ensure df_edited_key is always a DataFrame instance for the editor
                if not isinstance(st.session_state.get(df_edited_key), pd.DataFrame):
                    # print(f"Tab {tab_name}: df_edited_key was not a DataFrame. Resetting to empty DataFrame.") # Console log
                    st.session_state[df_edited_key] = pd.DataFrame()


                # --- Review Mode ---
                if st.session_state[review_mode_key]:
                    st.markdown("#### Review Changes")
                    
                    original_df = st.session_state[df_original_key]
                    edited_df = st.session_state[df_edited_key]

                    if not isinstance(original_df, pd.DataFrame) or not isinstance(edited_df, pd.DataFrame):
                        st.error("Error: Data for review is not in the expected format. Please try reloading.")
                        if st.button("Cancel Review and Edit"):
                            st.session_state[review_mode_key] = False
                            st.experimental_rerun()
                        continue


                    st.markdown("##### Full Data Snapshot")
                    col1_disp, col2_disp = st.columns(2)
                    with col1_disp:
                        st.markdown("**Original Data**")
                        st.dataframe(original_df, use_container_width=True, height=200)
                    with col2_disp:
                        st.markdown("**Modified Data**")
                        st.dataframe(edited_df, use_container_width=True, height=200)
                    st.markdown("---")

                    # --- Column Changes ---
                    st.markdown("##### Column Changes")
                    original_cols = set(original_df.columns)
                    edited_cols = set(edited_df.columns)

                    added_cols = list(edited_cols - original_cols)
                    if added_cols: st.write(f"**Columns Added:** ` {', '.join(added_cols)} `")
                    else: st.caption("No columns added.")

                    deleted_cols = list(original_cols - edited_cols)
                    if deleted_cols: st.write(f"**Columns Deleted:** ` {', '.join(deleted_cols)} `")
                    else: st.caption("No columns deleted.")
                    st.markdown("---")

                    # --- Row Changes ---
                    st.markdown("##### Row Changes")
                    orig_proc = original_df.reset_index(drop=True)
                    edit_proc = edited_df.reset_index(drop=True)

                    max_idx = max(len(orig_proc), len(edit_proc))
                    modified_rows_details = []
                    added_rows_df_list = []
                    deleted_rows_df_list = []
                    
                    has_any_row_change = False

                    for i in range(max_idx):
                        row_in_orig = i < len(orig_proc)
                        row_in_edit = i < len(edit_proc)

                        if row_in_orig and row_in_edit:
                            orig_row_series = orig_proc.iloc[i]
                            edit_row_series = edit_proc.iloc[i]
                            
                            # Align columns for accurate comparison of the row
                            all_cols_for_row = sorted(list(set(orig_row_series.index) | set(edit_row_series.index)))
                            orig_row_aligned = orig_row_series.reindex(all_cols_for_row)
                            edit_row_aligned = edit_row_series.reindex(all_cols_for_row)

                            if not orig_row_aligned.equals(edit_row_aligned):
                                modified_rows_details.append({
                                    "index": i,
                                    "original": orig_row_series.to_dict(),
                                    "modified": edit_row_series.to_dict()
                                })
                                has_any_row_change = True
                        elif not row_in_orig and row_in_edit:
                            added_rows_df_list.append(edit_proc.iloc[i])
                            has_any_row_change = True
                        elif row_in_orig and not row_in_edit:
                            deleted_rows_df_list.append(orig_proc.iloc[i])
                            has_any_row_change = True
                    
                    if added_rows_df_list:
                        st.subheader("Added Rows")
                        st.dataframe(pd.DataFrame(added_rows_df_list), use_container_width=True)
                    else:
                        st.caption("No rows purely added.")

                    if deleted_rows_df_list:
                        st.subheader("Deleted Rows")
                        st.dataframe(pd.DataFrame(deleted_rows_df_list), use_container_width=True)
                    else:
                        st.caption("No rows purely deleted.")

                    if modified_rows_details:
                        st.subheader("Modified Rows (by original index)")
                        for row_info in modified_rows_details:
                            st.markdown(f"**Row Index {row_info['index']}:**")
                            col_m1, col_m2 = st.columns(2)
                            with col_m1:
                                st.caption("Original Content:")
                                st.dataframe(pd.Series(row_info['original']).to_frame().T.reset_index(drop=True), use_container_width=True)
                            with col_m2:
                                st.caption("Modified Content:")
                                st.dataframe(pd.Series(row_info['modified']).to_frame().T.reset_index(drop=True), use_container_width=True)
                            st.markdown("---")
                    else:
                        st.caption("No rows modified in place (values changed at the same index).")

                    if not added_cols and not deleted_cols and not has_any_row_change:
                        if original_df.equals(edited_df):
                             st.info("No changes detected between original and modified data.")
                        else:
                            # This might happen due to column reordering or other subtle differences
                            st.warning("DataFrames are not identical, but no specific add/delete/modify actions were itemized by the current logic. Please review the full snapshots above.")


                    save_col, cancel_col = st.columns(2)
                    if save_col.button("Confirm and Save to Production", key=f"save_prod_{sanitized_tab_key}", type="primary"):
                        if save_data_to_s3(edited_df, tab_name): # Save the state of edited_df
                            st.session_state[df_original_key] = edited_df.copy(deep=True) # Update original to new saved state
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
                    
                    # -------- START DEBUG PRINTS (check console) --------
                    # print(f"Tab {tab_name}: Entering Edit Mode. Current df_edited_key type: {type(st.session_state[df_edited_key])}")
                    # if isinstance(st.session_state[df_edited_key], pd.DataFrame):
                    #     print(f"Tab {tab_name}: df_edited_key shape: {st.session_state[df_edited_key].shape}, Columns: {st.session_state[df_edited_key].columns.tolist()}")
                    #     if st.session_state[df_edited_key].empty:
                    #         print(f"Tab {tab_name}: df_edited_key is an EMPTY DataFrame.")
                    # else:
                    #     print(f"Tab {tab_name}: df_edited_key is NOT a DataFrame instance prior to editor call!")
                    # -------- END DEBUG PRINTS --------

                    # Ensure we always pass a DataFrame to st.data_editor
                    current_df_to_edit = st.session_state[df_edited_key]
                    if not isinstance(current_df_to_edit, pd.DataFrame):
                        st.error("Critical error: Data for editing is not a DataFrame. Resetting. Please try again.")
                        current_df_to_edit = pd.DataFrame() # Fallback to empty
                        st.session_state[df_edited_key] = current_df_to_edit # Correct session state too
                    
                    edited_df_from_editor = st.data_editor(
                        current_df_to_edit, 
                        num_rows="dynamic",
                        key=f"editor_{sanitized_tab_key}", # Crucial for widget state
                        use_container_width=True,
                        height=400 
                    )
                    
                    # -------- START DEBUG PRINTS (check console) --------
                    # print(f"Tab {tab_name}: df_returned_from_editor type: {type(edited_df_from_editor)}")
                    # if isinstance(edited_df_from_editor, pd.DataFrame):
                    #     print(f"Tab {tab_name}: df_returned_from_editor shape: {edited_df_from_editor.shape}, Columns: {edited_df_from_editor.columns.tolist()}")
                    # -------- END DEBUG PRINTS --------

                    # IMPORTANT: Update session state with the DataFrame returned by the editor
                    if isinstance(edited_df_from_editor, pd.DataFrame):
                        st.session_state[df_edited_key] = edited_df_from_editor
                    else:
                        # This case should ideally not happen if st.data_editor behaves as expected
                        st.error("Warning: Data editor did not return a DataFrame. Edits might not be saved correctly.")
                        # print(f"Tab {tab_name}: Data editor returned type {type(edited_df_from_editor)}, NOT DataFrame!") # Console log
                        # Optionally, revert to the pre-edit state or keep the old session state value
                        # st.session_state[df_edited_key] = current_df_to_edit # Revert if editor fails


                    col_b1, col_b2 = st.columns(2)
                    with col_b1:
                        if st.button("Save for Review", key=f"review_changes_{sanitized_tab_key}"):
                            # The df_edited_key in session state *should* now have the latest from editor
                            st.session_state[review_mode_key] = True
                            st.experimental_rerun()
                    with col_b2:
                        if st.button("Reload Data from Source", key=f"reload_data_{sanitized_tab_key}"):
                            loaded_df = load_data_from_s3(tab_name)
                            if loaded_df is not None and not loaded_df.empty:
                                st.session_state[df_original_key] = loaded_df.copy(deep=True)
                                st.session_state[df_edited_key] = loaded_df.copy(deep=True) # Reset edited to fresh load
                                st.info(f"Data for {tab_name} reloaded.")
                            else:
                                st.session_state[df_original_key] = pd.DataFrame()
                                st.session_state[df_edited_key] = pd.DataFrame()
                                st.error(f"Failed to reload data for {tab_name}, or data source is empty.")
                            st.experimental_rerun()

    elif st.session_state.authenticated is False and not st.session_state.show_login_form:
        st.info("Please login to access NPS Update features.")
        if st.button("Show Login Form"):
            st.session_state.show_login_form = True
            st.experimental_rerun()
