import streamlit as st
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import json # For safer secret parsing

# Initialize AWS clients
try:
    secrets_client = boto3.client('secretsmanager')
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
except Exception as e:
    st.error(f"Error initializing AWS clients: {e}. Check AWS credentials/permissions.")
    st.stop()

# Configuration - UPDATE THESE WITH YOUR ACTUAL VALUES
SECRET_NAME = 'your-secret-name'
BUCKET_NAME = 'your-bucket-name'
SNS_TOPIC_ARN = 'your-sns-topic-arn'
DATA_FILES = {
    "Survey Weights": "data/survey_weights.csv",
    "BU Allocation Target": "data/bu_allocation_target.csv",
}

# System columns that are managed by the script and NOT directly edited by user for content (except is_active)
# last_modified and modified_by are purely system-set on save.
SYSTEM_COLUMNS_S3_ONLY = ['last_modified', 'modified_by']

# --- Helper Functions ---

def authenticate(username_attempt, password_attempt):
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret_string = response['SecretString']
        secrets = json.loads(secret_string)
        return secrets.get('username') == username_attempt and secrets.get('password') == password_attempt
    except Exception as e:
        st.sidebar.error(f"Authentication error: {e}")
        return False

def load_csv_s3(key, bucket_name):
    try:
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(csv_obj['Body'])
        # Ensure 'is_active' is boolean, handle missing
        if 'is_active' in df.columns:
            df['is_active'] = df['is_active'].astype(bool)
        else: # If file doesn't have it, add it as True by default for existing rows
            df['is_active'] = True
        return df
    except s3_client.exceptions.NoSuchKey:
        st.warning(f"File not found in S3 (s3://{bucket_name}/{key}). Will start with an empty table.")
        return pd.DataFrame(columns=['is_active']) # Start with is_active if file is new
    except Exception as e:
        st.error(f"Error loading data from S3 (key: {key}): {e}")
        return pd.DataFrame(columns=['is_active'])

def save_csv_s3(df, key, bucket_name):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        return True
    except Exception as e:
        st.error(f"Error saving data to S3 (key: {key}): {e}")
        return False

def calculate_row_diffs(original_df_snap, edited_df_snap):
    """
    Calculates added and deleted rows based on a hash of row content.
    Aligns columns before hashing for robustness.
    """
    temp_original = original_df_snap.copy()
    temp_edited = edited_df_snap.copy()

    if temp_original.empty and temp_edited.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Align columns before hashing. Use all columns present in either DataFrame.
    all_cols = sorted(list(set(temp_original.columns) | set(temp_edited.columns)))
    
    # Reindex and fill NaNs with empty strings for consistent hashing
    temp_original_aligned = temp_original.reindex(columns=all_cols).fillna('')
    temp_edited_aligned = temp_edited.reindex(columns=all_cols).fillna('')

    # Generate merge keys only if DataFrames are not empty after alignment
    if not temp_original_aligned.empty:
        temp_original_aligned['_merge_key'] = temp_original_aligned.astype(str).agg('-'.join, axis=1)
    else:
        # Create an empty df with _merge_key to prevent errors if one df is empty
        temp_original_aligned = pd.DataFrame(columns=all_cols + ['_merge_key'])


    if not temp_edited_aligned.empty:
        temp_edited_aligned['_merge_key'] = temp_edited_aligned.astype(str).agg('-'.join, axis=1)
    else:
        temp_edited_aligned = pd.DataFrame(columns=all_cols + ['_merge_key'])

    # Perform diff
    if temp_original_aligned['_merge_key'].dropna().empty: # Original was effectively empty
        added_rows_aligned = temp_edited_aligned.copy()
        deleted_rows_aligned = pd.DataFrame(columns=temp_original_aligned.columns)
    elif temp_edited_aligned['_merge_key'].dropna().empty: # Edited is effectively empty
        added_rows_aligned = pd.DataFrame(columns=temp_edited_aligned.columns)
        deleted_rows_aligned = temp_original_aligned.copy()
    else:
        added_rows_aligned = temp_edited_aligned[~temp_edited_aligned['_merge_key'].isin(temp_original_aligned['_merge_key'].dropna())]
        deleted_rows_aligned = temp_original_aligned[~temp_original_aligned['_merge_key'].isin(temp_edited_aligned['_merge_key'].dropna())]

    # Restore original column structure for the output (use original snapshot's columns as reference)
    added_rows = added_rows_aligned.drop('_merge_key', axis=1, errors='ignore')
    if not added_rows.empty:
         # Ensure columns in added_rows are a subset of or match edited_df_snap's columns
        added_rows = added_rows[edited_df_snap.columns.intersection(added_rows.columns)].copy()


    deleted_rows = deleted_rows_aligned.drop('_merge_key', axis=1, errors='ignore')
    if not deleted_rows.empty:
        # Ensure columns in deleted_rows are a subset of or match original_df_snap's columns
        deleted_rows = deleted_rows[original_df_snap.columns.intersection(deleted_rows.columns)].copy()
    
    return added_rows, deleted_rows


# --- Initialize session state ---
if 'auth' not in st.session_state: st.session_state.auth = False
if 'login_time' not in st.session_state: st.session_state.login_time = None
if 'current_user' not in st.session_state: st.session_state.current_user = None

# --- Sidebar and Authentication ---
st.sidebar.title("EDP Dashboard")

if not st.session_state.auth:
    st.sidebar.subheader("Login")
    username = st.sidebar.text_input("Username", key="login_username")
    password = st.sidebar.text_input("Password", type="password", key="login_password")
    if st.sidebar.button("Login", key="login_button"):
        if username and password:
            if authenticate(username, password):
                st.session_state.auth = True
                st.session_state.login_time = datetime.now()
                st.session_state.current_user = username
                st.sidebar.success("Authenticated successfully")
                st.experimental_rerun()
            else: st.sidebar.error("Authentication failed")
        else: st.sidebar.warning("Please enter username and password.")
else:
    st.sidebar.write(f"User: {st.session_state.current_user}")
    st.sidebar.write(f"Login Time: {st.session_state.login_time.strftime('%Y-%m-%d %H:%M:%S') if st.session_state.login_time else 'N/A'}")
    if st.sidebar.button("Logout", key="logout_button"):
        keys_to_delete = [k for k in st.session_state.keys() if k not in ['auth', 'login_time', 'current_user']]
        for key_to_del in st.session_state.keys():
            if key_to_del not in ['auth', 'login_time', 'current_user']:
                # More selectively clear app-specific state if needed
                if key_to_del.startswith(('original_df_', 'edited_df_', 'review_mode_', 'original_for_review_', 'edited_for_review_')):
                    del st.session_state[key_to_del]

        st.session_state.auth = False
        st.session_state.login_time = None
        st.session_state.current_user = None
        st.experimental_rerun()

    # --- Main Page Content (When Authenticated) ---
    st.title("EDP Data Editor")
    
    if not DATA_FILES:
        st.error("No data files configured.")
        st.stop()

    menu_options = list(DATA_FILES.keys())
    selected_menu = st.sidebar.radio("Select Data Point:", menu_options, key="menu_radio")

    st.header(f"Editing: {selected_menu}")
    s3_key = DATA_FILES[selected_menu]

    # Keys for session state
    s3_df_key = f"s3_df_{selected_menu}" # Stores the full df from S3 (or last save)
    original_for_review_key = f"original_for_review_{selected_menu}"
    edited_for_review_key = f"edited_for_review_{selected_menu}"
    review_mode_key = f"review_mode_{selected_menu}"


    # Load initial data or get from session state
    if s3_df_key not in st.session_state:
        st.session_state[s3_df_key] = load_csv_s3(s3_key, BUCKET_NAME)

    current_s3_df = st.session_state[s3_df_key]

    # Prepare DataFrame for the editor: drop system-only columns, keep 'is_active'
    df_for_editor = current_s3_df.drop(columns=SYSTEM_COLUMNS_S3_ONLY, errors='ignore').copy()
    # Ensure 'is_active' is boolean for the editor, especially if df_for_editor is empty initially
    if 'is_active' not in df_for_editor.columns and df_for_editor.empty:
        df_for_editor['is_active'] = pd.Series(dtype='bool')
    elif 'is_active' in df_for_editor.columns:
         df_for_editor['is_active'] = df_for_editor['is_active'].astype(bool)


    st.markdown("#### Current Data (Editable)")
    edited_df_from_editor = st.data_editor(
        df_for_editor, 
        num_rows="dynamic", 
        use_container_width=True, 
        height=500,
        key=f"data_editor_{selected_menu}",
        column_config={ # Ensure is_active is treated as a checkbox
            "is_active": st.column_config.CheckboxColumn(
                "Active",
                default=True, # Default for new rows
            )
        }
    )

    # "Review Changes" Button
    if st.button(f"Review Changes for {selected_menu}", key=f"review_btn_{selected_menu}"):
        st.session_state[review_mode_key] = True
        # Store snapshots for diffing:
        # 'df_for_editor' is the state *before* 'edited_df_from_editor' reflects changes
        st.session_state[original_for_review_key] = df_for_editor.copy() 
        st.session_state[edited_for_review_key] = edited_df_from_editor.copy()
        st.experimental_rerun()

    if st.session_state.get(review_mode_key, False):
        st.markdown("---")
        st.markdown("### Review of Proposed Changes")
        
        original_snapshot = st.session_state.get(original_for_review_key, pd.DataFrame())
        edited_snapshot = st.session_state.get(edited_for_review_key, pd.DataFrame())

        # --- UNCOMMENT FOR DEBUGGING THE DIFF INPUTS ---
        # st.markdown("Debug: Original Snapshot for Diff (`original_snapshot`)")
        # st.dataframe(original_snapshot)
        # st.markdown("Debug: Edited Snapshot for Diff (`edited_snapshot`)")
        # st.dataframe(edited_snapshot)
        # --- END DEBUG ---

        added_rows, deleted_rows = calculate_row_diffs(original_snapshot, edited_snapshot)
        
        no_changes_detected_flag = True

        if not added_rows.empty:
            no_changes_detected_flag = False
            st.markdown("#### Added Rows (or new versions of modified rows)")
            added_display = added_rows.copy()
            # Add expected system columns for display purposes in review
            added_display['last_modified (expected)'] = st.session_state.login_time.strftime('%Y-%m-%d %H:%M:%S') if st.session_state.login_time else "N/A"
            added_display['modified_by (expected)'] = st.session_state.current_user if st.session_state.current_user else "N/A"
            st.dataframe(added_display, use_container_width=True)

        if not deleted_rows.empty:
            no_changes_detected_flag = False
            st.markdown("#### Deleted Rows (or old versions of modified rows)")
            st.dataframe(deleted_rows, use_container_width=True)
        
        st.markdown(
            "**Note on Modified Rows:** Rows with changed content (including the 'Active' flag) will appear as one entry in 'Deleted Rows' (old version) "
            "and one entry in 'Added Rows' (new version) using this comparison method."
        )

        if no_changes_detected_flag:
            if original_snapshot.equals(edited_snapshot):
                st.info("No changes detected compared to the data presented for editing.")
            else:
                st.warning("Snapshots are different, but diff logic found no specific adds/deletes. Review manually or check debug dataframes if uncommented.")


        # "Save Changes" and "Cancel Review" buttons
        col_save, col_cancel_rev = st.columns(2)
        with col_save:
            if st.button(f"Confirm and Save Changes", key=f"save_btn_{selected_menu}"):
                # Use the 'edited_snapshot' which was captured when review started
                df_to_save_content = edited_snapshot.copy()

                # Prepare the final DataFrame for S3 (add/update system columns)
                final_df_to_s3 = df_to_save_content.copy()
                final_df_to_s3['last_modified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Actual save time
                final_df_to_s3['modified_by'] = st.session_state.current_user if st.session_state.current_user else "Unknown"
                # 'is_active' is already in df_to_save_content from the editor

                if save_csv_s3(final_df_to_s3, s3_key, BUCKET_NAME):
                    st.success(f"Data for {selected_menu} saved successfully.")
                    st.session_state[s3_df_key] = final_df_to_s3.copy() # Update main S3 state

                    # Send SNS notification using the diffs already calculated for review
                    sns_added_for_email, sns_deleted_for_email = calculate_row_diffs(original_snapshot, edited_snapshot) # Recalculate for safety or use stored
                    
                    email_subject = f"EDP Data Change: {selected_menu} by {st.session_state.current_user}"
                    email_body = (
                        f"User '{st.session_state.current_user}' saved changes to '{selected_menu}' "
                        f"at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n"
                        f"File: s3://{BUCKET_NAME}/{s3_key}\n\n"
                    )
                    if not sns_added_for_email.empty:
                        email_body += f"--- Added/Modified (New Version) ---\n{sns_added_for_email.to_string(index=False)}\n\n"
                    if not sns_deleted_for_email.empty:
                        email_body += f"--- Deleted/Modified (Old Version) ---\n{sns_deleted_for_email.to_string(index=False)}\n\n"
                    if sns_added_for_email.empty and sns_deleted_for_email.empty and original_snapshot.equals(edited_snapshot):
                         email_body += "No row content changes were made and saved.\n"
                    
                    try:
                        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Subject=email_subject, Message=email_body)
                        st.info("Change notification sent.")
                    except Exception as e_sns: st.warning(f"Failed to send SNS notification: {e_sns}")

                    st.session_state[review_mode_key] = False
                    st.session_state.pop(original_for_review_key, None)
                    st.session_state.pop(edited_for_review_key, None)
                    st.experimental_rerun()
                else: st.error(f"Failed to save data for {selected_menu}.")
        
        with col_cancel_rev:
            if st.button(f"Cancel Review", key=f"cancel_review_btn_{selected_menu}"):
                st.session_state[review_mode_key] = False
                st.session_state.pop(original_for_review_key, None)
                st.session_state.pop(edited_for_review_key, None)
                st.experimental_rerun()
            
    st.markdown("---")
    if st.button("Reload data from S3 (discard current edits)", key=f"reload_btn_{selected_menu}"):
        st.session_state[s3_df_key] = load_csv_s3(s3_key, BUCKET_NAME)
        # Clear editor and review states for this menu item
        if f"data_editor_{selected_menu}" in st.session_state: del st.session_state[f"data_editor_{selected_menu}"]
        if review_mode_key in st.session_state: st.session_state[review_mode_key] = False
        if original_for_review_key in st.session_state: st.session_state.pop(original_for_review_key, None)
        if edited_for_review_key in st.session_state: st.session_state.pop(edited_for_review_key, None)
        st.experimental_rerun()
