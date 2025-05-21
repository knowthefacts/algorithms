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

# Configuration
SECRET_NAME = 'your-secret-name'
BUCKET_NAME = 'your-bucket-name'
SNS_TOPIC_ARN = 'your-sns-topic-arn'
DATA_FILES = {
    "Survey Weights": "data/survey_weights.csv",
    "BU Allocation Target": "data/bu_allocation_target.csv",
}

SYSTEM_COLUMNS_S3_ONLY = ['last_modified', 'modified_by']
DATETIME_FORMAT_S3 = '%Y-%m-%d %H:%M:%S' # Define your desired format for S3

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
        # Let pandas infer types, parse 'last_modified' specifically if it exists
        # Get column names first to check if 'last_modified' is present
        temp_df_for_cols = pd.read_csv(StringIO(csv_obj['Body'].read().decode('utf-8')), nrows=0)
        parse_dates_list = []
        if 'last_modified' in temp_df_for_cols.columns:
            parse_dates_list.append('last_modified')
        
        # Reread the CSV with type inference and date parsing
        csv_obj['Body'].seek(0) # Reset stream position
        df = pd.read_csv(
            csv_obj['Body'], 
            parse_dates=parse_dates_list if parse_dates_list else False, # Only parse if column exists
            infer_datetime_format=True # Helps pandas guess format if not exact
        )
        
        if 'is_active' in df.columns:
            # Handle various representations for robust boolean conversion
            if df['is_active'].dtype == 'object': # If read as string
                 df['is_active'] = df['is_active'].astype(str).str.lower().map({
                    'true': True, '1': True, 
                    'false': False, '0': False,
                    '': False # Treat empty string as False
                }).fillna(True) # Default for unmappable strings
            df['is_active'] = df['is_active'].astype(bool)
        else:
            df['is_active'] = True
        
        return df
    except s3_client.exceptions.NoSuchKey:
        st.warning(f"File not found (s3://{bucket_name}/{key}). Starting with empty table.")
        return pd.DataFrame({'is_active': pd.Series(dtype='bool')})
    except Exception as e:
        st.error(f"Error loading data from S3 (key: {key}): {e}")
        return pd.DataFrame({'is_active': pd.Series(dtype='bool')})


def save_csv_s3(df, key, bucket_name):
    try:
        csv_buffer = StringIO()
        df_to_save = df.copy()
        
        # Ensure 'is_active' is bool for saving
        if 'is_active' in df_to_save.columns:
            df_to_save['is_active'] = df_to_save['is_active'].astype(bool)
        
        # Pandas to_csv handles datetime objects by default, usually to ISO 8601.
        # If a specific format like YYYY-MM-DD HH:MM:SS is needed *in the CSV*:
        if 'last_modified' in df_to_save.columns and pd.api.types.is_datetime64_any_dtype(df_to_save['last_modified']):
            # If you want to ensure it's saved without timezone and in specific format:
            # df_to_save['last_modified'] = df_to_save['last_modified'].dt.strftime(DATETIME_FORMAT_S3)
            # However, saving as string might make reparsing harder.
            # For now, let pandas handle datetime saving; it's usually fine.
            pass

        df_to_save.to_csv(csv_buffer, index=False, date_format=DATETIME_FORMAT_S3) # Use date_format for consistency
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        return True
    except Exception as e:
        st.error(f"Error saving data to S3 (key: {key}): {e}")
        return False

def calculate_hashed_row_diffs(original_df_snap, edited_df_snap):
    temp_original = original_df_snap.copy()
    temp_edited = edited_df_snap.copy()

    if temp_original.empty and temp_edited.empty:
        return pd.DataFrame(), pd.DataFrame()

    all_cols = sorted(list(set(temp_original.columns) | set(temp_edited.columns)))
    
    temp_original_aligned = temp_original.reindex(columns=all_cols) # Keep original types for now
    temp_edited_aligned = temp_edited.reindex(columns=all_cols)   # Keep original types

    # For hashing, convert all to string. fillna('') is important here.
    temp_original_for_hash = temp_original_aligned.fillna('').astype(str)
    temp_edited_for_hash = temp_edited_aligned.fillna('').astype(str)


    if not temp_original_for_hash.empty:
        temp_original_for_hash['_merge_key'] = temp_original_for_hash.agg('-'.join, axis=1)
    else: # Should not happen if all_cols is derived correctly
        temp_original_for_hash = pd.DataFrame(columns=all_cols + ['_merge_key'])


    if not temp_edited_for_hash.empty:
        temp_edited_for_hash['_merge_key'] = temp_edited_for_hash.agg('-'.join, axis=1)
    else:
        temp_edited_for_hash = pd.DataFrame(columns=all_cols + ['_merge_key'])
    
    # Perform diff based on hashed keys
    if temp_original_for_hash['_merge_key'].dropna().empty: # original was effectively empty or all NaNs
        added_rows_indices = temp_edited_aligned.index
        deleted_rows_indices = pd.Index([])
    elif temp_edited_for_hash['_merge_key'].dropna().empty: # edited is effectively empty or all NaNs
        added_rows_indices = pd.Index([])
        deleted_rows_indices = temp_original_aligned.index
    else:
        added_mask = ~temp_edited_for_hash['_merge_key'].isin(temp_original_for_hash['_merge_key'].dropna())
        deleted_mask = ~temp_original_for_hash['_merge_key'].isin(temp_edited_for_hash['_merge_key'].dropna())
        added_rows_indices = temp_edited_aligned[added_mask].index
        deleted_rows_indices = temp_original_aligned[deleted_mask].index

    # Get the actual rows from the original snapshots (with original dtypes)
    added_rows = edited_df_snap.loc[added_rows_indices].copy()
    deleted_rows = original_df_snap.loc[deleted_rows_indices].copy()
    
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
    st.sidebar.write(f"Login Time: {st.session_state.login_time.strftime(DATETIME_FORMAT_S3) if st.session_state.login_time else 'N/A'}") # Use defined format
    if st.sidebar.button("Logout", key="logout_button"):
        for key_to_del in list(st.session_state.keys()): # Iterate over a copy
            if key_to_del not in ['auth', 'login_time', 'current_user']:
                 if key_to_del.startswith(('s3_df_', 'original_for_review_', 'edited_for_review_', 'review_mode_')):
                    del st.session_state[key_to_del]
        st.session_state.auth = False
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

    s3_df_key = f"s3_df_{selected_menu}"
    original_for_review_key = f"original_for_review_{selected_menu}"
    edited_for_review_key = f"edited_for_review_{selected_menu}"
    review_mode_key = f"review_mode_{selected_menu}"

    if s3_df_key not in st.session_state:
        st.session_state[s3_df_key] = load_csv_s3(s3_key, BUCKET_NAME)

    current_s3_df = st.session_state[s3_df_key] # Has inferred types, last_modified as datetime

    # DataFrame for editor (user-visible columns)
    df_for_editor = current_s3_df.drop(columns=SYSTEM_COLUMNS_S3_ONLY, errors='ignore').copy()
    
    if 'is_active' not in df_for_editor.columns:
        if df_for_editor.empty:
            df_for_editor['is_active'] = pd.Series(dtype='bool')
        else: # Should be added by load_csv_s3
            df_for_editor['is_active'] = True
    df_for_editor['is_active'] = df_for_editor['is_active'].astype(bool)


    st.markdown("#### Current Data (Editable)")
    # st.data_editor will try to infer input types for columns.
    # For datetime columns, it should offer a date picker if pandas dtype is datetime64.
    edited_df_from_editor = st.data_editor(
        df_for_editor, # Should have correct dtypes from load_csv_s3
        num_rows="dynamic", 
        use_container_width=True, 
        height=500,
        key=f"data_editor_{selected_menu}",
        column_config={
            "is_active": st.column_config.CheckboxColumn("Active", default=True)
            # If you have actual datetime columns to edit (not system ones),
            # you might use st.column_config.DatetimeColumn
        }
    )
    # edited_df_from_editor will have dtypes as modified/set by the editor.

    if st.button(f"Review Changes for {selected_menu}", key=f"review_btn_{selected_menu}"):
        st.session_state[review_mode_key] = True
        st.session_state[original_for_review_key] = df_for_editor.copy() 
        st.session_state[edited_for_review_key] = edited_df_from_editor.copy()
        st.experimental_rerun()

    if st.session_state.get(review_mode_key, False):
        st.markdown("---")
        st.markdown("### Review of Proposed Changes")
        
        original_snapshot = st.session_state.get(original_for_review_key, pd.DataFrame())
        edited_snapshot = st.session_state.get(edited_for_review_key, pd.DataFrame())

        # --- UNCOMMENT FOR DEBUGGING THE DIFF INPUTS ---
        # st.markdown("Debug: Original Snapshot for Diff (Types should be inferred)")
        # st.dataframe(original_snapshot)
        # st.write(original_snapshot.dtypes)
        # st.markdown("Debug: Edited Snapshot for Diff (Types from editor)")
        # st.dataframe(edited_snapshot)
        # st.write(edited_snapshot.dtypes)
        # --- END DEBUG ---

        added_df, deleted_df = calculate_hashed_row_diffs(original_snapshot, edited_snapshot)
        
        no_changes_detected_flag = True

        if not added_df.empty:
            no_changes_detected_flag = False
            st.markdown("#### Added Rows (or new versions of modified rows)")
            added_display = added_df.copy()
            added_display['last_modified (expected)'] = st.session_state.login_time.strftime(DATETIME_FORMAT_S3) if st.session_state.login_time else "N/A"
            added_display['modified_by (expected)'] = st.session_state.current_user if st.session_state.current_user else "N/A"
            st.dataframe(added_display, use_container_width=True) # Display with original types

        if not deleted_df.empty:
            no_changes_detected_flag = False
            st.markdown("#### Deleted Rows (or old versions of modified rows)")
            st.dataframe(deleted_df, use_container_width=True) # Display with original types
        
        st.info(
            "**Understanding Changes:** "
            "Changes are identified by comparing entire row contents (all data converted to text for this comparison). "
            "If a row's content is modified, its old version will appear in 'Deleted Rows' and "
            "its new version will appear in 'Added Rows'."
        )

        if no_changes_detected_flag:
            # For equality check, convert to string to mimic hashing behavior
            original_for_eq = original_snapshot.fillna('').astype(str)
            edited_for_eq = edited_snapshot.fillna('').astype(str)
            if original_for_eq.equals(edited_for_eq):
                st.info("No changes detected compared to the data presented for editing.")
            else:
                st.warning("Snapshots appear different by content, but diff logic found no distinct adds/deletes. This could be due to subtle data changes (e.g., whitespace, float precision) not altering the hash, or an issue in hashing. Review debug dataframes if uncommented.")


        col_save, col_cancel_rev = st.columns(2)
        with col_save:
            if st.button(f"Confirm and Save Changes", key=f"save_btn_{selected_menu}"):
                df_to_save_content = edited_snapshot.copy() # Dtypes from editor

                final_df_to_s3 = df_to_save_content.copy()
                current_time_for_save = datetime.now()
                final_df_to_s3['last_modified'] = current_time_for_save # This is a datetime object
                final_df_to_s3['modified_by'] = st.session_state.current_user if st.session_state.current_user else "Unknown"
                final_df_to_s3['is_active'] = final_df_to_s3['is_active'].astype(bool)

                if save_csv_s3(final_df_to_s3, s3_key, BUCKET_NAME):
                    st.success(f"Data for {selected_menu} saved successfully.")
                    # Update session state with the DataFrame that includes system cols and correct types
                    st.session_state[s3_df_key] = final_df_to_s3.copy() 

                    sns_added, sns_deleted = calculate_hashed_row_diffs(original_snapshot, edited_snapshot)
                    
                    email_subject = f"EDP Data Change: {selected_menu} by {st.session_state.current_user}"
                    email_body = (
                        f"User '{st.session_state.current_user}' saved changes to '{selected_menu}' "
                        f"at {current_time_for_save.strftime(DATETIME_FORMAT_S3)}.\n" # Use defined format
                        f"File: s3://{BUCKET_NAME}/{s3_key}\n\n"
                    )
                    if not sns_added.empty:
                        # For email, convert to string for reliable text representation
                        email_body += f"--- Rows Added / New Versions of Modified Rows ---\n{sns_added.fillna('').astype(str).to_string(index=False)}\n\n"
                    if not sns_deleted.empty:
                        email_body += f"--- Rows Deleted / Old Versions of Modified Rows ---\n{sns_deleted.fillna('').astype(str).to_string(index=False)}\n\n"
                    
                    original_for_eq_sns = original_snapshot.fillna('').astype(str)
                    edited_for_eq_sns = edited_snapshot.fillna('').astype(str)
                    if sns_added.empty and sns_deleted.empty and original_for_eq_sns.equals(edited_for_eq_sns):
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
        if f"data_editor_{selected_menu}" in st.session_state: del st.session_state[f"data_editor_{selected_menu}"]
        if review_mode_key in st.session_state: st.session_state[review_mode_key] = False
        if original_for_review_key in st.session_state: st.session_state.pop(original_for_review_key, None)
        if edited_for_review_key in st.session_state: st.session_state.pop(edited_for_review_key, None)
        st.experimental_rerun()
