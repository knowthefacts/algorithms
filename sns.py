import streamlit as st
import pandas as pd
import boto3
from io import StringIO, BytesIO
from datetime import datetime
import json

# Initialize AWS clients
try:
    secrets_client = boto3.client('secretsmanager')
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
except Exception as e:
    st.error(f"Error initializing AWS clients: {e}. Check AWS credentials/permissions.")
    st.stop()

# Configuration
SECRET_NAME = 'your-secret-name'  # UPDATE THIS
BUCKET_NAME = 'your-bucket-name'  # UPDATE THIS
SNS_TOPIC_ARN = 'your-sns-topic-arn'  # UPDATE THIS
DATA_FILES = {
    "Survey Weights": "data/survey_weights.csv",
    "BU Allocation Target": "data/bu_allocation_target.csv",
}

DATETIME_FORMAT_S3 = '%Y-%m-%d %H:%M:%S'
# No SYSTEM_COLUMNS_S3_ONLY needed as all are passed to editor

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
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
        s3_data_bytes = s3_object['Body'].read()

        # Assume 'last_modified' and 'is_active' columns exist
        df = pd.read_csv(
            BytesIO(s3_data_bytes),
            parse_dates=['last_modified'], # Parse last_modified as datetime
            infer_datetime_format=True
        )
        
        # Ensure 'is_active' is boolean
        if 'is_active' in df.columns:
            if not pd.api.types.is_bool_dtype(df['is_active']):
                true_values = ['true', '1', 't', 'yes']
                is_active_series = df['is_active'].astype(str).str.lower()
                df['is_active'] = is_active_series.isin(true_values)
            elif df['is_active'].isnull().any(): # Handle nullable booleans if any
                 df['is_active'] = df['is_active'].fillna(True) # Default for NaN/None booleans
            df['is_active'] = df['is_active'].astype(bool)
        else:
            # This case should ideally not happen if 'is_active' is always in tables
            st.warning(f"'is_active' column not found in {key}. Adding it as True.")
            df['is_active'] = True
        
        return df
    except s3_client.exceptions.NoSuchKey:
        st.warning(f"File not found (s3://{bucket_name}/{key}). Starting with empty table.")
        # Return empty DF with expected columns for editor if file is new
        return pd.DataFrame({
            'last_modified': pd.Series(dtype='datetime64[ns]'), 
            'is_active': pd.Series(dtype='bool')
        })
    except Exception as e:
        st.error(f"Error loading data from S3 (key: {key}): {e}")
        return pd.DataFrame({
            'last_modified': pd.Series(dtype='datetime64[ns]'),
            'is_active': pd.Series(dtype='bool')
        })


def save_csv_s3(df, key, bucket_name):
    try:
        csv_buffer = StringIO()
        df_to_save = df.copy()
        
        if 'is_active' in df_to_save.columns:
            df_to_save['is_active'] = df_to_save['is_active'].astype(bool)
        
        df_to_save.to_csv(csv_buffer, index=False, date_format=DATETIME_FORMAT_S3)
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
    
    temp_original_aligned = temp_original.reindex(columns=all_cols)
    temp_edited_aligned = temp_edited.reindex(columns=all_cols)

    temp_original_for_hash = temp_original_aligned.fillna('').astype(str)
    temp_edited_for_hash = temp_edited_aligned.fillna('').astype(str)

    if not temp_original_for_hash.empty:
        temp_original_for_hash['_merge_key'] = temp_original_for_hash.agg('-'.join, axis=1)
    else:
        temp_original_for_hash = pd.DataFrame(columns=all_cols + ['_merge_key'])

    if not temp_edited_for_hash.empty:
        temp_edited_for_hash['_merge_key'] = temp_edited_for_hash.agg('-'.join, axis=1)
    else:
        temp_edited_for_hash = pd.DataFrame(columns=all_cols + ['_merge_key'])
    
    if temp_original_for_hash['_merge_key'].dropna().empty:
        added_rows_indices = temp_edited_aligned.index
        deleted_rows_indices = pd.Index([])
    elif temp_edited_for_hash['_merge_key'].dropna().empty:
        added_rows_indices = pd.Index([])
        deleted_rows_indices = temp_original_aligned.index
    else:
        added_mask = ~temp_edited_for_hash['_merge_key'].isin(temp_original_for_hash['_merge_key'].dropna())
        deleted_mask = ~temp_original_for_hash['_merge_key'].isin(temp_edited_for_hash['_merge_key'].dropna())
        added_rows_indices = temp_edited_aligned[added_mask].index
        deleted_rows_indices = temp_original_aligned[deleted_mask].index

    added_rows = edited_df_snap.loc[added_rows_indices].copy()
    deleted_rows = original_df_snap.loc[deleted_rows_indices].copy()
    
    return added_rows, deleted_rows


# --- Initialize session state ---
if 'auth' not in st.session_state: st.session_state.auth = False
if 'login_time' not in st.session_state: st.session_state.login_time = None # User's login time
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
    # Display user login time, not data's last_modified time
    st.sidebar.write(f"Login Time: {st.session_state.login_time.strftime(DATETIME_FORMAT_S3) if st.session_state.login_time else 'N/A'}")
    if st.sidebar.button("Logout", key="logout_button"):
        for key_to_del in list(st.session_state.keys()):
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

    current_s3_df = st.session_state[s3_df_key] # This is the DataFrame with all columns

    # All columns from current_s3_df are passed to the editor
    df_for_editor = current_s3_df.copy() 
    
    # Ensure 'is_active' is bool for the editor, even if load_csv_s3 handled it, this is a safeguard
    if 'is_active' in df_for_editor.columns:
        df_for_editor['is_active'] = df_for_editor['is_active'].astype(bool)
    else: # Should not happen given assumptions
        st.error(f"'is_active' column is missing from data for {selected_menu} after loading.")
        df_for_editor['is_active'] = True # Add as a fallback

    # Ensure 'last_modified' is datetime for the editor if present
    if 'last_modified' in df_for_editor.columns:
        try:
            df_for_editor['last_modified'] = pd.to_datetime(df_for_editor['last_modified'])
        except Exception: # If conversion fails, leave as is or handle
            pass 


    st.markdown("#### Current Data (Editable)")
    edited_df_from_editor = st.data_editor(
        df_for_editor, # Pass all columns
        num_rows="dynamic", 
        use_container_width=True, 
        height=500,
        key=f"data_editor_{selected_menu}",
        column_config={
            "is_active": st.column_config.CheckboxColumn("Active", default=True),
            "last_modified": st.column_config.DatetimeColumn(
                "Last Modified",
                format="YYYY-MM-DD HH:mm:ss", # Display format in editor
                # disabled=True, # Consider making this read-only if st.data_editor supports it well
            )
        }
    )

    if st.button(f"Review Changes for {selected_menu}", key=f"review_btn_{selected_menu}"):
        st.session_state[review_mode_key] = True
        st.session_state[original_for_review_key] = df_for_editor.copy() # Snapshot before edits from this session
        st.session_state[edited_for_review_key] = edited_df_from_editor.copy() # Snapshot of current editor state
        st.experimental_rerun()

    if st.session_state.get(review_mode_key, False):
        st.markdown("---")
        st.markdown("### Review of Proposed Changes")
        
        original_snapshot = st.session_state.get(original_for_review_key, pd.DataFrame())
        edited_snapshot = st.session_state.get(edited_for_review_key, pd.DataFrame())

        # --- UNCOMMENT FOR DEBUGGING ---
        # st.markdown("Debug: Original Snapshot (Types from S3/previous save)")
        # st.dataframe(original_snapshot)
        # st.write(original_snapshot.dtypes)
        # st.markdown("Debug: Edited Snapshot (Types from editor)")
        # st.dataframe(edited_snapshot)
        # st.write(edited_snapshot.dtypes)
        # --- END DEBUG ---

        added_df, deleted_df = calculate_hashed_row_diffs(original_snapshot, edited_snapshot)
        
        no_changes_detected_flag = True

        if not added_df.empty:
            no_changes_detected_flag = False
            st.markdown("#### Added Rows (or new versions of modified rows)")
            st.dataframe(added_df, use_container_width=True) # Display with original types

        if not deleted_df.empty:
            no_changes_detected_flag = False
            st.markdown("#### Deleted Rows (or old versions of modified rows)")
            st.dataframe(deleted_df, use_container_width=True)
        
        st.info(
            "**Understanding Changes:** "
            "Changes are identified by comparing entire row contents (all data converted to text for this comparison). "
            "If a row's content (including 'last_modified' if user changed it, or 'is_active') is modified, "
            "its old version will appear in 'Deleted Rows' and its new version in 'Added Rows'."
        )

        if no_changes_detected_flag:
            original_for_eq = original_snapshot.fillna('').astype(str)
            edited_for_eq = edited_snapshot.fillna('').astype(str)
            if original_for_eq.equals(edited_for_eq):
                st.info("No changes detected compared to the data presented for editing.")
            else:
                st.warning("Snapshots appear different by content, but diff logic found no distinct adds/deletes. Review debug dataframes if uncommented.")


        col_save, col_cancel_rev = st.columns(2)
        with col_save:
            if st.button(f"Confirm and Save Changes", key=f"save_btn_{selected_menu}"):
                df_to_save_content = edited_snapshot.copy()

                # System override for last_modified
                current_time_for_save = datetime.now()
                df_to_save_content['last_modified'] = current_time_for_save
                df_to_save_content['is_active'] = df_to_save_content['is_active'].astype(bool)
                # No 'modified_by' in this version

                if save_csv_s3(df_to_save_content, s3_key, BUCKET_NAME):
                    st.success(f"Data for {selected_menu} saved successfully.")
                    st.session_state[s3_df_key] = df_to_save_content.copy() # Update S3 state in session

                    sns_added, sns_deleted = calculate_hashed_row_diffs(original_snapshot, edited_snapshot)
                    
                    email_subject = f"EDP Data Change: {selected_menu} by {st.session_state.current_user}"
                    email_body = (
                        f"User '{st.session_state.current_user}' saved changes to '{selected_menu}' "
                        f"at {current_time_for_save.strftime(DATETIME_FORMAT_S3)}.\n"
                        f"File: s3://{BUCKET_NAME}/{s3_key}\n\n"
                    )
                    if not sns_added.empty:
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
