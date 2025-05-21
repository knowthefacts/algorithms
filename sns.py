import streamlit as st
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import json # For safer secret parsing

# Initialize AWS clients
# Ensure your ECS Task Role or local AWS credentials have permissions
# for Secrets Manager, S3, and SNS.
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
    "Survey Weights": "data/survey_weights.csv", # Example: "data/datapoint1.csv"
    "BU Allocation Target": "data/bu_allocation_target.csv", # Example: "data/datapoint2.csv"
    # "DataPoint3": "data/datapoint3.csv"
}

# --- Helper Functions ---

def authenticate(username_attempt, password_attempt):
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret_string = response['SecretString']
        # Assuming secret is stored as JSON: {"username": "user", "password": "pw"}
        secrets = json.loads(secret_string)
        
        # Case-sensitive comparison for username is typical
        # Password comparison should ideally be done with hashed passwords,
        # but for direct comparison from Secrets Manager:
        return secrets.get('username') == username_attempt and secrets.get('password') == password_attempt
    except Exception as e:
        st.sidebar.error(f"Authentication error: {e}")
        return False

def load_csv_s3(key, bucket_name):
    try:
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(csv_obj['Body'])
        return df
    except Exception as e:
        st.error(f"Error loading data from S3 (key: {key}): {e}")
        return pd.DataFrame() # Return empty DataFrame on error

def save_csv_s3(df, key, bucket_name):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        return True
    except Exception as e:
        st.error(f"Error saving data to S3 (key: {key}): {e}")
        return False

def calculate_row_diffs(original_df, edited_df):
    """
    Calculates added and deleted rows based on a hash of row content.
    'Modified' rows appear as one deleted (old version) and one added (new version).
    """
    # Work on copies to avoid modifying original DataFrames
    temp_original = original_df.copy()
    temp_edited = edited_df.copy()

    # Ensure consistent column order for generating merge keys if columns might be reordered
    # For st.data_editor, column order usually stays same as input, so explicit sort might be optional
    # common_cols = sorted(list(set(temp_original.columns) & set(temp_edited.columns)))
    # temp_original = temp_original[common_cols]
    # temp_edited = temp_edited[common_cols]

    if temp_original.empty and temp_edited.empty:
        return pd.DataFrame(columns=original_df.columns), pd.DataFrame(columns=original_df.columns)
    if temp_original.empty:
        return temp_edited.copy(), pd.DataFrame(columns=original_df.columns)
    if temp_edited.empty:
        return pd.DataFrame(columns=edited_df.columns), temp_original.copy()


    temp_original['_merge_key'] = temp_original.astype(str).agg('-'.join, axis=1)
    temp_edited['_merge_key'] = temp_edited.astype(str).agg('-'.join, axis=1)

    added_rows = temp_edited[~temp_edited['_merge_key'].isin(temp_original['_merge_key'])]
    deleted_rows = temp_original[~temp_original['_merge_key'].isin(temp_edited['_merge_key'])]

    # Drop the temporary merge key and restore original columns if they were subsetted
    added_rows = added_rows.drop('_merge_key', axis=1, errors='ignore')
    deleted_rows = deleted_rows.drop('_merge_key', axis=1, errors='ignore')
    
    # Ensure columns match the original df structure if possible
    if not added_rows.empty:
        added_rows = added_rows[original_df.columns.intersection(added_rows.columns)]
    if not deleted_rows.empty:
        deleted_rows = deleted_rows[original_df.columns.intersection(deleted_rows.columns)]


    return added_rows, deleted_rows


# --- Initialize session state ---
if 'auth' not in st.session_state:
    st.session_state.auth = False
if 'login_time' not in st.session_state:
    st.session_state.login_time = None
if 'current_user' not in st.session_state:
    st.session_state.current_user = None


# --- Sidebar and Authentication ---
st.sidebar.title("EDP Dashboard")

if not st.session_state.auth:
    st.sidebar.subheader("Login")
    username = st.sidebar.text_input("Username", key="login_username")
    password = st.sidebar.text_input("Password", type="password", key="login_password")
    if st.sidebar.button("Login", key="login_button"):
        if username and password: # Basic check for non-empty inputs
            if authenticate(username, password):
                st.session_state.auth = True
                st.session_state.login_time = datetime.now()
                st.session_state.current_user = username
                st.sidebar.success("Authenticated successfully")
                st.experimental_rerun()
            else:
                st.sidebar.error("Authentication failed")
        else:
            st.sidebar.warning("Please enter username and password.")
else:
    st.sidebar.write(f"User: {st.session_state.current_user}")
    st.sidebar.write(f"Login Time: {st.session_state.login_time.strftime('%Y-%m-%d %H:%M:%S') if st.session_state.login_time else 'N/A'}")
    if st.sidebar.button("Logout", key="logout_button"):
        for key in list(st.session_state.keys()): # Clear session state on logout
            if key.startswith('original_df_') or key.startswith('edited_df_'):
                del st.session_state[key]
        st.session_state.auth = False
        st.session_state.login_time = None
        st.session_state.current_user = None
        st.experimental_rerun()

    # --- Main Page Content (When Authenticated) ---
    st.title("EDP Data Editor")
    
    # Ensure DATA_FILES is not empty
    if not DATA_FILES:
        st.error("No data files configured. Please check the DATA_FILES setting in the script.")
        st.stop()

    menu_options = list(DATA_FILES.keys())
    selected_menu = st.sidebar.radio("Select Data Point:", menu_options, key="menu_radio")

    st.header(f"Editing: {selected_menu}")
    s3_key = DATA_FILES[selected_menu]

    # Load original data (or reload if forced)
    original_df_key = f"original_df_{selected_menu}"
    if original_df_key not in st.session_state:
        st.session_state[original_df_key] = load_csv_s3(s3_key, BUCKET_NAME)

    # This is the DataFrame reflecting the current state in S3 (includes system columns)
    current_s3_df = st.session_state[original_df_key]

    # DataFrame for display and editing (user-visible columns only)
    # These columns are considered "system-managed"
    system_columns = ['last_modified', 'is_active', 'modified_by'] # Add 'modified_by'
    display_df = current_s3_df.drop(columns=system_columns, errors='ignore').copy()

    st.markdown("#### Current Data (Editable)")
    # The key for st.data_editor ensures its state is preserved per data point
    edited_df_from_editor = st.data_editor(
        display_df, 
        num_rows="dynamic", 
        use_container_width=True, 
        height=500, # Adjust as needed
        key=f"data_editor_{selected_menu}"
    )

    # "Review Changes" Button
    if st.button(f"Review Changes for {selected_menu}", key=f"review_btn_{selected_menu}"):
        st.session_state[f'review_mode_{selected_menu}'] = True
        # Store the edited_df at the time of review for consistent display
        st.session_state[f'edited_for_review_{selected_menu}'] = edited_df_from_editor.copy()
        st.experimental_rerun() # Rerun to show review section

    if st.session_state.get(f'review_mode_{selected_menu}', False):
        st.markdown("---")
        st.markdown("### Review of Proposed Changes")
        
        original_display_for_review = display_df # This is the 'before' state without system columns
        edited_for_review = st.session_state.get(f'edited_for_review_{selected_menu}', pd.DataFrame(columns=original_display_for_review.columns))

        added_rows, deleted_rows = calculate_row_diffs(original_display_for_review, edited_for_review)
        
        no_changes = True

        if not added_rows.empty:
            no_changes = False
            st.markdown("#### Added Rows")
            # For display, simulate what these rows would look like with system columns
            added_display = added_rows.copy()
            added_display['last_modified'] = st.session_state.login_time.strftime('%Y-%m-%d %H:%M:%S') if st.session_state.login_time else "N/A"
            added_display['modified_by'] = st.session_state.current_user if st.session_state.current_user else "N/A"
            added_display['is_active'] = True
            st.dataframe(added_display, use_container_width=True)

        if not deleted_rows.empty:
            no_changes = False
            st.markdown("#### Deleted Rows")
             # For display, show them as they were, perhaps mark as inactive
            deleted_display = deleted_rows.copy()
            # If original df had these sys columns, they'd be here. Otherwise, add placeholders.
            original_system_cols_present = all(col in current_s3_df.columns for col in ['last_modified', 'modified_by', 'is_active'])
            if original_system_cols_present: # Try to get original values for context
                 # This requires merging deleted_rows back to current_s3_df to get their original sys values
                 # For simplicity now, just mark as inactive conceptually
                pass # More complex logic to show original system values for deleted rows

            deleted_display['is_active (review)'] = False # Indicate inactive for review
            st.dataframe(deleted_display, use_container_width=True)
        
        st.markdown(
            "**Note on Modified Rows:** Rows with changed content will appear as one entry in 'Deleted Rows' (old version) "
            "and one entry in 'Added Rows' (new version) using this comparison method."
        )

        if no_changes:
            st.info("No changes detected compared to the current data source.")

        # "Save Changes" Button (appears only in review mode)
        if st.button(f"Confirm and Save Changes for {selected_menu}", key=f"save_btn_{selected_menu}"):
            # Use the edited_df_from_editor which reflects the latest state from data_editor
            # or the st.session_state[f'edited_for_review_{selected_menu}'] if review is final decision point
            
            df_to_process_for_saving = st.session_state.get(f'edited_for_review_{selected_menu}', edited_df_from_editor).copy()

            # Prepare the DataFrame for saving (add system columns)
            final_df_to_save = df_to_process_for_saving.copy()
            final_df_to_save['last_modified'] = st.session_state.login_time.strftime('%Y-%m-%d %H:%M:%S') if st.session_state.login_time else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            final_df_to_save['modified_by'] = st.session_state.current_user if st.session_state.current_user else "Unknown"
            final_df_to_save['is_active'] = True

            if save_csv_s3(final_df_to_save, s3_key, BUCKET_NAME):
                st.success(f"Data for {selected_menu} saved successfully.")
                
                # Update the session state's original_df to reflect the new S3 state
                st.session_state[original_df_key] = final_df_to_save.copy()

                # Send SNS notification
                # For SNS, use diffs based on user-visible columns (original_display_for_review vs df_to_process_for_saving)
                sns_added, sns_deleted = calculate_row_diffs(original_display_for_review, df_to_process_for_saving)
                
                email_subject = f"EDP Data Change Notification: {selected_menu}"
                email_body = (
                    f"User '{st.session_state.current_user}' made changes to the data point '{selected_menu}' "
                    f"at {st.session_state.login_time.strftime('%Y-%m-%d %H:%M:%S') if st.session_state.login_time else 'N/A'}.\n\n"
                    f"File updated: s3://{BUCKET_NAME}/{s3_key}\n\n"
                )
                if not sns_added.empty:
                    email_body += f"--- Added Rows ---\n{sns_added.to_string(index=False)}\n\n"
                if not sns_deleted.empty:
                    email_body += f"--- Deleted Rows ---\n{sns_deleted.to_string(index=False)}\n\n"
                if sns_added.empty and sns_deleted.empty:
                     email_body += "No row content changes were made (e.g., might be a save without actual data modification, or only system column updates if that were allowed via UI).\n"
                
                try:
                    sns_client.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Subject=email_subject,
                        Message=email_body
                    )
                    st.info("Change notification sent.")
                except Exception as e_sns:
                    st.warning(f"Failed to send SNS notification: {e_sns}")

                # Exit review mode and rerun
                st.session_state[f'review_mode_{selected_menu}'] = False
                st.session_state.pop(f'edited_for_review_{selected_menu}', None) # Clean up
                st.experimental_rerun()
            else:
                st.error(f"Failed to save data for {selected_menu}.")
        
        if st.button(f"Cancel Review for {selected_menu}", key=f"cancel_review_btn_{selected_menu}"):
            st.session_state[f'review_mode_{selected_menu}'] = False
            st.session_state.pop(f'edited_for_review_{selected_menu}', None) # Clean up
            st.experimental_rerun()
            
    st.markdown("---")
    if st.button("Reload data from S3 (discard current edits)", key=f"reload_btn_{selected_menu}"):
        st.session_state[original_df_key] = load_csv_s3(s3_key, BUCKET_NAME)
        if f"data_editor_{selected_menu}" in st.session_state: # Clear editor state
            del st.session_state[f"data_editor_{selected_menu}"]
        if f'review_mode_{selected_menu}' in st.session_state:
            st.session_state[f'review_mode_{selected_menu}'] = False
        if f'edited_for_review_{selected_menu}' in st.session_state:
             st.session_state.pop(f'edited_for_review_{selected_menu}', None)
        st.experimental_rerun()
