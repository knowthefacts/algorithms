import streamlit as st
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager')
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Configuration
SECRET_NAME = 'your-secret-name'  # Update this
BUCKET_NAME = 'your-bucket-name'  # Update this
SNS_TOPIC_ARN = 'your-sns-topic-arn'  # Update this
DATA_FILES = {
    "DataPoint1": "data/datapoint1.csv",
    "DataPoint2": "data/datapoint2.csv",
    "DataPoint3": "data/datapoint3.csv"
}

# Authenticate user
def authenticate(username, password):
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secrets = eval(response['SecretString'])
    return secrets.get('username') == username and secrets.get('password') == password

# Load data from S3
def load_csv_s3(key):
    csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(csv_obj['Body'])

# Save data to S3
def save_csv_s3(df, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())

# Initialize session state
if 'auth' not in st.session_state:
    st.session_state.auth = False

st.sidebar.title("EDP Dashboard")

# Authentication section
if not st.session_state.auth:
    st.sidebar.subheader("Login")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Login"):
        if authenticate(username, password):
            st.session_state.auth = True
            st.session_state.login_time = datetime.now().strftime('%m/%d/%Y %H:%M')
            st.sidebar.success("Authenticated successfully")
            st.experimental_rerun()
        else:
            st.sidebar.error("Authentication failed")
else:
    menu = st.sidebar.radio("", list(DATA_FILES.keys()))

    st.title("EDP Data Editor")
    st.subheader(menu)

    if f"original_df_{menu}" not in st.session_state:
        st.session_state[f"original_df_{menu}"] = load_csv_s3(DATA_FILES[menu])

    original_df = st.session_state[f"original_df_{menu}"]
    display_df = original_df.drop(columns=['last_modified', 'is_active'], errors='ignore')

    edited_df = st.data_editor(display_df, num_rows="dynamic", use_container_width=True, height=500)

    if st.button(f"Review Changes for {menu}"):
        added = edited_df.merge(display_df, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
        deleted = display_df.merge(edited_df, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
        modified = pd.concat([edited_df, display_df]).drop_duplicates(keep=False)
        modified = modified[~modified.isin(added.to_dict(orient='list')).all(1)]
        modified = modified[~modified.isin(deleted.to_dict(orient='list')).all(1)]

        if not added.empty:
            st.write("### Added Rows")
            st.dataframe(added, use_container_width=True)

        if not deleted.empty:
            st.write("### Deleted Rows")
            st.dataframe(deleted, use_container_width=True)

        if not modified.empty:
            st.write("### Modified Rows")
            st.dataframe(modified, use_container_width=True)

        if added.empty and deleted.empty and modified.empty:
            st.info("No changes detected.")

    if st.button(f"Save Changes for {menu}"):
        edited_df['last_modified'] = st.session_state.login_time
        edited_df['is_active'] = True
        save_csv_s3(edited_df, DATA_FILES[menu])
        st.session_state[f"original_df_{menu}"] = edited_df.copy()

        email_subject = f"NPS table changes: {menu}"
        email_body = f"""Changes for table {menu}:

Added Rows:\n{added.to_string(index=False)}\n
Modified Rows:\n{modified.to_string(index=False)}\n
Deleted Rows:\n{deleted.to_string(index=False)}"""

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=email_subject,
            Message=email_body
        )

        st.success(f"Data for {menu} saved successfully. Notification sent.")
