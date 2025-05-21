import streamlit as st
import pandas as pd
import boto3
import difflib
from io import StringIO

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager')
s3_client = boto3.client('s3')

# Configuration
SECRET_NAME = 'your-secret-name'  # Update this
BUCKET_NAME = 'your-bucket-name'  # Update this
DATA_FILES = {
    "DataPoint1": "data/datapoint1.csv",
    "DataPoint2": "data/datapoint2.csv",
    "DataPoint3": "data/datapoint3.csv"
}

# Helper function to authenticate user
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

# UI
st.sidebar.title("Navigation")
menu = st.sidebar.radio("", ["Home", "NPS Update"])

if menu == "Home":
    st.title("Welcome to EDP Dashboard")
    st.write("Still under development.")

elif menu == "NPS Update":
    if 'auth' not in st.session_state:
        st.session_state.auth = False

    if not st.session_state.auth:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            if authenticate(username, password):
                st.session_state.auth = True
                st.success("Authenticated successfully")
            else:
                st.error("Authentication failed")

    if st.session_state.auth:
        tabs = st.tabs(list(DATA_FILES.keys()))

        for i, (tab_name, file_key) in enumerate(DATA_FILES.items()):
            with tabs[i]:
                st.subheader(tab_name)

                if f"df_{tab_name}" not in st.session_state:
                    st.session_state[f"df_{tab_name}"] = load_csv_s3(file_key)

                df = st.session_state[f"df_{tab_name}"]
                edited_df = st.data_editor(df, num_rows="dynamic", key=f"editor_{tab_name}")

                if st.button(f"Review Changes for {tab_name}"):
                    diff = difflib.unified_diff(
                        df.to_csv(index=False).splitlines(),
                        edited_df.to_csv(index=False).splitlines(),
                        fromfile="original",
                        tofile="modified",
                        lineterm=""
                    )
                    diff_text = '\n'.join(diff)
                    if diff_text:
                        st.text_area("Review Modifications", diff_text, height=300)
                    else:
                        st.info("No changes detected.")

                if st.button(f"Save Changes for {tab_name}"):
                    save_csv_s3(edited_df, file_key)
                    st.session_state[f"df_{tab_name}"] = edited_df.copy()
                    st.success(f"Data for {tab_name} saved successfully.")
