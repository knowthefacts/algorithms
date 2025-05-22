import streamlit as st
import boto3
import pandas as pd
import io

st.title("ECS Streamlit App with Dynamic Inputs")

# S3 inputs
st.subheader("Fetch CSV from S3")
bucket_name = st.text_input("S3 Bucket Name")
file_key = st.text_input("CSV File Path in S3")

if st.button("Fetch and Display CSV"):
    if bucket_name and file_key:
        try:
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error fetching CSV: {e}")
    else:
        st.warning("Please provide both Bucket Name and File Path")

st.divider()

# SNS inputs
st.subheader("Send SNS Email")
sns_topic_arn = st.text_input("SNS Topic ARN")
email_message = st.text_area("Email Message", value="Hi")

if st.button("Send SNS Email"):
    if sns_topic_arn and email_message:
        try:
            sns = boto3.client('sns')
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject="Message from Streamlit App",
                Message=email_message
            )
            st.success("SNS email sent successfully!")
        except Exception as e:
            st.error(f"Error sending SNS message: {e}")
    else:
        st.warning("Please provide SNS Topic ARN and Email Message")
