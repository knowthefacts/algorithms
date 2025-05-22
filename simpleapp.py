import streamlit as st
import boto3
import pandas as pd
import io

st.title("Simple ECS Streamlit App")

if st.button("Fetch and Display CSV from S3"):
    s3 = boto3.client('s3')
    bucket_name = "your-bucket-name"
    file_key = "path/to/your/file.csv"
    
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error fetching CSV: {e}")

if st.button("Send SNS Email"):
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = "arn:aws:sns:us-east-1:your-account-id:your-topic-name"
    
    try:
        sns.publish(
            TopicArn=topic_arn,
            Subject="Test Message from Streamlit App",
            Message="Hi"
        )
        st.success("SNS email sent successfully!")
    except Exception as e:
        st.error(f"Error sending SNS message: {e}")
