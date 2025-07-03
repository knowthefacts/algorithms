import requests
import datetime
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

# Temporary credentials (from STS or assume-role)
access_key = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"
session_token = "YOUR_SESSION_TOKEN"
region = "us-east-1"
service = "sqs"

# Queue URL
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/your-queue-name"

# SQS action parameters
payload = {
    'Action': 'SendMessage',
    'MessageBody': 'Hello World',
    'Version': '2012-11-05'
}

# Build request
req = AWSRequest(method="POST", url=queue_url, data=payload)
creds = Credentials(access_key, secret_key, session_token)
SigV4Auth(creds, service, region).add_auth(req)

# Send request using requests
response = requests.post(
    queue_url,
    headers=dict(req.headers.items()),
    data=payload
)

print(response.status_code)
print(response.text)
