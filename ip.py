import boto3
import pandas as pd

# Input: Region and Subnet IDs
region = 'us-east-1'  # Change this to your region
subnet_ids = ['subnet-xxxxxxx1', 'subnet-xxxxxxx2']  # Add all your subnet IDs

# Create EC2 client
ec2 = boto3.client('ec2', region_name=region)

# Collect results
results = []

for subnet_id in subnet_ids:
    response = ec2.describe_network_interfaces(
        Filters=[{"Name": "subnet-id", "Values": [subnet_id]}]
    )

    for eni in response['NetworkInterfaces']:
        data = {
            'Subnet ID': subnet_id,
            'ENI ID': eni.get('NetworkInterfaceId'),
            'Description': eni.get('Description'),
            'Status': eni.get('Status'),
            'Private IP': eni.get('PrivateIpAddress'),
            'Private IPs (all)': ", ".join([ip['PrivateIpAddress'] for ip in eni.get('PrivateIpAddresses', [])]),
            'Public IP': eni.get('Association', {}).get('PublicIp', 'N/A'),
            'MAC Address': eni.get('MacAddress'),
            'VPC ID': eni.get('VpcId'),
            'Availability Zone': eni.get('AvailabilityZone'),
            'Interface Type': eni.get('InterfaceType'),
            'Owner ID': eni.get('OwnerId'),
            'Attachment Instance ID': eni.get('Attachment', {}).get('InstanceId', 'N/A'),
            'Attachment Status': eni.get('Attachment', {}).get('Status', 'N/A'),
            'Device Index': eni.get('Attachment', {}).get('DeviceIndex', 'N/A'),
            'Delete on Termination': eni.get('Attachment', {}).get('DeleteOnTermination', 'N/A'),
            'Security Group IDs': ", ".join([g['GroupId'] for g in eni.get('Groups', [])]),
            'Security Group Names': ", ".join([g['GroupName'] for g in eni.get('Groups', [])]),
            'Tags': ", ".join([f"{tag['Key']}={tag['Value']}" for tag in eni.get('TagSet', [])])
        }

        results.append(data)

# Convert to DataFrame and save as Excel
df = pd.DataFrame(results)
df.to_excel('network_interfaces_full.xlsx', index=False)

print("✅ Excel file 'network_interfaces_full.xlsx' created successfully.")
