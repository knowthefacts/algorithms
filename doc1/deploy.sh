#!/bin/bash

# Configuration
AWS_REGION="us-east-1"
ECR_REPO_NAME="lambda-db2"
LAMBDA_FUNCTION_NAME="db2-lambda-function"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Build and tag the Docker image
echo "Building Docker image..."
docker build -t ${ECR_REPO_NAME}:latest .

# Create ECR repository if it doesn't exist
echo "Creating ECR repository..."
aws ecr create-repository --repository-name ${ECR_REPO_NAME} --region ${AWS_REGION} 2>/dev/null || true

# Get login token and login to ECR
echo "Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Tag image for ECR
docker tag ${ECR_REPO_NAME}:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest

# Push image to ECR
echo "Pushing image to ECR..."
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest

# Update or create Lambda function
echo "Updating Lambda function..."
aws lambda update-function-code \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --image-uri ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest \
    --region ${AWS_REGION} 2>/dev/null || \
aws lambda create-function \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --package-type Image \
    --code ImageUri=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest \
    --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/lambda-execution-role \
    --timeout 30 \
    --memory-size 512 \
    --environment Variables='{
        "DB2_HOSTNAME":"your-db2-hostname",
        "DB2_PORT":"50000",
        "DB2_DATABASE":"your-database-name",
        "DB2_USERNAME":"your-username",
        "DB2_PASSWORD":"your-password",
        "DB2_SECURITY":"SSL"
    }' \
    --region ${AWS_REGION}

echo "Deployment completed!"
