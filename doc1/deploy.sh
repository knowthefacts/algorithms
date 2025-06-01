#!/bin/bash

# Ubuntu-based Lambda Docker build script
set -e

# Configuration
AWS_REGION="us-east-1"
ECR_REPO_NAME="lambda-db2-ubuntu"
LAMBDA_FUNCTION_NAME="db2-lambda-ubuntu"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "Building Ubuntu-based Lambda Docker image..."

# Build the Docker image
docker build -t ${ECR_REPO_NAME}:latest -f Dockerfile .

# Test the image locally (optional)
echo "Testing image locally..."
docker run --rm -p 9000:8080 ${ECR_REPO_NAME}:latest &
CONTAINER_PID=$!

# Wait a moment for container to start
sleep 5

# Test with curl (uncomment to test)
# curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'

# Stop test container
kill $CONTAINER_PID 2>/dev/null || true

# Create ECR repository
echo "Creating ECR repository..."
aws ecr create-repository \
    --repository-name ${ECR_REPO_NAME} \
    --region ${AWS_REGION} \
    --image-scanning-configuration scanOnPush=true 2>/dev/null || true

# Login to ECR
echo "Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | \
    docker login --username AWS --password-stdin \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Tag and push image
IMAGE_URI=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest
docker tag ${ECR_REPO_NAME}:latest ${IMAGE_URI}

echo "Pushing image to ECR..."
docker push ${IMAGE_URI}

# Create or update Lambda function
echo "Creating/updating Lambda function..."

# Check if function exists
if aws lambda get-function --function-name ${LAMBDA_FUNCTION_NAME} --region ${AWS_REGION} >/dev/null 2>&1; then
    echo "Updating existing function..."
    aws lambda update-function-code \
        --function-name ${LAMBDA_FUNCTION_NAME} \
        --image-uri ${IMAGE_URI} \
        --region ${AWS_REGION}
else
    echo "Creating new function..."
    aws lambda create-function \
        --function-name ${LAMBDA_FUNCTION_NAME} \
        --package-type Image \
        --code ImageUri=${IMAGE_URI} \
        --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/lambda-execution-role \
        --timeout 60 \
        --memory-size 1024 \
        --environment Variables='{
            "DB2_HOSTNAME":"your-db2-hostname",
            "DB2_PORT":"50000", 
            "DB2_DATABASE":"your-database-name",
            "DB2_USERNAME":"your-username",
            "DB2_PASSWORD":"your-password",
            "DB2_SECURITY":"SSL"
        }' \
        --region ${AWS_REGION}
fi

echo "Ubuntu-based Lambda deployment completed!"
echo "Image URI: ${IMAGE_URI}"
echo "Function Name: ${LAMBDA_FUNCTION_NAME}"
