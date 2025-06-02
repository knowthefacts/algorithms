# Use Ubuntu 22.04 as base
FROM ubuntu:22.04

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Install system dependencies required for DB2 client and Lambda runtime
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3.11-distutils \
    python3-pip \
    build-essential \
    gcc \
    g++ \
    make \
    unixodbc \
    unixodbc-dev \
    tar \
    gzip \
    wget \
    curl \
    libaio1 \
    libpam0g \
    ksh \
    numactl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install AWS Lambda Runtime Interface Client
RUN pip3 install awslambdaric

# Set Python 3.11 as default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1

# Create directory for DB2 client
RUN mkdir -p /opt/ibm

# Download and install IBM DB2 client (you'll need to provide the client files)
# Option 1: If you have the client locally, copy it
# COPY v11.5.8_linuxx64_client.tar.gz /tmp/
# Option 2: Download from IBM (requires entitlement)
# RUN wget <your-db2-client-url> -O /tmp/v11.5.8_linuxx64_client.tar.gz

# Extract and install DB2 client
RUN cd /tmp && \
    tar -xzf v11.5.8_linuxx64_client.tar.gz && \
    cd client && \
    ./db2_install -b /opt/ibm/db2/V11.5 -p CLIENT -n

# Set DB2 environment variables
ENV DB2_HOME=/opt/ibm/db2/V11.5
ENV PATH=$DB2_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$DB2_HOME/lib64:$DB2_HOME/lib32:$LD_LIBRARY_PATH

# Set up Lambda runtime environment
ENV LAMBDA_TASK_ROOT=/var/task
ENV LAMBDA_RUNTIME_DIR=/var/runtime

# Create Lambda directories
RUN mkdir -p ${LAMBDA_TASK_ROOT} ${LAMBDA_RUNTIME_DIR}

# Install Python dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip3 install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/
COPY db2_config.py ${LAMBDA_TASK_ROOT}/

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Set the entrypoint for Lambda
ENTRYPOINT ["/usr/local/bin/python", "-m", "awslambdaric"]
CMD ["lambda_function.lambda_handler"]
