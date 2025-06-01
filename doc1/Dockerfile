# Alternative lighter approach using Ubuntu 22.04
FROM ubuntu:22.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC
ENV PYTHONUNBUFFERED=1

# Install minimal system dependencies
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    build-essential \
    wget \
    curl \
    unixodbc \
    unixodbc-dev \
    libaio1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create symlinks for python
RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python

# Install AWS Lambda Runtime Interface Client
RUN pip3 install --no-cache-dir awslambdaric

# Set up Lambda environment
ENV LAMBDA_TASK_ROOT=/var/task
ENV LAMBDA_RUNTIME_DIR=/var/runtime
RUN mkdir -p ${LAMBDA_TASK_ROOT} ${LAMBDA_RUNTIME_DIR}

# Option 1: Install DB2 client from IBM package
# Copy your DB2 client installer
# COPY ibm_data_server_driver_package_linuxx64_v11.5.tar.gz /tmp/
# RUN cd /tmp && \
#     tar -xzf ibm_data_server_driver_package_linuxx64_v11.5.tar.gz && \
#     cd dsdriver && \
#     ./installDSDriver

# Option 2: Use pre-built IBM DB2 client libraries (if available)
# COPY db2_libs/ /opt/ibm/db2/

# Set DB2 environment (adjust paths based on your installation)
ENV IBM_DB_HOME=/opt/ibm/db2
ENV LD_LIBRARY_PATH=/opt/ibm/db2/lib:$LD_LIBRARY_PATH
ENV PATH=/opt/ibm/db2/bin:$PATH

# Install Python dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip3 install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy application code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/
COPY db2_config.py ${LAMBDA_TASK_ROOT}/

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Configure Lambda runtime
ENTRYPOINT ["/usr/bin/python3", "-m", "awslambdaric"]
CMD ["lambda_function.lambda_handler"]
