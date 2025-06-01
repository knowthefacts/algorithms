# Use AWS Lambda Python runtime as base
FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies required for DB2 client
RUN yum update -y && \
    yum install -y \
    gcc \
    gcc-c++ \
    make \
    unixODBC \
    unixODBC-devel \
    tar \
    gzip \
    wget \
    libaio \
    pam \
    ksh \
    numactl-libs \
    && yum clean all

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

# Install Python dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY db2_config.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["lambda_function.lambda_handler"]
