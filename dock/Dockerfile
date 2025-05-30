FROM public.ecr.aws/lambda/python:3.13

# Install system dependencies for DB2
RUN yum update -y && \
    yum install -y gcc gcc-c++ make wget unzip tar && \
    yum clean all

# Download and install IBM DB2 client
RUN mkdir -p /opt/ibm && \
    cd /opt/ibm && \
    wget https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/linuxx64_odbc_cli.tar.gz && \
    tar -xzf linuxx64_odbc_cli.tar.gz && \
    rm linuxx64_odbc_cli.tar.gz

# Set DB2 environment variables
ENV IBM_DB_HOME=/opt/ibm/clidriver
ENV LD_LIBRARY_PATH=$IBM_DB_HOME/lib:$LD_LIBRARY_PATH
ENV PATH=$IBM_DB_HOME/bin:$PATH

# Copy requirements and install Python packages
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["lambda_function.lambda_handler"]
