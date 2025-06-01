import os

def get_db2_connection_string():
    """
    Build DB2 connection string from environment variables
    """
    # Get connection parameters from environment variables
    hostname = os.getenv('DB2_HOSTNAME', 'localhost')
    port = os.getenv('DB2_PORT', '50000')
    database = os.getenv('DB2_DATABASE', 'SAMPLE')
    username = os.getenv('DB2_USERNAME')
    password = os.getenv('DB2_PASSWORD')
    
    # Additional optional parameters
    security = os.getenv('DB2_SECURITY', 'SSL')  # SSL or None
    sslservercertificate = os.getenv('DB2_SSL_CERT_PATH')
    
    # Build connection string
    conn_str = f"DATABASE={database};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP;UID={username};PWD={password};"
    
    # Add SSL configuration if specified
    if security == 'SSL':
        conn_str += "SECURITY=SSL;"
        if sslservercertificate:
            conn_str += f"SSLServerCertificate={sslservercertificate};"
    
    return conn_str

def get_db2_connection_dict():
    """
    Alternative: Return connection parameters as dictionary
    """
    return {
        'database': os.getenv('DB2_DATABASE', 'SAMPLE'),
        'hostname': os.getenv('DB2_HOSTNAME', 'localhost'),
        'port': int(os.getenv('DB2_PORT', '50000')),
        'protocol': 'TCPIP',
        'uid': os.getenv('DB2_USERNAME'),
        'pwd': os.getenv('DB2_PASSWORD'),
        'security': os.getenv('DB2_SECURITY', 'SSL')
    }
