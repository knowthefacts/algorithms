import json
import os
import ibm_db
import ibm_db_dbi
from db2_config import get_db2_connection_string

def lambda_handler(event, context):
    try:
        # Get DB2 connection string from environment variables or config
        conn_str = get_db2_connection_string()
        
        # Establish connection to DB2
        conn = ibm_db.connect(conn_str, "", "")
        
        if conn:
            print("Connected to DB2 successfully")
            
            # Example query - replace with your actual query
            sql = "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1"
            stmt = ibm_db.exec_immediate(conn, sql)
            
            result = ibm_db.fetch_assoc(stmt)
            
            # Close connection
            ibm_db.close(conn)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'DB2 connection successful',
                    'timestamp': str(result['1']) if result else None
                })
            }
        else:
            error_msg = ibm_db.conn_errormsg()
            print(f"Connection failed: {error_msg}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Failed to connect to DB2',
                    'details': error_msg
                })
            }
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Lambda execution failed',
                'details': str(e)
            })
        }
