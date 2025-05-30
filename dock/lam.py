import json
import boto3
import ibm_db
import yaml
import pandas as pd
import logging
from io import StringIO, BytesIO
from datetime import datetime
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DB2DataProcessor:
    def __init__(self):
        self.secrets_client = boto3.client('secretsmanager')
        self.s3_client = boto3.client('s3')
        self.db_connection = None
        
    def get_db_credentials(self, secret_name):
        """Retrieve DB2 credentials from AWS Secrets Manager"""
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            credentials = json.loads(response['SecretString'])
            return credentials
        except Exception as e:
            logger.error(f"Error retrieving credentials: {str(e)}")
            raise
    
    def get_config_from_s3(self, bucket_name, config_key):
        """Download and parse YAML config from S3"""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=config_key)
            config_content = response['Body'].read().decode('utf-8')
            config = yaml.safe_load(config_content)
            return config
        except Exception as e:
            logger.error(f"Error retrieving config from S3: {str(e)}")
            raise
    
    def connect_to_db2(self, credentials):
        """Establish connection to DB2"""
        try:
            connection_string = (
                f"DATABASE={credentials['database']};"
                f"HOSTNAME={credentials['hostname']};"
                f"PORT={credentials['port']};"
                f"PROTOCOL=TCPIP;"
                f"UID={credentials['username']};"
                f"PWD={credentials['password']};"
            )
            
            self.db_connection = ibm_db.connect(connection_string, "", "")
            logger.info("Successfully connected to DB2")
            return True
        except Exception as e:
            logger.error(f"Error connecting to DB2: {str(e)}")
            raise
    
    def execute_query(self, query):
        """Execute query and return results as DataFrame"""
        try:
            if not self.db_connection:
                raise Exception("No database connection available")
            
            stmt = ibm_db.exec_immediate(self.db_connection, query)
            
            # Fetch column metadata
            columns = []
            num_columns = ibm_db.num_fields(stmt)
            for i in range(num_columns):
                columns.append(ibm_db.field_name(stmt, i))
            
            # Fetch all rows
            rows = []
            while ibm_db.fetch_row(stmt):
                row = []
                for i in range(num_columns):
                    row.append(ibm_db.result(stmt, i))
                rows.append(row)
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Query executed successfully. Retrieved {len(df)} rows.")
            return df
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def upload_to_s3(self, dataframe, bucket_name, s3_key, file_format='parquet'):
        """Upload DataFrame to S3 in specified format"""
        try:
            if file_format.lower() == 'parquet':
                buffer = BytesIO()
                dataframe.to_parquet(buffer, index=False)
                buffer.seek(0)
                content_type = 'application/octet-stream'
            elif file_format.lower() == 'csv':
                buffer = StringIO()
                dataframe.to_csv(buffer, index=False)
                buffer = BytesIO(buffer.getvalue().encode('utf-8'))
                content_type = 'text/csv'
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType=content_type
            )
            
            logger.info(f"Data uploaded to s3://{bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            raise
    
    def process_tables(self, config):
        """Process all tables defined in config"""
        results = []
        
        for table_config in config.get('tables', []):
            table_name = table_config.get('name')
            query = table_config.get('query')
            output_config = table_config.get('output', {})
            
            logger.info(f"Processing table: {table_name}")
            
            try:
                # Execute query
                df = self.execute_query(query)
                
                # Generate output key with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                s3_key = f"{output_config.get('prefix', 'data')}/{table_name}_{timestamp}.{output_config.get('format', 'parquet')}"
                
                # Upload to S3
                self.upload_to_s3(
                    df, 
                    output_config.get('bucket'), 
                    s3_key, 
                    output_config.get('format', 'parquet')
                )
                
                results.append({
                    'table': table_name,
                    'rows_processed': len(df),
                    's3_location': f"s3://{output_config.get('bucket')}/{s3_key}",
                    'status': 'success'
                })
                
            except Exception as e:
                logger.error(f"Error processing table {table_name}: {str(e)}")
                results.append({
                    'table': table_name,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return results
    
    def close_connection(self):
        """Close DB2 connection"""
        if self.db_connection:
            ibm_db.close(self.db_connection)
            logger.info("DB2 connection closed")

def lambda_handler(event, context):
    """Main Lambda handler"""
    processor = DB2DataProcessor()
    
    try:
        # Extract parameters from event
        secret_name = event.get('secret_name', 'db2-credentials')
        config_bucket = event.get('config_bucket')
        config_key = event.get('config_key', 'config/table_config.yaml')
        
        if not config_bucket:
            raise ValueError("config_bucket parameter is required")
        
        logger.info(f"Starting processing with secret: {secret_name}, config: s3://{config_bucket}/{config_key}")
        
        # Get DB2 credentials
        credentials = processor.get_db_credentials(secret_name)
        
        # Get table configuration
        config = processor.get_config_from_s3(config_bucket, config_key)
        
        # Connect to DB2
        processor.connect_to_db2(credentials)
        
        # Process tables
        results = processor.process_tables(config)
        
        # Close connection
        processor.close_connection()
        
        # Return results
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'results': results,
                'processed_tables': len(results),
                'successful_tables': len([r for r in results if r['status'] == 'success'])
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        
        # Ensure connection is closed
        processor.close_connection()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Processing failed',
                'error': str(e)
            })
        }
