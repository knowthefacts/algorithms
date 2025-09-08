import boto3
import pandas as pd
from datetime import datetime, timezone
from decimal import Decimal
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GlueCostCalculator:
    def __init__(self, region_name='us-east-1'):
        """Initialize the Glue cost calculator"""
        self.glue_client = boto3.client('glue', region_name=region_name)
        
        # AWS Glue pricing per DPU-hour (as of 2025)
        self.pricing = {
            '2.0': Decimal('0.44'),
            '3.0': Decimal('0.44'),
            '4.0': Decimal('0.44'),
            '5.0': Decimal('0.44')
        }
        
        # August 2025 date range
        self.start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        self.end_date = datetime(2025, 8, 31, 23, 59, 59, tzinfo=timezone.utc)
        
    def read_job_names_from_csv(self, csv_file_path, job_name_column='job_name'):
        """Read Glue job names from CSV file"""
        try:
            df = pd.read_csv(csv_file_path)
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            
            if job_name_column not in df.columns:
                available_columns = list(df.columns)
                raise ValueError(f"Column '{job_name_column}' not found. Available columns: {available_columns}")
            
            # Get unique job names and remove any null values
            job_names = df[job_name_column].dropna().unique().tolist()
            logger.info(f"Found {len(job_names)} unique job names in CSV")
            return job_names
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise
    
    def get_job_runs_for_august_2025(self, job_name):
        """Get all job runs for a specific job in August 2025"""
        try:
            job_runs = []
            next_token = None
            
            while True:
                kwargs = {
                    'JobName': job_name,
                    'MaxResults': 200
                }
                
                if next_token:
                    kwargs['NextToken'] = next_token
                
                response = self.glue_client.get_job_runs(**kwargs)
                
                for run in response['JobRuns']:
                    start_time = run.get('StartedOn')
                    if start_time and self.start_date <= start_time <= self.end_date:
                        job_runs.append(run)
                
                next_token = response.get('NextToken')
                if not next_token:
                    break
            
            logger.info(f"Found {len(job_runs)} job runs for {job_name} in August 2025")
            return job_runs
            
        except Exception as e:
            logger.error(f"Error getting job runs for {job_name}: {str(e)}")
            return []
    
    def get_job_details(self, job_name):
        """Get job details including Glue version"""
        try:
            response = self.glue_client.get_job(JobName=job_name)
            job = response['Job']
            
            # Extract Glue version
            glue_version = job.get('GlueVersion', '2.0')
            
            # Get default DPUs if not specified in job runs
            if glue_version in ['2.0']:
                default_allocated_capacity = job.get('AllocatedCapacity', 10)  # Legacy parameter
                default_max_capacity = job.get('MaxCapacity', default_allocated_capacity)
            else:
                default_max_capacity = job.get('MaxCapacity', 10)
            
            return {
                'glue_version': glue_version,
                'default_max_capacity': default_max_capacity
            }
            
        except Exception as e:
            logger.error(f"Error getting job details for {job_name}: {str(e)}")
            return {'glue_version': '2.0', 'default_max_capacity': 10}
    
    def calculate_job_run_cost(self, job_run, job_details):
        """Calculate cost for a single job run"""
        try:
            # Get execution time in seconds
            started_on = job_run.get('StartedOn')
            completed_on = job_run.get('CompletedOn')
            
            if not started_on or not completed_on:
                logger.warning(f"Missing start or completion time for job run {job_run.get('Id', 'Unknown')}")
                return Decimal('0')
            
            execution_time_seconds = (completed_on - started_on).total_seconds()
            execution_time_hours = Decimal(str(execution_time_seconds)) / Decimal('3600')
            
            # Get DPUs used (prefer AllocatedCapacity, then MaxCapacity, then default)
            dpus = (job_run.get('AllocatedCapacity') or 
                   job_run.get('MaxCapacity') or 
                   job_details['default_max_capacity'])
            
            # Get Glue version and corresponding price
            glue_version = job_details['glue_version']
            price_per_dpu_hour = self.pricing.get(glue_version, self.pricing['2.0'])
            
            # Calculate cost
            cost = execution_time_hours * Decimal(str(dpus)) * price_per_dpu_hour
            
            return cost
            
        except Exception as e:
            logger.error(f"Error calculating cost for job run: {str(e)}")
            return Decimal('0')
    
    def calculate_job_total_cost(self, job_name):
        """Calculate total cost for all runs of a job in August 2025"""
        try:
            # Get job details
            job_details = self.get_job_details(job_name)
            
            # Get all job runs for August 2025
            job_runs = self.get_job_runs_for_august_2025(job_name)
            
            total_cost = Decimal('0')
            successful_runs = 0
            
            for job_run in job_runs:
                # Only calculate cost for completed runs
                if job_run.get('JobRunState') == 'SUCCEEDED':
                    run_cost = self.calculate_job_run_cost(job_run, job_details)
                    total_cost += run_cost
                    successful_runs += 1
            
            return {
                'job_name': job_name,
                'glue_version': job_details['glue_version'],
                'total_runs': len(job_runs),
                'successful_runs': successful_runs,
                'total_cost_usd': float(total_cost)
            }
            
        except Exception as e:
            logger.error(f"Error calculating total cost for {job_name}: {str(e)}")
            return {
                'job_name': job_name,
                'glue_version': 'Unknown',
                'total_runs': 0,
                'successful_runs': 0,
                'total_cost_usd': 0.0
            }
    
    def calculate_costs_from_csv(self, csv_file_path, job_name_column='job_name', output_csv=None):
        """Main function to calculate costs for all jobs in CSV"""
        try:
            # Read job names from CSV
            job_names = self.read_job_names_from_csv(csv_file_path, job_name_column)
            
            # Calculate costs for each job
            results = []
            for i, job_name in enumerate(job_names, 1):
                logger.info(f"Processing job {i}/{len(job_names)}: {job_name}")
                result = self.calculate_job_total_cost(job_name)
                results.append(result)
            
            # Create results DataFrame
            results_df = pd.DataFrame(results)
            
            # Sort by total cost descending
            results_df = results_df.sort_values('total_cost_usd', ascending=False)
            
            # Add summary statistics
            total_cost_all_jobs = results_df['total_cost_usd'].sum()
            total_runs_all_jobs = results_df['total_runs'].sum()
            
            logger.info(f"Total cost for all jobs in August 2025: ${total_cost_all_jobs:.2f}")
            logger.info(f"Total job runs processed: {total_runs_all_jobs}")
            
            # Save to CSV if requested
            if output_csv:
                results_df.to_csv(output_csv, index=False)
                logger.info(f"Results saved to {output_csv}")
            
            return results_df
            
        except Exception as e:
            logger.error(f"Error in main calculation: {str(e)}")
            raise

def main():
    """Example usage"""
    # Initialize calculator with parallel processing
    # max_workers: Number of concurrent threads (adjust based on AWS API limits and your needs)
    calculator = GlueCostCalculator(
        region_name='us-east-1',  # Change region as needed
        max_workers=10            # Adjust based on your requirements (5-20 recommended)
    )
    
    # Calculate costs from CSV
    csv_file_path = 'glue_jobs.csv'  # Path to your CSV file
    job_name_column = 'job_name'     # Column name containing job names
    
    try:
        start_time = time.time()
        
        results_df = calculator.calculate_costs_from_csv(
            csv_file_path=csv_file_path,
            job_name_column=job_name_column,
            output_csv='glue_costs_august_2025.csv'  # Optional: save results to CSV
        )
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n=== AWS Glue Job Costs for August 2025 ===")
        print(f"Processing completed in {total_time:.2f} seconds")
        print(results_df.to_string(index=False))
        print(f"\nTotal Cost: ${results_df['costaugusttotal'].sum():.2f}")
        
        # Display sample of output format
        print("\n=== Output CSV Format ===")
        print("gluejob_name,costaugusttotal,number_of_job_runs,total_success_jobs_count,total_unsuccessful_jobs_count,month")
        for _, row in results_df.head(3).iterrows():
            print(f"{row['gluejob_name']},{row['costaugusttotal']},{row['number_of_job_runs']},{row['total_success_jobs_count']},{row['total_unsuccessful_jobs_count']},{row['month']}")
        
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")

if __name__ == "__main__":
    main()
