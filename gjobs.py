"""
AWS Glue Job Cost Calculator with Parallel Processing

This module calculates AWS Glue job costs for a specific time period using parallel processing
for improved performance when analyzing multiple jobs.

Features:
- Parallel processing using ThreadPoolExecutor for concurrent API calls
- Configurable number of worker threads
- Progress tracking and error handling
- Performance statistics and reporting
- Support for both parallel and sequential processing modes

Usage:
    calculator = GlueCostCalculator(
        region_name='us-east-1',
        max_workers=16,           # Number of parallel workers
        enable_parallel=True      # Enable parallel processing
    )
    
    results = calculator.calculate_costs_from_csv('glue_jobs.csv')
"""

import boto3
import pandas as pd
from datetime import datetime, timezone
from decimal import Decimal
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from functools import partial

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GlueCostCalculator:
    def __init__(self, region_name='us-east-1', max_workers=None, enable_parallel=True):
        """Initialize the Glue cost calculator
        
        Args:
            region_name: AWS region name
            max_workers: Maximum number of worker threads for parallel processing. 
                        If None, defaults to min(32, (number of CPUs + 4))
            enable_parallel: Whether to enable parallel processing
        """
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.enable_parallel = enable_parallel
        
        # Set max workers for parallel processing
        if max_workers is None:
            self.max_workers = min(32, (multiprocessing.cpu_count() + 4))
        else:
            self.max_workers = max_workers
        
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
    
    @staticmethod
    def _calculate_job_cost_worker(job_name, region_name, start_date, end_date, pricing):
        """Static worker function for parallel processing of job cost calculation
        
        This function creates its own boto3 client to avoid thread safety issues
        """
        try:
            # Create a separate client for this worker
            glue_client = boto3.client('glue', region_name=region_name)
            
            # Create a temporary calculator instance for this worker
            temp_calc = GlueCostCalculator.__new__(GlueCostCalculator)
            temp_calc.glue_client = glue_client
            temp_calc.pricing = pricing
            temp_calc.start_date = start_date
            temp_calc.end_date = end_date
            
            # Use the existing methods through the temporary instance
            job_details = temp_calc.get_job_details(job_name)
            job_runs = temp_calc.get_job_runs_for_august_2025(job_name)
            
            total_cost = Decimal('0')
            successful_runs = 0
            failed_runs = 0
            
            for job_run in job_runs:
                if job_run.get('JobRunState') == 'SUCCEEDED':
                    run_cost = temp_calc.calculate_job_run_cost(job_run, job_details)
                    total_cost += run_cost
                    successful_runs += 1
                else:
                    failed_runs += 1
            
            return {
                'job_name': job_name,
                'glue_version': job_details['glue_version'],
                'total_runs': len(job_runs),
                'successful_runs': successful_runs,
                'failed_runs': failed_runs,
                'total_cost_usd': float(total_cost),
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error calculating cost for {job_name} in worker: {str(e)}")
            return {
                'job_name': job_name,
                'glue_version': 'Unknown',
                'total_runs': 0,
                'successful_runs': 0,
                'failed_runs': 0,
                'total_cost_usd': 0.0,
                'status': 'error',
                'error_message': str(e)
            }
    
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
    
    def calculate_costs_parallel(self, job_names):
        """Calculate costs for multiple jobs using parallel processing"""
        if not self.enable_parallel or len(job_names) == 1:
            # Fall back to sequential processing
            results = []
            for job_name in job_names:
                result = self.calculate_job_total_cost(job_name)
                results.append(result)
            return results
        
        logger.info(f"Starting parallel processing of {len(job_names)} jobs with {self.max_workers} workers")
        
        # Prepare arguments for worker function
        worker_func = partial(
            self._calculate_job_cost_worker,
            region_name=self.glue_client.meta.region_name,
            start_date=self.start_date,
            end_date=self.end_date,
            pricing=self.pricing
        )
        
        results = []
        completed_jobs = 0
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all jobs to the executor
                future_to_job = {
                    executor.submit(worker_func, job_name): job_name 
                    for job_name in job_names
                }
                
                # Process completed futures as they finish
                for future in as_completed(future_to_job):
                    job_name = future_to_job[future]
                    completed_jobs += 1
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        # Log progress
                        if result['status'] == 'success':
                            logger.info(f"✓ Completed {completed_jobs}/{len(job_names)}: {job_name} - Cost: ${result['total_cost_usd']:.2f}")
                        else:
                            logger.warning(f"✗ Error in {completed_jobs}/{len(job_names)}: {job_name} - {result.get('error_message', 'Unknown error')}")
                            
                    except Exception as e:
                        logger.error(f"Future failed for {job_name}: {str(e)}")
                        results.append({
                            'job_name': job_name,
                            'glue_version': 'Unknown',
                            'total_runs': 0,
                            'successful_runs': 0,
                            'failed_runs': 0,
                            'total_cost_usd': 0.0,
                            'status': 'error',
                            'error_message': str(e)
                        })
                        
        except Exception as e:
            logger.error(f"Error in parallel processing: {str(e)}")
            raise
        
        logger.info(f"Parallel processing completed. Processed {len(results)} jobs.")
        return results
    
    def get_performance_stats(self, results):
        """Get performance statistics from the results"""
        if not results:
            return {}
            
        successful = [r for r in results if r.get('status') == 'success']
        failed = [r for r in results if r.get('status') == 'error']
        
        total_cost = sum(r['total_cost_usd'] for r in successful)
        total_runs = sum(r['total_runs'] for r in successful)
        
        return {
            'total_jobs_processed': len(results),
            'successful_jobs': len(successful),
            'failed_jobs': len(failed),
            'success_rate': len(successful) / len(results) * 100 if results else 0,
            'total_cost_usd': total_cost,
            'total_job_runs': total_runs,
            'average_cost_per_job': total_cost / len(successful) if successful else 0
        }
    
    def calculate_costs_from_csv(self, csv_file_path, job_name_column='job_name', output_csv=None):
        """Main function to calculate costs for all jobs in CSV"""
        try:
            # Read job names from CSV
            job_names = self.read_job_names_from_csv(csv_file_path, job_name_column)
            
            # Calculate costs using parallel processing
            if self.enable_parallel:
                logger.info(f"Using parallel processing with {self.max_workers} workers")
                results = self.calculate_costs_parallel(job_names)
            else:
                logger.info("Using sequential processing")
                results = []
                for i, job_name in enumerate(job_names, 1):
                    logger.info(f"Processing job {i}/{len(job_names)}: {job_name}")
                    result = self.calculate_job_total_cost(job_name)
                    results.append(result)
            
            # Create results DataFrame
            results_df = pd.DataFrame(results)
            
            # Sort by total cost descending
            results_df = results_df.sort_values('total_cost_usd', ascending=False)
            
            # Get performance statistics
            stats = self.get_performance_stats(results)
            
            # Log summary statistics
            logger.info(f"=== Processing Summary ===")
            logger.info(f"Total jobs processed: {stats.get('total_jobs_processed', 0)}")
            logger.info(f"Successful jobs: {stats.get('successful_jobs', 0)}")
            logger.info(f"Failed jobs: {stats.get('failed_jobs', 0)}")
            logger.info(f"Success rate: {stats.get('success_rate', 0):.1f}%")
            logger.info(f"Total cost for all jobs in August 2025: ${stats.get('total_cost_usd', 0):.2f}")
            logger.info(f"Total job runs processed: {stats.get('total_job_runs', 0)}")
            logger.info(f"Average cost per job: ${stats.get('average_cost_per_job', 0):.2f}")
            
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
    # Initialize calculator with parallel processing options
    calculator = GlueCostCalculator(
        region_name='us-east-1',  # Change region as needed
        max_workers=16,           # Optional: set number of parallel workers
        enable_parallel=True      # Optional: enable/disable parallel processing
    )
    
    # Calculate costs from CSV
    csv_file_path = 'glue_jobs.csv'  # Path to your CSV file
    job_name_column = 'job_name'     # Column name containing job names
    
    try:
        results_df = calculator.calculate_costs_from_csv(
            csv_file_path=csv_file_path,
            job_name_column=job_name_column,
            output_csv='glue_costs_august_2025.csv'  # Optional: save results to CSV
        )
        
        print("\n=== AWS Glue Job Costs for August 2025 ===")
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
