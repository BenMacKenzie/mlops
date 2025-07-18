#!/usr/bin/env python3
"""
Test script to list all jobs in the Databricks workspace.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

def test_jobs_functionality():
    """Test the jobs functionality by listing all jobs in the workspace."""
    print("Testing Databricks Jobs Functionality")
    print("=" * 40)
    
    try:
        # Import Databricks SDK
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import Job, Run
        
        # Initialize workspace client
        print("1. Initializing Databricks Workspace Client...")
        wc = WorkspaceClient()
        print("   ✅ Workspace client initialized successfully")
        
        # List all jobs
        print("\n2. Fetching all jobs...")
        jobs = list(wc.jobs.list())
        
        if not jobs:
            print("   No jobs found in the workspace.")
            return
        
        print(f"   Found {len(jobs)} jobs in the workspace")
        
        # Convert jobs to DataFrame for better display
        jobs_data = []
        for job in jobs:
            job_info = {
                'job_id': job.job_id,
                'job_name': job.settings.name if job.settings and job.settings.name else 'Unnamed Job',
                'created_time': datetime.fromtimestamp(job.created_time / 1000) if job.created_time else None,
                'creator_user_name': job.creator_user_name,
                'run_as_user_name': job.settings.run_as.user_name if job.settings and job.settings.run_as else None,
                'max_concurrent_runs': job.settings.max_concurrent_runs if job.settings else None,
                'timeout_seconds': job.settings.timeout_seconds if job.settings else None,
                'schedule': job.settings.schedule.quartz_cron_expression if job.settings and job.settings.schedule else None,
                'email_notifications': bool(job.settings.email_notifications) if job.settings else False,
                'webhook_notifications': bool(job.settings.webhook_notifications) if job.settings else False,
                'continuous': job.settings.continuous.enabled if job.settings and job.settings.continuous else False,
                'tags': job.settings.tags if job.settings else None
            }
            jobs_data.append(job_info)
        
        # Create DataFrame
        jobs_df = pd.DataFrame(jobs_data)
        
        # Display job information
        print(f"\n3. Job Summary:")
        print(f"   Total Jobs: {len(jobs_df)}")
        
        # Show job details
        print(f"\n4. Job Details:")
        for idx, job in jobs_df.iterrows():
            print(f"   Job {idx + 1}:")
            print(f"     ID: {job['job_id']}")
            print(f"     Name: {job['job_name']}")
            print(f"     Creator: {job['creator_user_name']}")
            print(f"     Created: {job['created_time']}")
            print(f"     Schedule: {job['schedule'] or 'Manual'}")
            print(f"     Max Concurrent Runs: {job['max_concurrent_runs']}")
            print(f"     Continuous: {'Yes' if job['continuous'] else 'No'}")
            print(f"     Email Notifications: {'Yes' if job['email_notifications'] else 'No'}")
            print(f"     Webhook Notifications: {'Yes' if job['webhook_notifications'] else 'No'}")
            if job['tags']:
                print(f"     Tags: {job['tags']}")
            print()
        
        # Test getting recent runs for each job
        print("5. Recent Job Runs:")
        for idx, job in jobs_df.iterrows():
            try:
                print(f"   Job '{job['job_name']}' (ID: {job['job_id']}):")
                
                # Get recent runs for this job
                runs = list(wc.jobs.list_runs(job_id=job['job_id'], limit=5))
                
                if runs:
                    print(f"     Recent runs ({len(runs)}):")
                    for run in runs:
                        run_state = run.state.life_cycle_state if run.state else 'UNKNOWN'
                        run_result = run.state.result_state if run.state else 'UNKNOWN'
                        start_time = datetime.fromtimestamp(run.start_time / 1000) if run.start_time else None
                        end_time = datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None
                        
                        print(f"       Run {run.run_id}: {run_state} -> {run_result}")
                        print(f"         Started: {start_time}")
                        if end_time:
                            print(f"         Ended: {end_time}")
                        print()
                else:
                    print("     No recent runs found")
                    print()
                    
            except Exception as e:
                print(f"     Error getting runs for job {job['job_id']}: {str(e)}")
                print()
        
        # Test job statistics
        print("6. Job Statistics:")
        print(f"   Jobs with schedules: {len(jobs_df[jobs_df['schedule'].notna()])}")
        print(f"   Continuous jobs: {len(jobs_df[jobs_df['continuous'] == True])}")
        print(f"   Jobs with email notifications: {len(jobs_df[jobs_df['email_notifications'] == True])}")
        print(f"   Jobs with webhook notifications: {len(jobs_df[jobs_df['webhook_notifications'] == True])}")
        
        # Show DataFrame columns for reference
        print(f"\n7. Available Job Information:")
        print(f"   Columns: {list(jobs_df.columns)}")
        
        return jobs_df
        
    except ImportError as e:
        print(f"❌ Error: Could not import Databricks SDK: {str(e)}")
        print("   Make sure 'databricks-sdk' is installed: pip install databricks-sdk")
        return None
        
    except Exception as e:
        print(f"❌ Error accessing Databricks jobs: {str(e)}")
        print("   Make sure you have proper authentication configured.")
        return None

def get_job_details(job_id: int) -> Optional[Dict]:
    """Get detailed information about a specific job."""
    try:
        from databricks.sdk import WorkspaceClient
        
        wc = WorkspaceClient()
        job = wc.jobs.get(job_id=job_id)
        
        return {
            'job_id': job.job_id,
            'job_name': job.settings.name if job.settings else None,
            'created_time': datetime.fromtimestamp(job.created_time / 1000) if job.created_time else None,
            'creator_user_name': job.creator_user_name,
            'settings': job.settings
        }
        
    except Exception as e:
        print(f"Error getting job details for job {job_id}: {str(e)}")
        return None

def list_job_runs(job_id: int, limit: int = 10) -> List[Dict]:
    """List recent runs for a specific job."""
    try:
        from databricks.sdk import WorkspaceClient
        
        wc = WorkspaceClient()
        runs = list(wc.jobs.list_runs(job_id=job_id, limit=limit))
        
        runs_data = []
        for run in runs:
            run_info = {
                'run_id': run.run_id,
                'run_name': run.run_name,
                'life_cycle_state': run.state.life_cycle_state if run.state else None,
                'result_state': run.state.result_state if run.state else None,
                'start_time': datetime.fromtimestamp(run.start_time / 1000) if run.start_time else None,
                'end_time': datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None,
                'duration': (run.end_time - run.start_time) / 1000 if run.end_time and run.start_time else None
            }
            runs_data.append(run_info)
        
        return runs_data
        
    except Exception as e:
        print(f"Error listing runs for job {job_id}: {str(e)}")
        return []

if __name__ == "__main__":
    # Run the main test
    jobs_df = test_jobs_functionality()
    
    if jobs_df is not None and not jobs_df.empty:
        print("\n" + "=" * 40)
        print("Test completed successfully!")
        print(f"Found {len(jobs_df)} jobs in the workspace.")
        
        # Example: Get details for the first job if available
        if len(jobs_df) > 0:
            first_job_id = jobs_df.iloc[0]['job_id']
            print(f"\nExample: Getting details for job {first_job_id}")
            job_details = get_job_details(first_job_id)
            if job_details:
                print(f"Job name: {job_details['job_name']}")
                print(f"Created by: {job_details['creator_user_name']}")
                
                # Get recent runs for this job
                runs = list_job_runs(first_job_id, limit=3)
                if runs:
                    print(f"Recent runs: {len(runs)}")
                    for run in runs:
                        print(f"  Run {run['run_id']}: {run['life_cycle_state']} -> {run['result_state']}")
    else:
        print("\nTest failed. Check your Databricks configuration and authentication.") 