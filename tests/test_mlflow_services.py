#!/usr/bin/env python3
"""
Test script to compare MLflow client service vs REST API service.
"""

import pandas as pd
from mlflow_service import mlflow_service
from mlflow_rest_service import mlflow_rest_service
import time

def test_mlflow_client():
    """Test the MLflow client service."""
    print("=== Testing MLflow Client Service ===")
    try:
        start_time = time.time()
        runs = mlflow_service.get_runs()
        end_time = time.time()
        
        print(f"✅ Success! Retrieved {len(runs)} runs in {end_time - start_time:.2f} seconds")
        print(f"Columns: {list(runs.columns)}")
        
        if not runs.empty:
            print(f"Sample run: {runs.iloc[0]['run_name']}")
        
        return True
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        return False

def test_mlflow_rest():
    """Test the MLflow REST API service."""
    print("\n=== Testing MLflow REST API Service ===")
    try:
        # First test connection
        if not mlflow_rest_service.test_connection():
            print("❌ Connection test failed")
            return False
        
        start_time = time.time()
        runs = mlflow_rest_service.get_runs()
        end_time = time.time()
        
        print(f"✅ Success! Retrieved {len(runs)} runs in {end_time - start_time:.2f} seconds")
        print(f"Columns: {list(runs.columns)}")
        
        if not runs.empty:
            print(f"Sample run: {runs.iloc[0]['run_name']}")
        
        return True
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        return False

def compare_services():
    """Compare both services side by side."""
    print("Comparing MLflow Client vs REST API Services")
    print("=" * 50)
    
    client_success = test_mlflow_client()
    rest_success = test_mlflow_rest()
    
    print("\n" + "=" * 50)
    print("SUMMARY:")
    print(f"MLflow Client: {'✅ Working' if client_success else '❌ Failed'}")
    print(f"REST API: {'✅ Working' if rest_success else '❌ Failed'}")
    
    if client_success and rest_success:
        print("\nBoth services are working! You can choose either one.")
    elif client_success:
        print("\nUse the MLflow Client service (mlflow_service.py)")
    elif rest_success:
        print("\nUse the REST API service (mlflow_rest_service.py)")
    else:
        print("\nBoth services failed. Check your Databricks configuration.")

if __name__ == "__main__":
    compare_services() 