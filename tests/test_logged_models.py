#!/usr/bin/env python3
"""
Test script to verify logged models functionality in MLflow service.
"""

import pandas as pd
from mlflow_service import MLflowService
from mlflow import MlflowClient
from mlflow.entities.metric import Metric

def test_logged_models_functionality():
    """Test the logged models functionality."""
    print("Testing MLflow Service Logged Models Functionality")
    print("=" * 55)
    
    # Create service instance
    service = MLflowService()
    
    # Debug: Show LoggedModel structure
    print("0. Debug: Examining LoggedModel structure...")
    
    try:
        client = MlflowClient()
        experiment = client.get_experiment_by_name('/ML/mlflow_workshop/mlflow3-ml-example')
        if experiment:
            logged_models = client.search_logged_models([experiment.experiment_id])
            if logged_models:
                print(f"   Found {len(logged_models)} logged models")
                print(f"   Sample metrics from first model:")
                for model in logged_models[:1]:  # Just first model
                    if hasattr(model, 'metrics') and model.metrics:
                        for metric in model.metrics:
                            dataset_name = getattr(metric, 'dataset_name', 'None')
                            print(f"     {metric.key} (dataset: {dataset_name}) = {metric.value}")
            else:
                print("   No logged models found")
        else:
            print("   Default experiment not found")
    except Exception as e:
        print(f"   Error accessing MLflow: {str(e)}")
    
    # Test getting logged models
    print("\n1. Fetching logged models...")
    models = service.get_logged_models()
    
    if models.empty:
        print("   No logged models found. Make sure you have logged models in your MLflow tracking.")
        return
    
    print(f"   Found {len(models)} logged models")
    print(f"   Columns: {list(models.columns)}")
    
    # Show sample column names to verify no unnecessary columns
    print(f"   Sample column names:")
    for i, col in enumerate(models.columns[:10]):  # Show first 10 columns
        print(f"     {i+1}. '{col}'")
    
    # Test dataset metrics summary
    print("\n2. Testing Dataset Metrics Summary...")
    dataset_summary = service.get_dataset_metrics_summary(models)
    
    if dataset_summary:
        print(f"   Found {len(dataset_summary)} datasets:")
        for dataset_name, metrics in dataset_summary.items():
            print(f"     {dataset_name.upper()}: {len(metrics)} metrics")
            for metric in metrics:
                print(f"       - {metric['metric_name']} (full: {metric['full_name']})")
    else:
        print("   No dataset-specific metrics found")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_logged_models_functionality()
    
