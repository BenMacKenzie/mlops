#!/usr/bin/env python3
"""
Test script for the new MLflowWorkspaceService that uses the Databricks workspace client.
This script demonstrates the usage of the new service and compares it with the original.
"""

import pandas as pd
from mlflow_service import mlflow_service
from mlflow_service import MLflowWorkspaceService

# Initialize workspace service if global instance is not available
try:
    from mlflow_service import mlflow_workspace_service
    if mlflow_workspace_service is None:
        mlflow_workspace_service = MLflowWorkspaceService()
except ImportError:
    mlflow_workspace_service = MLflowWorkspaceService()

def test_experiment_id():
    """Test getting experiment ID with both services."""
    print("Testing get_experiment_id...")
    
    experiment_name = '/ML/mlflow_workshop/mlflow3-ml-example'
    
    # Test with original service
    try:
        original_id = mlflow_service.get_experiment_id(experiment_name)
        print(f"Original service - Experiment ID: {original_id}")
    except Exception as e:
        print(f"Original service error: {e}")
    
    # Test with workspace service
    try:
        workspace_id = mlflow_workspace_service.get_experiment_id(experiment_name)
        print(f"Workspace service - Experiment ID: {workspace_id}")
    except Exception as e:
        print(f"Workspace service error: {e}")

def test_get_runs():
    """Test getting runs with both services."""
    print("\nTesting get_runs...")
    
    experiment_name = '/ML/mlflow_workshop/mlflow3-ml-example'
    
    # Test with original service
    try:
        original_runs = mlflow_service.get_runs(experiment_name)
        print(f"Original service - Runs count: {len(original_runs)}")
        if not original_runs.empty:
            print(f"Original service - Sample run: {original_runs.iloc[0]['run_name']}")
    except Exception as e:
        print(f"Original service error: {e}")
    
    # Test with workspace service
    try:
        workspace_runs = mlflow_workspace_service.get_runs(experiment_name)
        print(f"Workspace service - Runs count: {len(workspace_runs)}")
        if not workspace_runs.empty:
            print(f"Workspace service - Sample run: {workspace_runs.iloc[0]['run_name']}")
    except Exception as e:
        print(f"Workspace service error: {e}")

def test_experiment_summary():
    """Test getting experiment summary with both services."""
    print("\nTesting get_experiment_summary...")
    
    experiment_name = '/ML/mlflow_workshop/mlflow3-ml-example'
    
    # Test with original service
    try:
        original_summary = mlflow_service.get_experiment_summary(experiment_name)
        print(f"Original service - Summary: {original_summary}")
    except Exception as e:
        print(f"Original service error: {e}")
    
    # Test with workspace service
    try:
        workspace_summary = mlflow_workspace_service.get_experiment_summary(experiment_name)
        print(f"Workspace service - Summary: {workspace_summary}")
    except Exception as e:
        print(f"Workspace service error: {e}")

def test_logged_models():
    """Test getting logged models with both services."""
    print("\nTesting get_logged_models...")
    
    # Test with original service
    try:
        original_models = mlflow_service.get_logged_models()
        print(f"Original service - Models count: {len(original_models)}")
        if not original_models.empty:
            print(f"Original service - Sample model: {original_models.iloc[0]['model_name']}")
    except Exception as e:
        print(f"Original service error: {e}")
    
    # Test with workspace service
    try:
        workspace_models = mlflow_workspace_service.get_logged_models()
        print(f"Workspace service - Models count: {len(workspace_models)}")
        if not workspace_models.empty:
            print(f"Workspace service - Sample model: {workspace_models.iloc[0]['model_name']}")
    except Exception as e:
        print(f"Workspace service error: {e}")

def compare_dataframes(df1, df2, name1, name2):
    """Compare two dataframes and show differences."""
    print(f"\nComparing {name1} vs {name2}:")
    print(f"{name1} shape: {df1.shape}")
    print(f"{name2} shape: {df2.shape}")
    
    if not df1.empty and not df2.empty:
        # Compare columns
        common_cols = set(df1.columns) & set(df2.columns)
        print(f"Common columns: {len(common_cols)}")
        
        # Compare data types
        print(f"{name1} dtypes: {df1.dtypes.to_dict()}")
        print(f"{name2} dtypes: {df2.dtypes.to_dict()}")

def check_workspace_service():
    """Check if workspace service is properly configured."""
    print("Checking workspace service configuration...")
    try:
        # Try to access the workspace client
        client = mlflow_workspace_service.workspace_client
        print("✓ Workspace service is properly configured")
        return True
    except Exception as e:
        print(f"✗ Workspace service configuration error: {e}")
        print("Please configure Databricks credentials before using the workspace service.")
        return False

def main():
    """Main test function."""
    print("=== MLflow Service Comparison Test ===\n")
    
    # Check workspace service configuration first
    workspace_configured = check_workspace_service()
    
    if not workspace_configured:
        print("\nSkipping workspace service tests due to configuration issues.")
        print("Please configure Databricks credentials and try again.")
        return
    
    # Test basic functionality
    test_experiment_id()
    test_get_runs()
    test_experiment_summary()
    test_logged_models()
    
    # Compare results for the same experiment
    print("\n=== Detailed Comparison ===")
    experiment_name = '/ML/mlflow_workshop/mlflow3-ml-example'
    
    try:
        original_runs = mlflow_service.get_runs(experiment_name)
        workspace_runs = mlflow_workspace_service.get_runs(experiment_name)
        
        compare_dataframes(original_runs, workspace_runs, "Original", "Workspace")
        
        if not original_runs.empty and not workspace_runs.empty:
            print(f"\nFirst run comparison:")
            print(f"Original: {original_runs.iloc[0].to_dict()}")
            print(f"Workspace: {workspace_runs.iloc[0].to_dict()}")
    
    except Exception as e:
        print(f"Comparison error: {e}")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main() 