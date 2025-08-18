#!/usr/bin/env python3
"""
Debug script for MLflow logged models functionality.
This script helps diagnose issues with fetching logged models.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mlflow_service import mlflow_workspace_service
from databricks.sdk import WorkspaceClient
import pandas as pd

def test_basic_connectivity():
    """Test basic MLflow/Databricks connectivity."""
    print("🔌 Testing basic connectivity...")
    print("=" * 50)
    
    try:
        # Test workspace client
        client = WorkspaceClient()
        print("✅ Databricks WorkspaceClient initialized successfully")
        
        # Test MLflow service
        if mlflow_workspace_service is None:
            print("❌ MLflow workspace service is None")
            return False
        
        print("✅ MLflow workspace service available")
        
        # Test listing experiments
        experiments = mlflow_workspace_service.list_experiments()
        print(f"✅ Successfully listed {len(experiments)} experiments")
        
        return True
        
    except Exception as e:
        print(f"❌ Connectivity error: {e}")
        return False

def list_all_experiments_detailed():
    """List all experiments with detailed information."""
    print("\n📋 Detailed experiment listing...")
    print("=" * 50)
    
    try:
        experiments = mlflow_workspace_service.list_experiments()
        
        if not experiments:
            print("❌ No experiments found")
            return []
        
        exp_info = []
        for exp in experiments:
            exp_obj = exp.experiment if hasattr(exp, 'experiment') else exp
            name = getattr(exp_obj, 'name', 'N/A')
            exp_id = getattr(exp_obj, 'experiment_id', 'N/A')
            lifecycle_stage = getattr(exp_obj, 'lifecycle_stage', 'N/A')
            
            exp_info.append({
                'name': name,
                'id': exp_id,
                'lifecycle_stage': lifecycle_stage
            })
            
            print(f"📁 Experiment: '{name}'")
            print(f"   ID: {exp_id}")
            print(f"   Lifecycle: {lifecycle_stage}")
            
            # Try to get runs for this experiment
            try:
                runs_df = mlflow_workspace_service.get_runs(name)
                print(f"   Runs: {len(runs_df)} found")
            except Exception as run_error:
                print(f"   Runs: Error fetching - {run_error}")
            
            print()
        
        return exp_info
        
    except Exception as e:
        print(f"❌ Error listing experiments: {e}")
        return []

def test_logged_models_for_experiment(experiment_name):
    """Test logged models for a specific experiment."""
    print(f"\n🔍 Testing logged models for: '{experiment_name}'")
    print("=" * 50)
    
    try:
        # Check if experiment exists
        experiment = mlflow_workspace_service.workspace_client.experiments.get_by_name(experiment_name)
        if experiment is None:
            print(f"❌ Experiment '{experiment_name}' not found")
            return pd.DataFrame()
        
        experiment_id = experiment.experiment.experiment_id
        print(f"✅ Found experiment ID: {experiment_id}")
        
        # Test the logged models API directly
        print("🔍 Testing search_logged_models API...")
        
        try:
            logged_models_response = mlflow_workspace_service.workspace_client.experiments.search_logged_models(
                experiment_ids=[experiment_id],
                max_results=10
            )
            
            models_count = len(logged_models_response.models) if hasattr(logged_models_response, 'models') else 0
            print(f"📊 Raw API returned {models_count} models")
            
            # Use our service method
            print("🔍 Testing service method...")
            logged_models_df = mlflow_workspace_service.get_logged_models(experiment_name)
            
            if logged_models_df.empty:
                print("❌ Service method returned empty DataFrame")
            else:
                print(f"✅ Service method returned {len(logged_models_df)} models")
                print(f"📊 Columns: {list(logged_models_df.columns)}")
                
                # Show first model details
                if len(logged_models_df) > 0:
                    first_model = logged_models_df.iloc[0]
                    print(f"\n📋 First model details:")
                    for col, val in first_model.items():
                        if pd.notna(val) and val is not None:
                            print(f"   {col}: {val}")
            
            return logged_models_df
            
        except Exception as api_error:
            print(f"❌ API Error: {api_error}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return pd.DataFrame()

def main():
    print("🧪 MLflow Logged Models Debug Script")
    print("=" * 70)
    
    # Test basic connectivity
    if not test_basic_connectivity():
        print("\n❌ Basic connectivity failed. Check your Databricks configuration.")
        return
    
    # List all experiments
    experiments = list_all_experiments_detailed()
    
    if not experiments:
        print("❌ No experiments found. Cannot test logged models.")
        return
    
    # Test logged models for each experiment
    for exp in experiments[:3]:  # Test first 3 experiments
        experiment_name = exp['name']
        if experiment_name and experiment_name != 'N/A':
            models_df = test_logged_models_for_experiment(experiment_name)
            
            if not models_df.empty:
                print(f"🎯 Found models in experiment: '{experiment_name}'")
                break
    else:
        print("\n❌ No logged models found in any tested experiments")
    
    print(f"\n✅ Debug script completed!")
    print(f"💡 To test a specific project, run:")
    print(f"   python test_logged_models_by_project.py <project_name>")

if __name__ == "__main__":
    main()