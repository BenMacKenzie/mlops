#!/usr/bin/env python3
"""
Test script to fetch logged models for a specific project/experiment.
Usage: python test_logged_models_by_project.py <project_name>
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mlflow_service import mlflow_workspace_service
import pandas as pd

def test_logged_models_by_project(project_name):
    """Test fetching logged models for a specific project."""
    print(f"Testing logged models for project: {project_name}")
    print("=" * 50)
    
    if mlflow_workspace_service is None:
        print("❌ MLflow service not available")
        return
    
    # Try different experiment name formats
    experiment_formats = [
        f"/{project_name}",
        f"/Users/{os.getenv('USER', 'unknown')}/{project_name}",
        f"/Shared/{project_name}",
        project_name,
        f"/ML/{project_name}",
        "/ML/mlflow_workshop/mlflow3-ml-example"  # Default for testing
    ]
    
    print("🔍 Trying different experiment name formats...")
    
    for experiment_name in experiment_formats:
        print(f"\n📊 Trying experiment: '{experiment_name}'")
        
        try:
            # First check if experiment exists
            experiment = mlflow_workspace_service.workspace_client.experiments.get_by_name(experiment_name)
            if experiment is None:
                print(f"   ❌ Experiment not found")
                continue
            
            experiment_id = experiment.experiment.experiment_id
            print(f"   ✅ Found experiment ID: {experiment_id}")
            
            # Get logged models for this experiment
            logged_models_df = mlflow_workspace_service.get_logged_models(experiment_name)
            
            if logged_models_df.empty:
                print(f"   📋 No logged models found in this experiment")
            else:
                print(f"   🎯 Found {len(logged_models_df)} logged models!")
                print(f"   📊 Columns: {list(logged_models_df.columns)}")
                
                # Display model details
                for idx, model in logged_models_df.iterrows():
                    print(f"\n   Model {idx + 1}:")
                    print(f"     - Name: {model.get('model_name', 'N/A')}")
                    print(f"     - ID: {model.get('model_id', 'N/A')}")
                    print(f"     - Created: {model.get('creation_timestamp', 'N/A')}")
                    print(f"     - User: {model.get('user_id', 'N/A')}")
                    
                    # Show metrics
                    metric_cols = [col for col in logged_models_df.columns 
                                  if not col.startswith('param_') and 
                                     col not in ['model_id', 'model_name', 'catalog_name', 'schema_name', 
                                               'creation_timestamp', 'last_updated_timestamp', 'user_id', 'description']]
                    
                    if metric_cols:
                        print(f"     - Metrics:")
                        for metric_col in metric_cols:
                            metric_value = model.get(metric_col)
                            if pd.notna(metric_value) and metric_value is not None:
                                print(f"       * {metric_col}: {metric_value}")
                    
                    # Show parameters
                    param_cols = [col for col in logged_models_df.columns if col.startswith('param_')]
                    if param_cols:
                        print(f"     - Parameters:")
                        for param_col in param_cols:
                            param_value = model.get(param_col)
                            if pd.notna(param_value) and param_value is not None:
                                param_name = param_col.replace('param_', '')
                                print(f"       * {param_name}: {param_value}")
                
                return logged_models_df  # Return the successful result
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            continue
    
    print(f"\n❌ No logged models found for project '{project_name}' in any experiment format")
    return pd.DataFrame()

def list_all_experiments():
    """List all available experiments."""
    print("\n🔍 Listing all available experiments...")
    print("=" * 50)
    
    if mlflow_workspace_service is None:
        print("❌ MLflow service not available")
        return
    
    try:
        experiments = mlflow_workspace_service.list_experiments()
        if not experiments:
            print("❌ No experiments found")
            return
        
        print(f"✅ Found {len(experiments)} experiments:")
        for exp in experiments:
            exp_info = exp.experiment if hasattr(exp, 'experiment') else exp
            exp_name = getattr(exp_info, 'name', 'N/A')
            exp_id = getattr(exp_info, 'experiment_id', 'N/A')
            print(f"   - Name: '{exp_name}' (ID: {exp_id})")
            
    except Exception as e:
        print(f"❌ Error listing experiments: {e}")

def test_default_experiment():
    """Test the default hardcoded experiment."""
    print("\n🧪 Testing default experiment...")
    print("=" * 50)
    
    default_experiment = "/ML/mlflow_workshop/mlflow3-ml-example"
    return test_logged_models_by_project("mlflow3-ml-example")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_logged_models_by_project.py <project_name>")
        print("Example: python test_logged_models_by_project.py my_project")
        sys.exit(1)
    
    project_name = sys.argv[1]
    
    # Test the specific project
    result = test_logged_models_by_project(project_name)
    
    # If no results, try listing all experiments
    if result.empty:
        list_all_experiments()
        
        # Also test the default experiment
        print("\n" + "="*70)
        test_default_experiment()
    
    print("\n✅ Test completed!")