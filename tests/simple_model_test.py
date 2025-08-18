#!/usr/bin/env python3
"""
Simple test script for logged models.
Usage: python simple_model_test.py <project_name>

This script helps debug why logged models aren't showing up in the UI.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_project_models(project_name):
    """Test logged models for a project."""
    print(f"🔍 Testing logged models for project: '{project_name}'")
    print("=" * 60)
    
    try:
        from utils.mlflow_service import mlflow_workspace_service
        
        if mlflow_workspace_service is None:
            print("❌ MLflow service is not initialized")
            print("💡 This usually means Databricks authentication is not configured")
            print("📋 Check these environment variables:")
            print(f"   DATABRICKS_HOST: {os.getenv('DATABRICKS_HOST', 'NOT SET')}")
            print(f"   DATABRICKS_TOKEN: {'SET' if os.getenv('DATABRICKS_TOKEN') else 'NOT SET'}")
            print(f"   DATABRICKS_WAREHOUSE_ID: {os.getenv('DATABRICKS_WAREHOUSE_ID', 'NOT SET')}")
            return
        
        print("✅ MLflow service is available")
        
        # Test different experiment name formats that the UI might use
        experiment_formats = [
            f"/{project_name}",           # /project_name
            f"/Users/{os.getenv('USER', 'unknown')}/{project_name}",  # /Users/username/project_name
            project_name,                 # project_name
        ]
        
        print(f"🧪 Testing experiment name formats for project '{project_name}':")
        
        for exp_name in experiment_formats:
            print(f"\n📊 Testing experiment: '{exp_name}'")
            
            try:
                # Check if experiment exists
                experiment = mlflow_workspace_service.workspace_client.experiments.get_by_name(exp_name)
                if experiment is None:
                    print(f"   ❌ Experiment not found")
                    continue
                
                print(f"   ✅ Experiment exists!")
                experiment_id = experiment.experiment.experiment_id
                print(f"   📋 Experiment ID: {experiment_id}")
                
                # Get logged models
                models_df = mlflow_workspace_service.get_logged_models(exp_name)
                
                if models_df.empty:
                    print(f"   📋 No logged models found")
                    
                    # Check if there are any runs in this experiment
                    runs_df = mlflow_workspace_service.get_runs(exp_name)
                    print(f"   📊 Found {len(runs_df)} runs in this experiment")
                    
                    if not runs_df.empty:
                        print(f"   💡 The experiment has runs but no logged models")
                        print(f"   💡 Models are only created when your training code calls mlflow.log_model()")
                else:
                    print(f"   🎯 Found {len(models_df)} logged models!")
                    print(f"   📊 Columns: {list(models_df.columns)}")
                    
                    for idx, model in models_df.iterrows():
                        print(f"\n   📋 Model {idx + 1}:")
                        print(f"      Name: {model.get('model_name', 'N/A')}")
                        print(f"      ID: {model.get('model_id', 'N/A')}")
                        print(f"      Created: {model.get('creation_timestamp', 'N/A')}")
                    
                    return True  # Found models
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        print(f"\n❌ No logged models found for project '{project_name}'")
        return False
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure you're running this from the project root directory")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

def main():
    if len(sys.argv) != 2:
        print("Usage: python simple_model_test.py <project_name>")
        print()
        print("Examples:")
        print("  python simple_model_test.py my_project")
        print("  python simple_model_test.py test_project")
        print()
        print("This script will:")
        print("1. Check if MLflow service is working")
        print("2. Look for experiments matching your project name")
        print("3. Check for logged models in those experiments")
        print("4. Show debugging information if models aren't found")
        sys.exit(1)
    
    project_name = sys.argv[1]
    test_project_models(project_name)

if __name__ == "__main__":
    main()