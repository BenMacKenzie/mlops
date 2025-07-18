#!/usr/bin/env python3

from mlflow_rest_service import MLflowRestService

def test_authentication():
    """Test the authentication setup."""
    try:
        # Initialize the service
        service = MLflowRestService()
        
        # Test connection
        print("Testing connection...")
        if service.test_connection():
            print("✅ Authentication successful!")
            
            # Test getting experiment summary
            print("Testing experiment summary...")
            summary = service.get_experiment_summary()
            print(f"Experiment: {summary.get('experiment_name', 'N/A')}")
            print(f"Total runs: {summary.get('total_runs', 0)}")
            
            # Test getting runs
            print("Testing getting runs...")
            runs = service.get_runs()
            print(f"Retrieved {len(runs)} runs")
            
            # Test getting logged models
            print("Testing getting logged models...")
            models = service.get_logged_models()
            print(f"Retrieved {len(models)} logged models")
            
        else:
            print("❌ Authentication failed!")
            
    except Exception as e:
        print(f"❌ Error during authentication test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_authentication() 