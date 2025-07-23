# MLOps Dashboard

A comprehensive dashboard for managing ML operations, including MLflow experiments, logged models, Databricks jobs, and feature lookups.

## Features

### 1. MLflow Integration
- View MLflow experiment runs
- Monitor logged models
- Track metrics and parameters

### 2. Databricks Jobs Management
- List and monitor Databricks jobs
- View job configurations and schedules

### 3. Feature Lookup Builder
- Create feature lookup configurations for ML models
- Validate table names and get column information
- Select features from available table columns
- Set default values for features
- Export configurations as Python code or JSON

### 4. Taxi Fare Prediction
- Interactive fare prediction based on pickup/dropoff locations
- Data visualization with scatter plots

## Setup

### Environment Variables
Make sure the following environment variables are set:
- `DATABRICKS_WAREHOUSE_ID`: Your Databricks SQL warehouse ID
- `DATABRICKS_HOST`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Your Databricks access token

### Installation
```bash
pip install -r requirements.txt
```

### Running the Application
```bash
python app.py
```

## Feature Lookup Builder

The Feature Lookup Builder allows you to create feature lookup configurations for your ML models. Here's how to use it:

### 1. Enter Table Name
- Enter the full table name (e.g., `ml.recommender_system.customer_features`)
- Click "Validate Table" to verify the table exists and get column information

### 2. Select Features
- Choose the lookup key (e.g., `customer_id`)
- Select the features you want to include from the dropdown
- The dropdown is populated with columns from the validated table

### 3. Set Default Values
- For each selected feature, you can set default values
- These will be used when a lookup key is not found in the table

### 4. Export Configuration
- Export as Python code: Generates `FeatureLookup` objects ready to use in your code
- Export as JSON: Generates JSON representation of the configuration

### Example Output
```python
feature_lookups = [
    FeatureLookup(
        table_name="ml.recommender_system.customer_features",
        feature_names=[
            "membership_tier",
            "age",
            "page_views_count_30days",
        ],
        lookup_key="customer_id",
        default_values={
          "age": 18,
          "membership_tier": "bronze"
        },
    ),
]
```

## Troubleshooting

1. **Workspace Client Issues**: The app uses the Databricks SDK version 0.57.0 for proper MLflow support
2. **Permissions**: Ensure your service principal has the right permissions to access MLflow experiments
3. **Environment Variables**: Make sure all required environment variables are set correctly
4. **OAuth Conflicts**: Avoid keeping DATABRICKS_TOKEN in .env files when using OAuth

## File Structure

- `app.py`: Main application file
- `feature_lookup_ui.py`: Feature lookup UI components and logic
- `mlflow_service.py`: MLflow service integration
- `requirements.txt`: Python dependencies
- `test_feature_lookup.py`: Tests for feature lookup functionality
