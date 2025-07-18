import pandas as pd
import mlflow
from databricks.sdk import WorkspaceClient

class MLflowWorkspaceService:
    def __init__(self):
        # Set up MLflow tracking
        mlflow.set_tracking_uri("databricks")
        
        # Initialize the workspace client lazily
        self._workspace_client = None
    
    @property
    def workspace_client(self):
        """Lazy initialization of workspace client."""
        if self._workspace_client is None:
            try:
                self._workspace_client = WorkspaceClient()
            except Exception as e:
                print(f"Warning: Could not initialize WorkspaceClient: {e}")
                print("Please ensure Databricks credentials are properly configured.")
                raise
        return self._workspace_client
    
    def get_experiment_id(self, experiment_name):
        """Get experiment ID by name."""
        try:
            experiment = self.workspace_client.experiments.get_by_name(experiment_name)
            return experiment.experiment.experiment_id if experiment else None
        except Exception as e:
            print(f"Error getting experiment ID: {str(e)}")
            return None
    
    def create_experiment(self, experiment_name, artifact_location=None):
        """Create a new experiment following the documented API."""
        try:
            experiment = self.workspace_client.experiments.create(
                name=experiment_name,
                artifact_location=artifact_location
            )
            return experiment.experiment.experiment_id
        except Exception as e:
            print(f"Error creating experiment: {str(e)}")
            return None
    
    def delete_experiment(self, experiment_name):
        """Delete an experiment following the documented API."""
        try:
            experiment = self.workspace_client.experiments.get_by_name(experiment_name)
            if experiment:
                self.workspace_client.experiments.delete(experiment.experiment.experiment_id)
                return True
            return False
        except Exception as e:
            print(f"Error deleting experiment: {str(e)}")
            return False
    
    def list_experiments(self, max_results=1000):
        """List experiments following the documented API."""
        try:
            experiments = self.workspace_client.experiments.list_experiments()
            return [exp for exp in experiments]
        except Exception as e:
            print(f"Error listing experiments: {str(e)}")
            return []
    
    def get_runs(self, experiment_name='/ML/mlflow_workshop/mlflow3-ml-example'):
        """Fetch MLflow runs for a given experiment."""
        try:
            # Get experiment by name
            experiment = self.workspace_client.experiments.get_by_name(experiment_name)
            if experiment is None:
                print(f"Experiment '{experiment_name}' not found")
                return pd.DataFrame()
            
            # Search runs for the experiment using the documented API
            runs = self.workspace_client.experiments.search_runs(
                experiment_ids=[experiment.experiment.experiment_id],
                max_results=1000  # Adjust as needed
            )
            
            runs_data = []
            for run in runs:
                run_info = {
                    'run_name': run.info.run_name,
                    'run_id': run.info.run_id,
                    'status': str(run.info.status) if run.info.status else None,
                    'start_time': pd.to_datetime(run.info.start_time, unit='ms'),
                    'end_time': pd.to_datetime(run.info.end_time, unit='ms') if run.info.end_time else None
                }
                
                # Add metrics - following the documented API structure
                if hasattr(run, 'data') and hasattr(run.data, 'metrics'):
                    for metric in run.data.metrics:
                        run_info[f'metric_{str(metric.key)}'] = metric.value
                
                # Add parameters - following the documented API structure
                if hasattr(run, 'data') and hasattr(run.data, 'params'):
                    for param in run.data.params:
                        run_info[f'param_{str(param.key)}'] = param.value
                
                runs_data.append(run_info)
            
            return pd.DataFrame(runs_data)
        except Exception as e:
            print(f"Error fetching MLflow runs: {str(e)}")
            return pd.DataFrame()
    
    def create_run(self, experiment_name, run_name=None, tags=None):
        """Create a new run following the documented API."""
        try:
            experiment = self.workspace_client.experiments.get_by_name(experiment_name)
            if experiment is None:
                print(f"Experiment '{experiment_name}' not found")
                return None
            
            run = self.workspace_client.experiments.create_run(
                experiment_id=experiment.experiment.experiment_id,
                start_time=int(pd.Timestamp.now().timestamp() * 1000),
                run_name=run_name,
                tags=tags or []
            )
            return run.info.run_id
        except Exception as e:
            print(f"Error creating run: {str(e)}")
            return None
    
    def get_run(self, run_id):
        """Get a specific run by ID following the documented API."""
        try:
            run = self.workspace_client.experiments.get_run(run_id)
            return run
        except Exception as e:
            print(f"Error getting run: {str(e)}")
            return None
    
    def update_run(self, run_id, status=None, end_time=None):
        """Update a run following the documented API."""
        try:
            update_data = {}
            if status:
                update_data['status'] = status
            if end_time:
                update_data['end_time'] = int(pd.Timestamp(end_time).timestamp() * 1000)
            
            if update_data:
                self.workspace_client.experiments.update_run(run_id, **update_data)
            return True
        except Exception as e:
            print(f"Error updating run: {str(e)}")
            return False
    
    def log_metric(self, run_id, key, value, step=None, timestamp=None):
        """Log a metric to a run following the documented API."""
        try:
            metric_data = {
                'key': key,
                'value': value
            }
            if step is not None:
                metric_data['step'] = step
            if timestamp is not None:
                metric_data['timestamp'] = int(pd.Timestamp(timestamp).timestamp() * 1000)
            
            self.workspace_client.experiments.log_metric(run_id, **metric_data)
            return True
        except Exception as e:
            print(f"Error logging metric: {str(e)}")
            return False
    
    def log_param(self, run_id, key, value):
        """Log a parameter to a run following the documented API."""
        try:
            self.workspace_client.experiments.log_parameter(run_id, key=key, value=value)
            return True
        except Exception as e:
            print(f"Error logging parameter: {str(e)}")
            return False
    
    def get_metrics_columns(self, runs_df):
        """Get list of metric columns from runs dataframe."""
        return [col for col in runs_df.columns if col.startswith('metric_')]
    
    def get_parameters_columns(self, runs_df):
        """Get list of parameter columns from runs dataframe."""
        return [col for col in runs_df.columns if col.startswith('param_')]
    
    def prepare_metrics_plot_data(self, runs_df):
        """Prepare data for metrics timeline visualization."""
        metric_cols = self.get_metrics_columns(runs_df)
        
        if not metric_cols:
            return pd.DataFrame()
        
        plot_data = []
        for _, row in runs_df.iterrows():
            for metric_col in metric_cols:
                if pd.notna(row[metric_col]):
                    plot_data.append({
                        'start_time': row['start_time'],
                        'metric_name': metric_col.replace('metric_', ''),
                        'metric_value': row[metric_col],
                        'run_name': row['run_name'],
                        'run_id': row['run_id']
                    })
        
        return pd.DataFrame(plot_data)
    
    def get_experiment_summary(self, experiment_name='/ML/mlflow_workshop/mlflow3-ml-example'):
        """Get summary statistics for an experiment."""
        runs_df = self.get_runs(experiment_name)
        
        if runs_df.empty:
            return {
                'total_runs': 0,
                'completed_runs': 0,
                'failed_runs': 0,
                'running_runs': 0,
                'experiment_name': experiment_name
            }
        
        summary = {
            'total_runs': len(runs_df),
            'completed_runs': len(runs_df[runs_df['status'] == 'FINISHED']),
            'failed_runs': len(runs_df[runs_df['status'] == 'FAILED']),
            'running_runs': len(runs_df[runs_df['status'] == 'RUNNING']),
            'experiment_name': experiment_name,
            'date_range': {
                'start': runs_df['start_time'].min(),
                'end': runs_df['start_time'].max()
            }
        }
        
        return summary
    
    def get_logged_models(self):
        """Get all logged models from MLflow using search_logged_models API."""
        try:
            # Get the experiment ID for the default experiment
            experiment = self.workspace_client.experiments.get_by_name('/ML/mlflow_workshop/mlflow3-ml-example')
            if experiment is None:
                print("Default experiment not found")
                return pd.DataFrame()
            
            experiment_id = experiment.experiment.experiment_id
            
            # Use the search_logged_models API as documented in the Databricks SDK
            # This returns a SearchLoggedModelsResponse object
            logged_models_response = self.workspace_client.experiments.search_logged_models(
                experiment_ids=[experiment_id],
                max_results=10 #don't chane this to 1000...it creates an error
            )
            
            # Debug: Print response structure
            print(f"DEBUG: Response type: {type(logged_models_response)}")
            print(f"DEBUG: Response attributes: {dir(logged_models_response)}")
            
            models_data = []
            all_metric_names = set()
            all_param_names = set()
            all_dataset_names = set()
            dataset_metric_combinations = set()
            
            # First pass: collect all metric names, parameter names, dataset names
            # SearchLoggedModelsResponse has a 'logged_models' attribute containing LoggedModelInfo objects
            for model in logged_models_response.models: #do't change this to logged_models...it creates an error
                # Access model.info to get the actual model information
                model_info = model.info
                
                # Debug: Print model structure
                print(f"DEBUG: Model type: {type(model)}")
                print(f"DEBUG: Model info type: {type(model_info)}")
                print(f"DEBUG: Model info attributes: {dir(model_info)}")
                
                # Collect metric names and dataset names
                if hasattr(model_info, 'metrics') and model_info.metrics:
                    print(f"DEBUG: Found metrics: {len(model_info.metrics)}")
                    for metric in model_info.metrics:
                        if hasattr(metric, 'key'):
                            metric_key = str(metric.key)
                            all_metric_names.add(metric_key)
                            dataset_name = getattr(metric, 'dataset_name', None)
                            if dataset_name:
                                all_dataset_names.add(dataset_name)
                                dataset_metric_combinations.add((dataset_name, metric_key))
                            else:
                                dataset_metric_combinations.add((None, metric_key))
                else:
                    print(f"DEBUG: No metrics found for model")
                
                # Collect parameter names
                if hasattr(model_info, 'parameters') and model_info.parameters:
                    print(f"DEBUG: Found parameters: {len(model_info.parameters)}")
                    for param in model_info.parameters:
                        if hasattr(param, 'key'):
                            all_param_names.add(str(param.key))
                else:
                    print(f"DEBUG: No parameters found for model")
            
            print(f"DEBUG: All metric names: {all_metric_names}")
            print(f"DEBUG: All param names: {all_param_names}")
            print(f"DEBUG: Dataset combinations: {dataset_metric_combinations}")
            
            # Second pass: create data with only existing metric-dataset combinations as columns
            for model in logged_models_response.models:  #don't change this to logged_models...it creates an error
                # Access model.info to get the actual model information
                model_info_obj = model.info
                
                # LoggedModelInfo object has these attributes according to the documentation
                model_info = {
                    'model_id': getattr(model_info_obj, 'model_id', None),
                    'model_name': getattr(model_info_obj, 'name', None),
                    'catalog_name': 'mlflow',  # Since we're using MLflow experiments
                    'schema_name': 'logged_models',
                    'creation_timestamp': pd.to_datetime(getattr(model_info_obj, 'creation_timestamp', None), unit='ms') if getattr(model_info_obj, 'creation_timestamp', None) else None,
                    'last_updated_timestamp': pd.to_datetime(getattr(model_info_obj, 'last_updated_timestamp', None), unit='ms') if getattr(model_info_obj, 'last_updated_timestamp', None) else None,
                    'user_id': getattr(model_info_obj, 'user_id', None),
                    'description': getattr(model_info_obj, 'description', '') or ''
                }
                
                # Debug: Print what we're extracting
                print(f"DEBUG: Extracted model_info: {model_info}")
                
                # Add metric columns organized by dataset
                metrics_by_dataset = {}
                if hasattr(model_info_obj, 'metrics') and model_info_obj.metrics:
                    for metric in model_info_obj.metrics:
                        if hasattr(metric, 'key') and hasattr(metric, 'value'):
                            metric_key = str(metric.key)
                            dataset_name = getattr(metric, 'dataset_name', None)
                            if dataset_name not in metrics_by_dataset:
                                metrics_by_dataset[dataset_name] = {}
                            metrics_by_dataset[dataset_name][metric_key] = metric.value
                
                # Create columns only for existing metric-dataset combinations
                for dataset_name, metric_name in sorted(dataset_metric_combinations, key=lambda x: (x[0] or '', x[1])):
                    if dataset_name is None:
                        # General metric (no dataset)
                        column_name = metric_name
                        model_info[column_name] = metrics_by_dataset.get(None, {}).get(metric_name, None)
                    else:
                        # Dataset-specific metric
                        column_name = f"{dataset_name}_{metric_name}"
                        model_info[column_name] = metrics_by_dataset.get(dataset_name, {}).get(metric_name, None)
                
                # Add parameter columns
                params_dict = {}
                if hasattr(model_info_obj, 'parameters') and model_info_obj.parameters:
                    for param in model_info_obj.parameters:
                        if hasattr(param, 'key') and hasattr(param, 'value'):
                            param_key = str(param.key)
                            params_dict[param_key] = param.value
                
                # Add all parameter columns (with None for missing values)
                for param_name in all_param_names:
                    model_info[f'param_{param_name}'] = params_dict.get(param_name, None)
                
                models_data.append(model_info)
            
            result_df = pd.DataFrame(models_data)
            print(f"DEBUG: Final DataFrame columns: {list(result_df.columns)}")
            print(f"DEBUG: Final DataFrame shape: {result_df.shape}")
            
            return result_df
                
        except Exception as e:
            print(f"Error fetching logged models: {str(e)}")
            return pd.DataFrame()
    
    def get_dataset_metrics_summary(self, models_df):
        """Analyze metrics by dataset and return a summary."""
        if models_df.empty:
            return {}
        
        # Extract metric columns
        standard_columns = ['model_id', 'model_name', 'creation_timestamp', 'last_updated_timestamp', 'user_id', 'description']
        metric_columns = [col for col in models_df.columns if col not in standard_columns and not col.startswith('param_')]
        
        # Group metrics by dataset based on column names
        dataset_metrics = {}
        for metric_col in metric_columns:
            # Check if this is a dataset-specific metric (format: dataset_metric)
            parts = metric_col.split('_', 1)
            if len(parts) > 1:
                # Check if the first part looks like a dataset name
                dataset_name = parts[0]
                metric_name = parts[1]
                
                # Common dataset names
                if dataset_name.lower() in ['train', 'validation', 'val', 'test', 'eval', 'evaluation']:
                    if dataset_name not in dataset_metrics:
                        dataset_metrics[dataset_name] = []
                    dataset_metrics[dataset_name].append({
                        'full_name': metric_col,
                        'metric_name': metric_name,
                        'dataset': dataset_name
                    })
                else:
                    # This might be a general metric without dataset prefix
                    if 'general' not in dataset_metrics:
                        dataset_metrics['general'] = []
                    dataset_metrics['general'].append({
                        'full_name': metric_col,
                        'metric_name': metric_col,
                        'dataset': 'general'
                    })
            else:
                # Single word metric, treat as general (no dataset)
                if 'general' not in dataset_metrics:
                    dataset_metrics['general'] = []
                dataset_metrics['general'].append({
                    'full_name': metric_col,
                    'metric_name': metric_col,
                    'dataset': 'general'
                })
        
        return dataset_metrics

# Create a global instance for easy access
try:
    mlflow_workspace_service = MLflowWorkspaceService()
except Exception as e:
    print(f"Warning: Could not initialize MLflowWorkspaceService: {e}")
    print("The service will be initialized when first used.")
    mlflow_workspace_service = None 