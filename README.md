1. I had been using the Workspace client but had switched to REST since it didn’t seem to work.
2. The service principal that the app run under didn’t have the right permissions to see the mlflow experiment.
3. I had been keeping the DATABRICKS_TOKEN in a .env file locally..but this was actually being picked up and used by the workspace client (it seems) which create a conflict with oauth.
4. out-of-the-box the databricks-sdk is 0.33.0 in databricks apps.  I had to explicitly set this to 0.57.0 in the requirements.txt file in order for the right version (with the right apis support for mlflow) to work
5. for what its worth, since I export host, token and warehouse as environment variables when running locally, I can instantiate the workspace client with just WorkspaceClient().
