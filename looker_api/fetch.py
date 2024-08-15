import looker_sdk
from looker_sdk.sdk.api40.methods import Looker40SDK
import pandas as pd 
import pandas_gbq


#def model(dbt, session):
    # Initialize Looker SDK
sdk = looker_sdk.init40("looker.ini")

# Define the query
query = {
    'model': 'system__activity',  # Replace with your model name
    'view': 'history',  # Replace with the primary view name
    'fields': [
        'user.email',
        'history.source',
        'query.view',
        'history.created_time',
        'look.title',
        'dashboard.title',
        'history.dashboard_session'
    ],

}

# Create the query in Looker
query_response = sdk.create_query(query)

# Run the query
query_id = query_response.id
results = sdk.run_query(query_id=query_id, result_format='json')

# Convert the results to a Pandas DataFrame
df = pd.read_json(results)

# Display the DataFrame
print(df)

df.columns = [
    'user_email',
    'history_source',
    'query_explore',
    'history_created_time',
    'look_title',
    'dashboard_title',
    'history_dashboard_session'
]


# BigQuery export configuration
project_id = 'astrafy-sandbox-greta'
dataset_id = 'system_monitoring'
table_name = 'looker_query_usage'

# Load the DataFrame to BigQuery
pandas_gbq.to_gbq(df, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')
print("Data successfully loaded to BigQuery")