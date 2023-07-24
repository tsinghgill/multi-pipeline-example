import requests
import csv
import time
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Path to the pipelines.csv file
pipelines_path = Path('pipelines.csv')

# Path to the audit.txt file
audit_path = Path('audit.txt')

# Store the current pipeline configurations
current_pipelines = []

# Function to create a pipeline
def create_pipeline(pipeline_config):
    # Prepare the request body
    payload = {
        "config": {
            "name": pipeline_config['pipeline_name'],
            "description": pipeline_config['pipeline_description']
        }
    }

    # Make the API call to create a new pipeline
    response = requests.post("http://localhost:8080/v1/pipelines", json=payload)
    logging.info(f"Response: {response.text}")  

    if response.status_code == 200:
        pipeline_data = response.json()
        logging.info(f"Created pipeline: {pipeline_data}")

        if 'id' in pipeline_data:
            log_audit(f"Pipeline: {pipeline_data['id']} - Created")
        
        return pipeline_data
    else:
        logging.error("Failed to create pipeline")
        log_audit("Failed to create pipeline")
        return None

# Function to create a connector
def create_connector(connector_config):
    # Prepare the request body
    payload = connector_config
    # Make the API call to create a new connector
    response = requests.post("http://localhost:8080/v1/connectors", json=payload)
    logging.info(f"Response: {response.text}")  

    if response.status_code == 200:
        connector_data = response.json()
        connector_id = connector_data['id']
        logging.info(f"Created connector: {connector_id}")
        log_audit(f"Connector: {connector_id} - Created")
        return connector_data
    else:
        logging.error("Failed to create connector")
        log_audit("Failed to create connector")
        return None

# Function to start a pipeline
def start_pipeline(pipeline_id):
    # Make the API call to start the pipeline
    response = requests.post(f"http://localhost:8080/v1/pipelines/{pipeline_id}/start")

    if response.status_code == 200:
        logging.info(f"Started pipeline: {pipeline_id}")
        log_audit(f"Pipeline: {pipeline_id} - Started")
    else:
        logging.error(f"Failed to start pipeline: {pipeline_id}")
        log_audit(f"Failed to start pipeline: {pipeline_id}")

# Function to stop a pipeline
def stop_pipeline(pipeline_id):
    # Make the API call to stop the pipeline
    response = requests.post(f"http://localhost:8080/v1/pipelines/{pipeline_id}/stop")

    if response.status_code == 200:
        logging.info(f"Stopped pipeline: {pipeline_id}")
        log_audit(f"Pipeline: {pipeline_id} - Stopped")
    else:
        logging.error(f"Failed to stop pipeline: {pipeline_id}")
        log_audit(f"Failed to stop pipeline: {pipeline_id}")

# Function to delete a connector
def delete_connector(connector_id):
    # Make the API call to delete the connector
    response = requests.delete(f"http://localhost:8080/v1/connectors/{connector_id}")

    if response.status_code == 200:
        logging.info(f"Deleted connector: {connector_id}")
        log_audit(f"Connector: {connector_id} - Deleted")
    else:
        logging.error(f"Failed to delete connector: {connector_id}")
        log_audit(f"Failed to delete connector: {connector_id}")

# Function to delete a pipeline
def delete_pipeline(pipeline_id):
    # Make the API call to delete the pipeline
    response = requests.delete(f"http://localhost:8080/v1/pipelines/{pipeline_id}")

    if response.status_code == 200:
        logging.info(f"Deleted pipeline: {pipeline_id}")
        log_audit(f"Pipeline: {pipeline_id} - Deleted")
    else:
        logging.error(f"Failed to delete pipeline: {pipeline_id}")
        log_audit(f"Failed to delete pipeline: {pipeline_id}")

# Function to log audit messages
def log_audit(message):
    with audit_path.open('a') as audit_file:
        audit_file.write(f"{message}\n")

# Watch the pipelines.csv file for changes
def watch_file_changes():
    global current_pipelines

    pipeline_response = None  # Initialize pipeline_response

    # Check if the pipelines.csv file exists
    if pipelines_path.exists():
        # Read the pipeline configs from the file
        with pipelines_path.open('r') as file:
            reader = csv.DictReader(file)
            pipeline_configs = list(reader)

        # Identify added and removed pipelines
        current_pipeline_names = [pipeline['config']['name'] for pipeline in current_pipelines]
        new_pipeline_names = [pipeline['pipeline_name'] for pipeline in pipeline_configs]
        added_pipelines = [pipeline for pipeline in pipeline_configs if pipeline['pipeline_name'] not in current_pipeline_names]
        removed_pipelines = [pipeline for pipeline in current_pipelines if pipeline['config']['name'] not in new_pipeline_names]


        # Provision new pipelines
        for pipeline in added_pipelines:
            # Create the pipeline
            pipeline_response = create_pipeline(pipeline)
            
            # Check the pipeline is successfully created before proceeding
            if pipeline_response is not None:
                # Create source and destination connectors
                source_connector_response = create_connector({
                    'type': 'TYPE_SOURCE',
                    'plugin': 'builtin:s3',
                    'pipelineId': pipeline_response['id'],
                    'config': {
                        'name': pipeline['connector_name_source'],
                        'settings': {
                            'aws.accessKeyId': os.environ.get('AWS_ACCESS_KEY_ID'),
                            'aws.secretAccessKey': os.environ.get('AWS_SECRET_ACCESS_KEY'),
                            'aws.region': os.environ.get('AWS_REGION'),
                            'aws.bucket': pipeline['aws.bucket_source'],
                            'prefix': pipeline['prefix_source']
                        }
                    }
                })

                destination_connector_response = create_connector({
                    'type': 'TYPE_DESTINATION',
                    'plugin': 'builtin:s3',
                    'pipelineId': pipeline_response['id'],
                    'config': {
                        'name': pipeline['connector_name_destination'],
                        'settings': {
                            'aws.accessKeyId': os.environ.get('AWS_ACCESS_KEY_ID'),
                            'aws.secretAccessKey': os.environ.get('AWS_SECRET_ACCESS_KEY'),
                            'aws.region': os.environ.get('AWS_REGION'),
                            'aws.bucket': pipeline['aws.bucket_destination'],
                            'prefix': pipeline['prefix_destination'],
                            'format': pipeline['format_destination']
                        }
                    }
                })

                if source_connector_response is not None and destination_connector_response is not None:
                    # Start the pipeline
                    start_pipeline(pipeline_response['id'])

                    # store the connector data in the pipeline dictionary
                    pipeline_response['source_connector'] = source_connector_response
                    pipeline_response['destination_connector'] = destination_connector_response

                    # Update the current pipeline configurations
                    current_pipelines.append(pipeline_response)

        # Unprovision the removed pipelines
        for pipeline in removed_pipelines:
            # Stop the pipeline
            stop_pipeline(pipeline['id'])

            # Delete the source connector
            delete_connector(pipeline['source_connector']['id'])

            # Delete the destination connector
            delete_connector(pipeline['destination_connector']['id'])

            # Delete the pipeline
            delete_pipeline(pipeline['id'])

            current_pipelines = [p for p in current_pipelines if p['id'] != pipeline['id']]

    else:
        logging.warning(f"pipelines.csv file not found")

# Main loop to watch for changes
if __name__ == '__main__':
    while True:
        watch_file_changes()
        time.sleep(1)  # Sleep for 1 second before checking for changes again
