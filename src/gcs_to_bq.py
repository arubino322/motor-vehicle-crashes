import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io import WriteToBigQuery, ReadFromText
import json

def parse_json(element):
    """Parses a JSON string into a Python dictionary."""
    try:
        return json.loads(element)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None

def run(argv=None):
    pipeline_options = PipelineOptions()
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'nyc-transit-426211'
    google_cloud_options.job_name = 'gcs-to-bq'
    google_cloud_options.region = 'us-east4'
    google_cloud_options.service_account_email = 'SA'
    google_cloud_options.staging_location = 'gs://motor-vehicle-crashes/staging'
    google_cloud_options.temp_location = 'gs://motor-vehicle-crashes/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    table_schema = {
        "fields": [
            {"name": "crash_date", "type": "STRING"},
            {"name": "crash_time", "type": "STRING"},
            {"name": "borough", "type": "STRING"},
            {"name": "zip_code", "type": "STRING"},
            {"name": "latitude", "type": "STRING"},
            {"name": "longitude", "type": "STRING"},
            {
                "name": "location", 
                "type": "RECORD", 
                "mode": "NULLABLE",
                "fields": [
                    {"name": "latitude", "type": "STRING"},
                    {"name": "longitude", "type": "STRING"},
                    {"name": "human_address", "type": "STRING"}
                ]
            },
            {"name": "cross_street_name", "type": "STRING"},
            {"name": "on_street_name", "type": "STRING"},
            {"name": "off_street_name", "type": "STRING"},
            {"name": "number_of_persons_injured", "type": "STRING"},
            {"name": "number_of_persons_killed", "type": "STRING"},
            {"name": "number_of_pedestrians_injured", "type": "STRING"},
            {"name": "number_of_pedestrians_killed", "type": "STRING"},
            {"name": "number_of_cyclist_injured", "type": "STRING"},
            {"name": "number_of_cyclist_killed", "type": "STRING"},
            {"name": "number_of_motorist_injured", "type": "STRING"},
            {"name": "number_of_motorist_killed", "type": "STRING"},
            {"name": "contributing_factor_vehicle_1", "type": "STRING"},
            {"name": "contributing_factor_vehicle_2", "type": "STRING"},
            {"name": "contributing_factor_vehicle_3", "type": "STRING"},
            {"name": "contributing_factor_vehicle_4", "type": "STRING"},
            {"name": "collision_id", "type": "STRING"},
            {"name": "vehicle_type_code1", "type": "STRING"},
            {"name": "vehicle_type_code2", "type": "STRING"},
            {"name": "vehicle_type_code_3", "type": "STRING"},
            {"name": "vehicle_type_code_4", "type": "STRING"},
            {"name": "vehicle_type_code_5", "type": "STRING"}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from GCS' >> ReadFromText('gs://motor-vehicle-crashes/collisions/2024-06-12/data.json')
            | 'Parse JSON' >> beam.Map(parse_json)
            | 'Filter None' >> beam.Filter(lambda record: record is not None)  # Filter out None values
            | 'Write to BigQuery' >> WriteToBigQuery(
                'nyc-transit-426211:motor_vehicle_crashes.collisions',
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
