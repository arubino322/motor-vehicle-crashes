import logging
import json
import sys
from datetime import date, timedelta

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io import WriteToBigQuery
import requests


class ReadFromAPI(beam.DoFn):
    def process(self, element, dt):
        url = f"https://data.cityofnewyork.us/resource/h9gi-nx95.json?$where=crash_date = '{dt}T00:00:00.000'"
        # url = f"https://data.cityofnewyork.us/resource/h9gi-nx95.json?$where=crash_date BETWEEN '{dt}T00:00:00' AND '2024-06-30T00:00:00' LIMIT 50000"
        api_key, secret_key = os.environ['NYCT_API_KEY'], os.environ['NYCT_SECRET_KEY']  # TODO: FIGURE ENVVARS IN DATAFLOW
        response = requests.get(url, auth=(api_key, secret_key))
        response.raise_for_status()
        data = response.json()
        for record in data:
            yield record

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--dt', required=True, help='Date in YYYY-MM-DD format')
    args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    date_str = args.dt  # Store the date as a string
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'nyc-transit-426211'
    google_cloud_options.job_name = f'gcs-to-bq-{(date.today()).strftime("%Y-%m-%d-%H-%M-%S")}'
    google_cloud_options.region = 'us-east4'
    google_cloud_options.staging_location = 'gs://motor-vehicle-crashes/staging'
    google_cloud_options.temp_location = 'gs://motor-vehicle-crashes/temp'
    

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
            | 'Start' >> beam.Create([None])
            | 'Read from API' >> beam.ParDo(ReadFromAPI(), date_str)
            | 'Process Data' >> beam.Map(lambda record: {
                "crash_date": record.get("crash_date"),
                "crash_time": record.get("crash_time"),
                "borough": record.get("borough"),
                "zip_code": record.get("zip_code"),
                "latitude": record.get("latitude"),
                "longitude": record.get("longitude"),
                "cross_street_name": record.get("cross_street_name"),
                "on_street_name": record.get("on_street_name"),
                "off_street_name": record.get("off_street_name"),
                "number_of_persons_injured": record.get("number_of_persons_injured"),
                "number_of_persons_killed": record.get("number_of_persons_killed"),
                "number_of_pedestrians_injured": record.get("number_of_pedestrians_injured"),
                "number_of_pedestrians_killed": record.get("number_of_pedestrians_killed"),
                "number_of_cyclist_injured": record.get("number_of_cyclist_injured"),
                "number_of_cyclist_killed": record.get("number_of_cyclist_killed"),
                "number_of_motorist_injured": record.get("number_of_motorist_injured"),
                "number_of_motorist_killed": record.get("number_of_motorist_killed"),
                "contributing_factor_vehicle_1": record.get("contributing_factor_vehicle_1"),
                "contributing_factor_vehicle_2": record.get("contributing_factor_vehicle_2"),
                "contributing_factor_vehicle_3": record.get("contributing_factor_vehicle_3"),
                "contributing_factor_vehicle_4": record.get("contributing_factor_vehicle_4"),
                "collision_id": record.get("collision_id"),
                "vehicle_type_code1": record.get("vehicle_type_code1"),
                "vehicle_type_code2": record.get("vehicle_type_code2"),
                "vehicle_type_code_3": record.get("vehicle_type_code_3"),
                "vehicle_type_code_4": record.get("vehicle_type_code_4"),
                "vehicle_type_code_5": record.get("vehicle_type_code_5")
            })
            | 'Write to BigQuery' >> WriteToBigQuery(
                'nyc-transit-426211:motor_vehicle_crashes.collisions',
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
