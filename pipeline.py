import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
def parse_csv(row):
    reader = csv.reader(row.split(','))
    for row in reader:
        yield row

def run_pipeline(event, context):
    # Get the uploaded file bucket and name
    bucket = event['bucket']
    name = event['name']

    # Create the pipeline
    options = PipelineOptions(
        flags=None,
        runner='DataflowRunner',
        project='sports-analytics-410301',
        temp_location='gs://cfdb-temp-bucket/temp',
        region='us-west1'

    )
    with beam.Pipeline(options=options) as p:
        csv_rows = p | beam.io.ReadFromText(f'gs://{bucket}/{name}', skip_header_lines=1)

        # Parse the CSV rows into a dictionary
        parsed_rows = csv_rows | beam.Map(parse_csv)
