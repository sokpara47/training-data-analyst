import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

def parse_csv_line(line):
    # Parse each line of the CSV file.
    # Customize based on your specific CSV format.
    # Extract fields and create a dictionary representing a BigQuery row.
    pass

def run(argv=None, save_main_session=True):
    options = PipelineOptions(flags=argv)

    with beam.Pipeline(options=options) as p:
        (
            p 
            | "Read from GCS" >> beam.io.ReadFromText('gs://demo-aws-data/puttshack-cur*.csv')
            | "Parse CSV" >> beam.Map(parse_csv_line)
            | "Write to BigQuery" >> WriteToBigQuery(
                table='samson-demo-sandbox.newdataset.new_table',
                schema=None, # Schema will be inferred from the first row of your CSV
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                time_partitioning=beam.io.gcp.bigquery.TimePartitioning(type='DAY')
            )
        )

if __name__ == '__main__':
    run()
