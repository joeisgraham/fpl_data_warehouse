import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# table_id = "fpl-data-warehouse.fpl_players.fpl_player_history"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
    "/home/joegraham433/fpl-data-warehouse-key.json"
)

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='fpl-data-warehouse',
    region='europe-west2',
    job_name='test-load-csv',
    temp_location='gs://fpl_dw_data/temp'
)

with beam.Pipeline(options=beam_options) as p:
    lines = (
        p | 'Read' >> beam.io.ReadFromText(
            "gs://fpl_dw_data/players_1617_utf8.csv"
        )
        | 'Split' >> beam.Map(lambda x: x.split(","))
        | 'Filter' >> beam.Filter(lambda x: x[0] == "Sam")
        | 'Write' >> beam.io.WriteToText(
            "gs://fpl_dw_data/Sams", ".txt"
        )
    )
