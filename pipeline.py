import logging
from re import M

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import storage

logging.basicConfig()


def list_folders(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(delimiter="/")
    return set(blob.prefix.rstrip("/") for blob in blobs.prefixes)


def get_header(bucket_name, folder):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"{folder}/"))
    if blobs:
        return next(bucket.blob(blobs[0].name).download_as_text().splitlines())
    return None


class RemoveSubsequentHeaders(beam.DoFn):
    def __init__(self):
        self.first_element = True

    def process(self, element, is_header):
        if is_header:
            if self.first_element:
                self.first_element = False
                yield element
        else:
            yield element


class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--input", type=str, help="Path of the file to read from"
        )
        parser.add_value_provider_argument(
            "--output", type=str, help="Path of the file to write to"
        )
        parser.add_value_provider_argument(
            "--pipeline_name", type=str, help="Name of the pipeline"
        )


def execute_pipeline(options: PipelineOptions, input_bucket, output_bucket):
    folders = list_folders(input_bucket)
    with beam.Pipeline(options=options) as pipeline:
        for folder in folders:
            (
                pipeline
                | f"Read {folder}"
                >> beam.io.ReadFromText(f"gs://{input_bucket}/{folder}/*")
                | f"Add key to {folder}"
                >> beam.Map(lambda x, folder=folder: (folder, x))
                | f"Is header {folder}"
                >> beam.Map(
                    lambda x: (x[0], x[1], x[1] == get_header(input_bucket, folder))
                )
                | f"Remove headers {folder}" >> beam.Pardo(RemoveSubsequentHeaders())
                | f"Remove key from {folder}" >> beam.Map(lambda x: x[1])
                | f"Write merged file for {folder}"
                >> beam.io.WriteToText(
                    f"gs://{output_bucket}/{folder}_merged.csv",
                    num_shards=1,
                    shard_name_template="",
                )
            )


def run():
    pipe_options = PipelineOptions().view_as(MyOptions)
    pipe_options.view_as(SetupOptions).save_main_session = True
    logging.info(f"Pipeline: {pipe_options.pipeline_name}")
    execute_pipeline(
        options=pipe_options,
        input_bucket=pipe_options.input,
        output_location=pipe_options.output,
    )
    logging.info("FINISHED")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
