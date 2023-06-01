from apache_beam.options.pipeline_options import PipelineOptions

# from apache_beam.options.value_provider import RuntimeValueProvider


class DataManagerOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--job_timestamp",
            dest="job_timestamp",
            required=True,
            default="",
            help="job time",
        )

        parser.add_argument(
            "--dt",
            dest="dt",
            required=True,
            help="prefix of the old version to be enriched with the new dataset to create the new version. If old files exists under this prefix --> will be copied to the new version before adding the new dataset",
        )

        parser.add_argument(
            "--new_dataset_path",
            dest="new_dataset_path",
            required=True,
            help="input file path for the new dataset (e.g. gs://....jsonl)",
        )

        parser.add_argument(
            "--bucket",
            dest="bucket",
            default="image-tagging-data-manager-output",
            help="gcs bucket name for data",
        )

        parser.add_argument(
            "--data_type",
            dest="data_type",
            default="train",
            choices=["train", "test"],
            help="type of provided data",
        )
        parser.add_argument(
            "--sample_fraction",
            dest="sample_fraction",
            default=0.8,
            help="train data fraction in case of data_type='train', range from 0-1",
        )
