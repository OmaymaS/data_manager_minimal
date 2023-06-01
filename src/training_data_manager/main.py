import logging

from apache_beam.options.pipeline_options import PipelineOptions

from pipeline import train_data_versions_manager
from pipeline.options import DataManagerOptions

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    data_manager_options = pipeline_options.view_as(DataManagerOptions)
    train_data_versions_manager.run(pipeline_options, data_manager_options)
