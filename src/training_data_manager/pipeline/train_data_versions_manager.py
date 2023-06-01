import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.pvalue import AsSingleton

from .sampler.sample_custom import SplitStratified
from .utils import JsonCoder, ReadCurrentTimestamp
from .utils.coder import JsonCoder


def run(pipeline_args, data_manager_options):
    with beam.Pipeline(options=pipeline_args) as p:
        job_timestamp = data_manager_options.job_timestamp
        dt_old_version = data_manager_options.dt
        bucket = data_manager_options.bucket
        new_dataset_path = data_manager_options.new_dataset_path
        data_type = data_manager_options.data_type
        sample_fraction_val = float(data_manager_options.sample_fraction)

        current_time = p | "Read Current Time" >> ReadCurrentTimestamp()
        current_time_singleton = AsSingleton(current_time)  ## to be used later

        new_data_input = (
            p
            | "Create new dataset pcoll" >> beam.Create([new_dataset_path])
            | "Read new dataset" >> beam.io.ReadAllFromText(coder=JsonCoder())
        )

        #### Move old data if exists ---------
        old_data_path = f"gs://{bucket}/data_versions/{dt_old_version}"

        ## TODO: add to transform
        source_path_match_res = FileSystems.match([f"{old_data_path}/**jsonl"])[0]
        source_path_match_files = [f.path for f in source_path_match_res.metadata_list]
        dest_path_files = [
            dt_file.replace(f"/{dt_old_version}/", f"/{job_timestamp}/")
            for dt_file in source_path_match_files
        ]

        FileSystems.copy(
            source_file_names=source_path_match_files,
            destination_file_names=dest_path_files,
        )

        #### Process new dataset -----------
        # split stratified if data_type == 'train' (i.e train_valid)
        if data_type == "train":
            data_input_keyed = new_data_input | "Key new_data_input" >> beam.WithKeys(
                lambda x: x["tag"]
            )

            ## split to two parts (part_01 for train, part_02 for valid)
            (
                sampled_data_part01,
                sampled_data_part02,
            ) = data_input_keyed | "Split Stratified" >> SplitStratified(
                sample_fraction=sample_fraction_val
            )

            ## add train/valid flag
            sampled_data_train = sampled_data_part01 | "Add type part01" >> beam.Map(
                lambda x: {**x, "is_valid": False}
            )
            sampled_data_valid = sampled_data_part02 | "Add type part02" >> beam.Map(
                lambda x: {**x, "is_valid": True}
            )

            ## gather train valid in one dataset
            new_dataset_processed = [
                sampled_data_train,
                sampled_data_valid,
            ] | "Flatten" >> beam.Flatten()

            data_type_op = "train_valid"

        elif data_type == "test":
            new_dataset_processed = new_data_input
            data_type_op = "test"
        else:
            raise Exception(f"Invalid data_type value: {data_type}")

        final_dataset = new_dataset_processed

        #### Write processed data -------------
        final_dataset | "Write processed data" >> beam.io.WriteToText(
            f"gs://{bucket}/data_versions/{job_timestamp}/{data_type_op}-{job_timestamp}",
            file_name_suffix=".jsonl",
            shard_name_template="",
            num_shards=1,
        )

        ## Do other stuff
        ## ....
