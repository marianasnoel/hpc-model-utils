import click

from app.adapter.repository.abstractmodel import ModelFactory
from app.utils.constants import MPICH_PATH, SLURM_PATH
from app.utils.log import Log


@click.group()
def cli():
    """
    CLI app that handles job script steps for running
    energy-related models in HPC clusters.
    """
    pass


@click.command("check_and_fetch_inputs")
@click.argument("model_name", type=str)
@click.argument("filename", type=str)
@click.argument("bucket", type=str)
@click.option("--parent-id", type=str, default="")
@click.option("--delete", is_flag=True, default=False)
def check_and_fetch_inputs(model_name, filename, bucket, parent_id, delete):
    """
    Checks and downloads input data from
    a given S3 bucket.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.check_and_fetch_inputs(
            filename, bucket, parent_id, delete=delete
        )
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(check_and_fetch_inputs)


@click.command("check_and_fetch_executables")
@click.argument("model_name", type=str)
@click.argument("model_version", type=str)
@click.argument("bucket", type=str)
def check_and_fetch_executables(model_name, model_version, bucket):
    """
    Checks and downloads model executables from
    a given S3 bucket.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.check_and_fetch_executables(model_version, bucket)
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(check_and_fetch_executables)


@click.command("extract_sanitize_inputs")
@click.argument("model_name", type=str)
@click.argument("compressed_input_file", type=str)
def extract_sanitize_inputs(model_name, compressed_input_file):
    """
    Deals with a compressed (.zip) file that contains
    model inputs, extracting and fixing encoding.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.extract_sanitize_inputs(compressed_input_file)
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(extract_sanitize_inputs)


@click.command("generate_unique_input_id")
@click.argument("model_name", type=str)
@click.argument("model_version", type=str)
@click.option("--parent-id", type=str, default="")
def generate_unique_input_id(model_name, model_version, parent_id):
    """
    Generates an unique ID by hashing the input data,
    model name and version.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        unique_id = model_type.generate_unique_input_id(
            model_version, parent_id
        )
        logger.info(f"Generated unique ID: {unique_id}")
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(generate_unique_input_id)


@click.command("preprocess")
@click.argument("model_name", type=str)
def preprocess(model_name):
    """
    Runs model-specific pre-processing.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.preprocess()
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(preprocess)


@click.command("run")
@click.argument("model_name", type=str)
@click.argument("queue", type=str)
@click.argument("core_count", type=int)
@click.option("--mpich-path", type=str, default=MPICH_PATH)
@click.option("--slurm-path", type=str, default=SLURM_PATH)
def run(model_name, queue, core_count, mpich_path, slurm_path):
    """
    Runs the model by submitting to a job scheduler.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.run(queue, core_count, mpich_path, slurm_path)
        logger.info("Model execution terminated")
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(run)


@click.command("generate_execution_status")
@click.argument("model_name", type=str)
@click.option("--job-id", type=str, default="")
def generate_execution_status(model_name, job_id):
    """
    Diagnosis the execution status with model-specific
    business rules.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        status = model_type.generate_execution_status(job_id)
        logger.info(f"Generated execution status: {status}")
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(generate_execution_status)


@click.command("postprocess")
@click.argument("model_name", type=str)
def postprocess(model_name):
    """
    Runs model-specific post-processing steps.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.postprocess()
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(postprocess)


@click.command("metadata_generation")
@click.argument("model_name", type=str)
def metadata_generation(model_name):
    """
    Generates a metadata ModelOps file and echoes to the console.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        metadata = model_type.metadata_generation()
        for name, value in metadata.items():
            print(f"METADATA|{name}: {value}")
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(metadata_generation)


@click.command("output_compression_and_cleanup")
@click.argument("model_name", type=str)
@click.argument("num_cpus", type=int)
def output_compression_and_cleanup(model_name, num_cpus):
    """
    Compresses the output files in some groups
    and cleans the root directory.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.output_compression_and_cleanup(num_cpus)
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(output_compression_and_cleanup)


@click.command("result_upload")
@click.argument("model_name", type=str)
@click.argument("filename", type=str)
@click.argument("inputs_bucket", type=str)
@click.argument("outputs_bucket", type=str)
def result_upload(model_name, filename, inputs_bucket, outputs_bucket):
    """
    Uploads the results to an S3 bucket.
    """
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.result_upload(filename, inputs_bucket, outputs_bucket)
    except Exception as e:
        logger.exception(str(e))
        raise e


cli.add_command(result_upload)
