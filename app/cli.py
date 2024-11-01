import click

from app.adapter.repository.abstractmodel import ModelFactory
from app.utils.log import Log


@click.group()
def cli():
    """
    Aplicação para realizar etapas do job script
    para a execução de modelos energéticos em cluster
    HPC.
    """
    pass


@click.command("check_and_fetch_model_executables")
@click.argument("model_name", type=str)
@click.argument("model_version", type=str)
def check_and_fetch_model_executables(model_name, model_version):
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.check_and_fetch_model_executables(model_version)
    except Exception as e:
        logger.exception(str(e))


cli.add_command(check_and_fetch_model_executables)


@click.command("validate_extract_sanitize_inputs")
@click.argument("model_name", type=str)
@click.argument("compressed_input_file", type=str)
def validate_extract_sanitize_inputs(model_name, compressed_input_file):
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.validate_extract_sanitize_inputs(compressed_input_file)
    except Exception as e:
        logger.exception(str(e))


cli.add_command(validate_extract_sanitize_inputs)


@click.command("generate_unique_input_id")
@click.argument("model_name", type=str)
@click.argument("model_version", type=str)
def generate_unique_input_id(model_name, model_version):
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        unique_id = model_type.generate_unique_input_id(model_version)
        print(unique_id)
    except Exception as e:
        logger.exception(str(e))


cli.add_command(generate_unique_input_id)


@click.command("preprocess")
@click.argument("model_name", type=str)
def preprocess(model_name):
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.preprocess()
    except Exception as e:
        logger.exception(str(e))


cli.add_command(preprocess)


@click.command("generate_execution_status")
@click.argument("model_name", type=str)
def generate_execution_status(model_name):
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        status = model_type.generate_execution_status()
        print(status)
    except Exception as e:
        logger.exception(str(e))


cli.add_command(generate_execution_status)


@click.command("postprocess")
@click.argument("model_name", type=str)
def postprocess(model_name):
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.postprocess()
    except Exception as e:
        logger.exception(str(e))


cli.add_command(postprocess)


@click.command("output_compression_and_cleanup")
@click.argument("model_name", type=str)
@click.argument("num_cpus", type=int)
def output_compression_and_cleanup(model_name, num_cpus):
    logger = Log.configure_logger()

    try:
        model_type = ModelFactory().factory(model_name, logger)
        model_type.output_compression_and_cleanup(num_cpus)
    except Exception as e:
        logger.exception(str(e))


cli.add_command(output_compression_and_cleanup)
