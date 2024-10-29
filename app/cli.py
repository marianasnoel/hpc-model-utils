import click

from app.decomp.pos_processa_decomp import pos_processa_decomp
from app.dessem.pos_processa_dessem import pos_processa_dessem
from app.dessem.pre_processa_dessem import pre_processa_dessem
from app.newave.pos_processa_newave import pos_processa_newave
from app.newave.programas_auxiliares_newave import programas_auxiliares_newave


@click.group()
def cli():
    """
    Aplicação para realizar etapas do job script
    para a execução de modelos energéticos em cluster
    HPC.
    """
    pass


cli.add_command(pos_processa_newave)
cli.add_command(programas_auxiliares_newave)
cli.add_command(pos_processa_decomp)
cli.add_command(pre_processa_dessem)
cli.add_command(pos_processa_dessem)
