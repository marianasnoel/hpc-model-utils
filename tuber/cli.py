import click

from tuber.decomp.pos_processa_decomp import pos_processa_decomp
from tuber.dessem.pos_processa_dessem import pos_processa_dessem
from tuber.dessem.pre_processa_dessem import pre_processa_dessem
from tuber.newave.pos_processa_newave import pos_processa_newave
from tuber.newave.programas_auxiliares_newave import programas_auxiliares_newave


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
