import click
from idessem.dessem.dessemarq import DessemArq
from idessem.dessem.dessopc import Dessopc
from idessem.dessem.operut import Operut

DessemArq.ENCODING = "ISO-8859-1"


@click.command("pre_processa_dessem")
@click.argument("numero_processadores", type=int)
def pre_processa_dessem(numero_processadores):
    def adequa_dessopc(nome_arquivo: str, num_processadores: int):
        dessopc = Dessopc.read(nome_arquivo)
        if dessopc.uctpar is not None:
            dessopc.uctpar = num_processadores
            dessopc.write(nome_arquivo)
        else:
            print("Registro UCTPAR não encontrado no arquivo ", nome_arquivo)

    def adequa_operut(nome_arquivo: str, num_processadores: int):
        operut = Operut.read(nome_arquivo)
        if operut.uctpar is not None:
            operut.uctpar = num_processadores
            operut.write(nome_arquivo)
        else:
            print("Registro UCTPAR não encontrado no arquivo ", nome_arquivo)

    dessem_arq = DessemArq.read("./dessem.arq")
    if dessem_arq.dessopc is not None:
        adequa_dessopc(dessem_arq.dessopc.valor, numero_processadores)
    else:
        adequa_operut(dessem_arq.operut.valor, numero_processadores)
