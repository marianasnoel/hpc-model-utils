from idessem.dessem.dessopc import Dessopc
from idessem.dessem.operut import Operut
from idessem.dessem.dessemarq import DessemArq
import argparse

DessemArq.ENCODING = "ISO-8859-1"


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="realiza o pre processamento do DESSEM"
    )
    parser.add_argument("numero_processadores", type=int)
    args = parser.parse_args()
    num_processadores = args["numero_processadores"]

    dessem_arq = DessemArq.read("./dessem.arq")
    if dessem_arq.dessopc is not None:
        adequa_dessopc(dessem_arq.dessopc.valor, num_processadores)
    else:
        adequa_dessopc(dessem_arq.operut.valor, num_processadores)
