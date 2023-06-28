from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.dger import DGer
import asyncio
from shutil import move
import argparse
from time import time


from tuber.utils import run_terminal


def gera_arquivosdat_nwlistcf():
    linhas = [
        "ARQUIVO DE DADOS GERAIS     : nwlistcf.dat\n",
        "ARQUIVO DE CORTES DE BENDERS: cortes.dat\n",
        "ARQUIVO DE CABECALHO CORTES : cortesh.dat\n",
        "ARQUIVO P/DESPACHO HIDROTERM: newdesp.dat\n",
        "ARQUIVO DE ESTADOS CORTES   : cortese.dat\n",
        "ARQUIVO DE ENERGIAS FORWARD : energiaf.dat\n",
        "ARQUIVO DE RESTRICOES SAR   : rsar.dat\n",
        "ARQUIVO DE CABECALHO SAR    : rsarh.dat\n",
        "ARQUIVO DE INDICE SAR       : rsari.dat\n",
        "ARQUIVO LISTAGEM CORTES     : nwlistcf.rel\n",
        "ARQUIVO LISTAGEM ESTADOS FCF: estados.rel\n",
        "ARQUIVO LISTAGEM SAR        : rsar.rel\n",
        "ARQUIVO DE ENERGIAS X FORW  : energiaxf.dat\n",
        "ARQUIVO DE VAZAO FORWARD    : vazaof.dat\n",
        "ARQUIVO DE VAZAO X FORWARD  : vazaoxf.dat\n",
    ]
    with open("arquivos.dat", "w") as arq:
        arq.writelines(linhas)


def gera_nwlistcf_estagio(estagio: int):
    linhas_anteriores = [
        " INI FIM FC (FC = 1: IMPRIME TODOS CORTES, FC = 0: IMPRIME APENAS CORTES VALIDOS NA ULTIMA ITERACAO)\n",
        " XXX XXX X\n",
    ]
    linhas_seguintes = [
        " OPCOES DE IMPRESSAO : 01 - CORTES FCF  02 - ESTADOS FCF  03 - RESTRICAO SAR\n",
        " XX XX XX (SE 99 CONSIDERA TODAS)\n",
        " 01 02\n",
    ]
    caso = Caso.read("./caso.dat")
    arquivos = Arquivos.read("./" + caso.arquivos)
    dger = DGer.read("./" + arquivos.dger)
    if estagio < 0:
        estagio = (dger.num_anos_estudo * 12) - (estagio + 1)
    else:
        estagio = dger.mes_inicio_estudo + estagio - 1
    mes = str(estagio).zfill(2)
    print(f"Gerando nwlistcf.dat para o mês: {mes}")
    with open("nwlistcf.dat", "w") as arq:
        arq.writelines(linhas_anteriores)
        arq.write(f"  {mes}  {mes} 1\n")
        arq.writelines(linhas_seguintes)


def gera_nwlistopdat_nwlistop(opcao: int):
    caso = Caso.read("./caso.dat")
    arquivos = Arquivos.read("./" + caso.arquivos)
    dger = DGer.read("./" + arquivos.dger)
    estagio_final = dger.num_anos_estudo * 12 - 11
    print(f"Gerando nwlistop.dat opção {opcao} até o estágio: {estagio_final}")
    linhas = [
        f" {opcao}\n",
        "FORWARD  (ARQ. DE DADOS)    : forward.dat\n",
        "FORWARDH (ARQ. CABECALHOS)  : forwarh.dat\n",
        "NEWDESP  (REL. CONFIGS)     : newdesp.dat\n",
        "-----------------------------------------\n",
        " XXX XXX    PERIODOS INICIAL E FINAL\n",
        f"   1 {estagio_final}\n",
        " 1-CMO           2-DEFICITS         3-ENA CONTROL.   4-EARM FINAL       5-ENA FIO BRUTA 6-EVAPORACAO    7-VERTIMENTO\n",
        " 8-VAZAO MIN.    9-GER.HIDR.CONT   10-GER. TERMICA  11-INTERCAMBIOS    12-MERC.LIQ.    13-VALOR AGUA   14-VOLUME MORTO\n",
        "15-EXCESSO      16-GHMAX           17-OUTROS USOS   18-BENEF.INT/AGR   19-F.CORR.EC    20-GHTOTAL      21-ENA BRUTA\n",
        "22-ACOPLAMENTO  23-INVASAO CG      24-PENAL.INV.CG. 25-ACIONAMENTO MAR 26-COPER        27-CTERM        28-CDEFICIT\n",
        "29-GER.FIO LIQ. 30-PERDA FIO       31-ENA FIO LIQ.  32- BENEF. GNL     33-VIOL.GHMIN   34-PERDAS       37-GEE             38-SOMA AFL.PAS.\n",
        " XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX (SE 99 CONSIDERA TODAS)\n",
        " 99\n",
        "-----------------------------------------------------------------------------------------------------------------------\n",
        " 1-VOL.ARMAZ       2-GER.HID         3-VOL.TURB.     4-VOL. VERT.      5-VIOL.GHMIN    6-ENCH.MORTO   7-FOLGA DEPMIN.\n",
        " 8-DESV. AGUA      9-DESV. POS.      10-DESVIO NEG.  11-FOLGA FPGHA   12-VAZAO AFL.  13-VAZAO INCREM. 14-VARM PCT.\n",
        " XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX (SE 99 CONSIDERA TODAS)\n",
        " 99\n",
        " XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX  (SE 999 CONSIDERA TODAS AS USINAS)\n",
        " 999\n",
    ]
    with open("nwlistop.dat", "w") as arq:
        arq.writelines(linhas)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="executa programas auxiliares do NEWAVE"
    )
    parser.add_argument("executavel_nwlistcf", type=str)
    parser.add_argument("executavel_nwlistop", type=str)
    args = parser.parse_args()

    ti = time()

    # Executa o NWLISTCF para o 2º mês
    try:
        caso = Caso.read("./caso.dat")
        gera_nwlistcf_estagio(2)
        move(caso.arquivos, "arquivos_bkp.dat")
        gera_arquivosdat_nwlistcf()
        print(f"Executando: {args.executavel_nwlistcf}")
        cod, saida = asyncio.run(
            run_terminal([args.executavel_nwlistcf], timeout=600.0)
        )
        for linha in saida.split("\n"):
            print(linha)

    except Exception as e:
        print(f"Erro na execução do NWLISTCF: {str(e)}")
    finally:
        move("arquivos_bkp.dat", caso.arquivos)
    # Executa o NWLISTOP tabelas e médias
    try:
        gera_nwlistopdat_nwlistop(2)
        print(f"Executando: {args.executavel_nwlistop}")
        cod, saida = asyncio.run(
            run_terminal([args.executavel_nwlistop], timeout=600.0)
        )
        for linha in saida.split("\n"):
            print(linha)
        gera_nwlistopdat_nwlistop(4)
        print(f"Executando: {args.executavel_nwlistop}")
        cod, saida = asyncio.run(
            run_terminal([args.executavel_nwlistop], timeout=600.0)
        )
        for linha in saida.split("\n"):
            print(linha)
    except Exception as e:
        print(f"Erro na execução do NWLISTOP: {str(e)}")

    tf = time()
    print(
        f"Programas auxiliares do NEWAVE executados em {tf - ti:.2f} segundos!"
    )
