from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
from inewave.newave.dger import DGer
import asyncio
from shutil import move
import argparse
from traceback import print_exc


from tuber.utils import run_terminal_retry


def gera_arquivosdat_nwlistcf():
    linhas = [
        "ARQUIVO DE DADOS GERAIS     : nwlistcf.dat",
        "ARQUIVO DE CORTES DE BENDERS: cortes.dat",
        "ARQUIVO DE CABECALHO CORTES : cortesh.dat",
        "ARQUIVO P/DESPACHO HIDROTERM: newdesp.dat",
        "ARQUIVO DE ESTADOS CORTES   : cortese.dat",
        "ARQUIVO DE ENERGIAS FORWARD : energiaf.dat",
        "ARQUIVO DE RESTRICOES SAR   : rsar.dat",
        "ARQUIVO DE CABECALHO SAR    : rsarh.dat",
        "ARQUIVO DE INDICE SAR       : rsari.dat",
        "ARQUIVO LISTAGEM CORTES     : nwlistcf.rel",
        "ARQUIVO LISTAGEM ESTADOS FCF: estados.rel",
        "ARQUIVO LISTAGEM SAR        : rsar.rel",
        "ARQUIVO DE ENERGIAS X FORW  : energiaxf.dat",
        "ARQUIVO DE VAZAO FORWARD    : vazaof.dat",
        "ARQUIVO DE VAZAO X FORWARD  : vazaoxf.dat",
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
    with open("nwlistcf.dat", "w") as arq:
        arq.writelines(linhas_anteriores)
        arq.write(f"  {mes}  {mes} 1\n")
        arq.writelines(linhas_seguintes)


def gera_nwlistopdat_nwlistop(opcao: int):
    caso = Caso.read("./caso.dat")
    arquivos = Arquivos.read("./" + caso.arquivos)
    dger = DGer.read("./" + arquivos.dger)
    estagio_final = dger.num_anos_estudo * 12 - 11
    linhas = [
        f" {opcao}",
        "FORWARD  (ARQ. DE DADOS)    : forward.dat",
        "FORWARDH (ARQ. CABECALHOS)  : forwarh.dat",
        "NEWDESP  (REL. CONFIGS)     : newdesp.dat",
        "-----------------------------------------",
        " XXX XXX    PERIODOS INICIAL E FINAL",
        f"   1  {estagio_final}",
        " 1-CMO           2-DEFICITS         3-ENA CONTROL.   4-EARM FINAL       5-ENA FIO BRUTA 6-EVAPORACAO    7-VERTIMENTO",
        " 8-VAZAO MIN.    9-GER.HIDR.CONT   10-GER. TERMICA  11-INTERCAMBIOS    12-MERC.LIQ.    13-VALOR AGUA   14-VOLUME MORTO",
        "15-EXCESSO      16-GHMAX           17-OUTROS USOS   18-BENEF.INT/AGR   19-F.CORR.EC    20-GHTOTAL      21-ENA BRUTA",
        "22-ACOPLAMENTO  23-INVASAO CG      24-PENAL.INV.CG. 25-ACIONAMENTO MAR 26-COPER        27-CTERM        28-CDEFICIT",
        "29-GER.FIO LIQ. 30-PERDA FIO       31-ENA FIO LIQ.  32- BENEF. GNL     33-VIOL.GHMIN   34-PERDAS       37-GEE             38-SOMA AFL.PAS.",
        " XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX (SE 99 CONSIDERA TODAS)",
        " 99",
        "-----------------------------------------------------------------------------------------------------------------------",
        " 1-VOL.ARMAZ       2-GER.HID         3-VOL.TURB.     4-VOL. VERT.      5-VIOL.GHMIN    6-ENCH.MORTO   7-FOLGA DEPMIN.",
        " 8-DESV. AGUA      9-DESV. POS.      10-DESVIO NEG.  11-FOLGA FPGHA   12-VAZAO AFL.  13-VAZAO INCREM. 14-VARM PCT.",
        " XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX (SE 99 CONSIDERA TODAS)",
        " 99",
        " XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX  (SE 999 CONSIDERA TODAS AS USINAS)",
        " 999",
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

    # Executa o NWLISTCF para o 2º mês
    try:
        move("arquivos.dat", "arquivos_bkp.dat")
        gera_arquivosdat_nwlistcf()
        gera_nwlistcf_estagio(2)
        asyncio.run(run_terminal_retry(args.executavel_nwlistcf))
    except Exception as e:
        print_exc()
        print(f"Erro na execução do NWLISTCF: {str(e)}")
    finally:
        move("arquivos_bkp.dat", "arquivos.dat")
    # Executa o NWLISTOP tabelas e médias
    try:
        gera_nwlistopdat_nwlistop(2)
        asyncio.run(run_terminal_retry(args.executavel_nwlistop))
        gera_nwlistopdat_nwlistop(4)
        asyncio.run(run_terminal_retry(args.executavel_nwlistop))
    except Exception as e:
        print_exc()
        print(f"Erro na execução do NWLISTOP: {str(e)}")
