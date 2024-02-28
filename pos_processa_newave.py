from inewave.newave.caso import Caso
from inewave.newave.arquivos import Arquivos
import pandas as pd
from os import listdir
import argparse

from tuber.utils import (
    identifica_arquivos_via_regex,
    zip_arquivos,
    zip_arquivos_paralelo,
    limpa_arquivos_saida,
)
from time import time


caso = Caso.read("./caso.dat")
arquivos = Arquivos.read("./" + caso.arquivos)


def identifica_arquivos_entrada():
    caso = Caso.read("./caso.dat")
    arquivos = Arquivos.read("./" + caso.arquivos)
    arquivos_gerais = [
        "caso.dat",
        caso.arquivos,
        arquivos.adterm,
        arquivos.agrint,
        arquivos.c_adic,
        arquivos.cvar,
        arquivos.sar,
        arquivos.clast,
        arquivos.confhd,
        arquivos.conft,
        arquivos.curva,
        arquivos.dger,
        arquivos.dsvagua,
        arquivos.vazpast,
        arquivos.exph,
        arquivos.expt,
        arquivos.ghmin,
        arquivos.gtminpat,
        "hidr.dat",
        arquivos.perda,
        arquivos.manutt,
        arquivos.modif,
        arquivos.patamar,
        arquivos.penalid,
        "postos.dat",
        arquivos.shist,
        arquivos.sistema,
        arquivos.term,
        "vazoes.dat",
        arquivos.tecno,
        "selcor.dat",
        arquivos.re,
        arquivos.ree,
        arquivos.clasgas,
        arquivos.abertura,
        arquivos.gee,
        "dbgcortes.dat",
        "volref_saz.dat",
        arquivos.cortesh_pos_estudo,
        arquivos.cortes_pos_estudo,
    ]
    arquivo_indice = ["indices.csv"] if "indices.csv" in listdir() else []
    arquivos_libs = (
        pd.read_csv(arquivo_indice[0], delimiter=";", header=None)[2]
        .unique()
        .tolist()
        if len(arquivo_indice) == 1
        else []
    )

    arquivos_entrada = (
        arquivos_gerais + arquivo_indice + [a.strip() for a in arquivos_libs]
    )

    return arquivos_entrada


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="realiza o pos processamento do NEWAVE"
    )
    parser.add_argument("numero_processadores", type=int, default=8)
    parser.add_argument("-ppq", "--pseudopartidaquente", action="store_true")
    args = parser.parse_args()

    if args.pseudopartidaquente:
        print(
            "Rodada de Pseudo Partida Quente (PPQ)."
            + " Pós-processamento do NEWAVE cancelado."
        )
        exit(0)

    ti = time()

    # Zipar deck de entrada
    arquivos_entrada = identifica_arquivos_entrada()
    zip_arquivos(arquivos_entrada, "deck")

    # Zipar csvs de saida com resultados da operação
    arquivos_saida_nwlistop = [
        ["", "", r".*\.CSV"],
        ["", "", r".*\.out"],

    ]
    arquivos_saida_nwlistop = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_nwlistop
    ) + ["nwlistop.dat"]
    print(arquivos_saida_nwlistop)
    zip_arquivos_paralelo(
        arquivos_saida_nwlistop, "operacao", args.numero_processadores
    )

    # Zipar demais relatorios de saída
    arquivos_saida_relatorios = [
        ["alertainv", "^", r".*\.rel"],
        ["cativo_", "^", r".*\.rel"],
        ["avl_desvfpha", "^", r".*\.dat"],
        ["avl_desvfpha", "^", r".*\.csv"],
        ["newave_", "^", r".*\.log"],
        ["nwv_", "^", r".*\.rel"],
        ["", "", r"newave\.tim"],
        ["", "", r"avl_cortesfpha_nwv\.dat"],
        ["", "", r"avl_cortesfpha_nwv\.csv"],
        ["", "", r"nwv_avl_evap\.csv"],
        ["", "", r"nwv_cortes_evap\.csv"],
        ["", "", r"nwv_eco_evap\.csv"],
        ["", "", r"boots\.rel"],
        ["", "", r"consultafcf\.rel"],
        ["", "", r"eco_fpha_\.dat"],
        ["", "", r"eco_fpha\.csv"],
        ["", "", r"parpeol\.dat"],
        ["", "", r"parpvaz\.dat"],
        ["", "", r"runtrace\.dat"],
        ["", "", r"runstate\.dat"],
        ["", "", r"prociter\.rel"],
        ["", "", r"CONVERG\.TMP"],
        ["","",r"ETAPA\.TMP"]
    ]
    arquivos_saida_relatorios = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_relatorios
    )
    arquivos_saida_relatorios += [
        arquivos.pmo,
        arquivos.parp,
        arquivos.dados_simulacao_final,
    ]
    zip_arquivos_paralelo(
        arquivos_saida_relatorios, "relatorios", args.numero_processadores
    )

    # Zipar recursos
    arquivos_saida_recursos = [
        ["energiaf", "^", r".*\.dat"],
        ["energiaaf", "^", r".*\.dat"],
        ["energiab", "^", r".*\.dat"],
        ["energiaxf", "^", r".*\.dat"],
        ["energias", "^", r".*\.dat"],
        ["energiaxs", "^", r".*\.dat"],
        ["energiap", "^", r".*\.dat"],
        ["energiaas", "^", r".*\.csv"],
        ["energiaasx", "^", r".*\.csv"],
        ["energiaf", "^", r".*\.csv"],
        ["energiaaf", "^", r".*\.csv"],
        ["energiax", "^", r".*\.csv"],
        ["energiaxf", "^", r".*\.csv"],
        ["energiaxs", "^", r".*\.csv"],
        ["energiap", "^", r".*\.csv"],
        ["eng", "^", r".*\.dat"],
        ["enavazf", "^", r".*\.dat"],
        ["enavazxf", "^", r".*\.dat"],
        ["enavazxs", "^", r".*\.dat"],
        ["enavazb", "^", r".*\.dat"],
        ["enavazs", "^", r".*\.dat"],
        ["enavazf", "^", r".*\.csv"],
        ["enavazxf", "^", r".*\.csv"],
        ["enavazaf", "^", r".*\.csv"],
        ["enavazb", "^", r".*\.csv"],
        ["enavazs", "^", r".*\.csv"],
        ["vazaof", "^", r".*\.dat"],
        ["vazaoaf", "^", r".*\.dat"],
        ["vazaob", "^", r".*\.dat"],
        ["vazaos", "^", r".*\.dat"],
        ["vazaoas", "^", r".*\.dat"],
        ["vazaoxs", "^", r".*\.dat"],
        ["vazaoxf", "^", r".*\.dat"],
        ["vazaop", "^", r".*\.dat"],
        ["vazaof", "^", r".*\.csv"],
        ["vazaoaf", "^", r".*\.csv"],
        ["vazaob", "^", r".*\.csv"],
        ["vazaos", "^", r".*\.csv"],
        ["vazthd", "^", r".*\.dat"],
        ["vazinat", "^", r".*\.dat"],
        ["ventos", "^", r".*\.dat"],
        ["vento", "^", r".*\.csv"],
        ["eolicaf", "^", r".*\.dat"],
        ["eolicab", "^", r".*\.dat"],
        ["eolicas", "^", r".*\.dat"],
        ["eolp", "^", r".*\.dat"],
        ["eolf", "^", r".*\.csv"],
        ["eolb", "^", r".*\.csv"],
        ["eolp", "^", r".*\.csv"],
        ["eols", "^", r".*\.csv"],
    ]
    arquivos_saida_recursos = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_recursos
    )
    zip_arquivos_paralelo(
        arquivos_saida_recursos, "recursos", args.numero_processadores
    )

    # Zipar cortes e cabeçalhos
    arquivos_saida_cortes = [
        ["cortes\-[0-9]*", "^", r".*\.dat"],
    ]
    arquivos_saida_cortes = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_cortes
    )
    arquivos_saida_cortes += [
        arquivos.cortesh,
        arquivos.cortes,
        "nwlistcf.rel",
    ]
    zip_arquivos_paralelo(
        arquivos_saida_cortes, "cortes", args.numero_processadores
    )

    # Zipar estados de construção dos cortes
    arquivos_saida_estados = [
        ["cortese\-[0-9]*", "^", r".*\.dat"],
    ]
    arquivos_saida_estados = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_estados
    )
    arquivos_saida_estados += ["cortese.dat", "estados.rel"]
    zip_arquivos_paralelo(
        arquivos_saida_estados, "estados", args.numero_processadores
    )

    # Zipar arquivos de simulação
    arquivos_saida_simulacao = [
        arquivos.forward,
        arquivos.forwardh,
        arquivos.newdesp,
        "planej.dat",
    ]
    zip_arquivos_paralelo(
        arquivos_saida_simulacao, "simulacao", args.numero_processadores
    )

    # Apagar arquivos para limpar diretório pós execução com sucesso
    arquivos_manter = arquivos_entrada + [
        "newave.tim",
        arquivos.pmo,
        arquivos.dados_simulacao_final,
    ]
    arquivos_zipados = (
        arquivos_entrada
        + arquivos_saida_nwlistop
        + arquivos_saida_relatorios
        + arquivos_saida_recursos
        + arquivos_saida_cortes
        + arquivos_saida_estados
        + arquivos_saida_simulacao
    )
    arquivos_limpar = [a for a in arquivos_zipados if a not in arquivos_manter]
    limpa_arquivos_saida(arquivos_limpar)

    # Apagar arquivos temporários para limpar diretório pós execução
    arquivos_apagar_regex = [
        ["svc", "", ""],
    ]
    arquivos_apagar = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_apagar_regex
    ) + [
        "nwlistcf.dat",
        "nwlistop.dat",
        "format.tmp",
        "mensag.tmp",
        "NewaveMsgPortug.txt",
        "ConvNomeArqsDados.log",
        "ETAPA.TMP",
        "LEITURA.TMP",
    ]
    limpa_arquivos_saida(arquivos_apagar)

    tf = time()
    print(f"Pós-processamento do NEWAVE feito em {tf - ti:.2f} segundos!")
