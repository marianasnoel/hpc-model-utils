from os import listdir
from time import time

import click
import pandas as pd  # type: ignore
from inewave.newave.arquivos import Arquivos
from inewave.newave.caso import Caso

from tuber.utils import (
    identifica_arquivos_via_regex,
    limpa_arquivos_saida,
    traz_conteudo_para_raiz,
    zip_arquivos,
    zip_arquivos_paralelo,
)


@click.command("pos_processa_newave")
@click.argument("numero_processadores", type=int)
@click.option("-ppq", is_flag=True)
def pos_processa_newave(numero_processadores, ppq):
    caso = Caso.read("./caso.dat")
    arquivos = Arquivos.read("./" + caso.arquivos)

    def identifica_arquivos_entrada() -> list[str]:
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
            pd.read_csv(
                arquivo_indice[0], delimiter=";", comment="&", header=None
            )[2]
            .unique()
            .tolist()
            if len(arquivo_indice) == 1
            else []
        )

        arquivos_entrada = (
            arquivos_gerais
            + arquivo_indice
            + [a.strip() for a in arquivos_libs]
        )
        arquivos_entrada = [a for a in arquivos_entrada if a is not None]

        return arquivos_entrada

    if ppq:
        print(
            "Rodada de Pseudo Partida Quente (PPQ)."
            + " Pós-processamento do NEWAVE cancelado."
        )
        exit(0)

    ti = time()

    # Zipar deck de entrada
    arquivos_entrada = identifica_arquivos_entrada()
    zip_arquivos(arquivos_entrada, "deck")

    # Traz arquivos LIBS e de outros diretorios para a raiz
    for d in ["out", "evaporacao", "fpha", "log"]:
        traz_conteudo_para_raiz(d)

    # Zipar csvs de saida com resultados da operação
    regex_arquivos_saida_nwlistop = [
        r"^.*\.CSV$",
        r"^.*\.out$",
    ]
    arquivos_saida_nwlistop = ["nwlistop.dat"]
    arquivos_saida_nwlistop += identifica_arquivos_via_regex(
        arquivos_entrada, regex_arquivos_saida_nwlistop
    )
    zip_arquivos_paralelo(
        arquivos_saida_nwlistop,
        "operacao",
        numero_processadores,
    )

    # Zipar demais relatorios de saída
    arquivos_saida_relatorios = [
        arquivos.pmo,
        arquivos.parp,
        arquivos.dados_simulacao_final,
        "newave.tim",
        "nwv_avl_evap.csv",
        "nwv_cortes_evap.csv",
        "nwv_eco_evap.csv",
        "evap_avl_desc.csv",
        "evap_eco.csv",
        "evap_cortes.csv",
        "boots.rel",
        "consultafcf.rel",
        "eco_fpha_.dat",
        "eco_fpha.csv",
        "fpha_eco.csv",
        "fpha_cortes.csv",
        "avl_cortesfpha_nwv.dat",
        "avl_cortesfpha_nwv.csv",
        "parpeol.dat",
        "parpvaz.dat",
        "runtrace.dat",
        "runstate.dat",
        "prociter.rel",
        "CONVERG.TMP",
        "ETAPA.TMP",
    ]
    arquivos_saida_relatorios = [
        a for a in arquivos_saida_relatorios if a is not None
    ]
    regex_arquivos_saida_relatorios = [
        r"^alertainv.*\.rel$",
        r"^cativo_.*\.rel$",
        r"^avl_desvfpha.*\.dat$",
        r"^avl_desvfpha.*\.csv$",
        r"^newave_.*\.log$",
        r"^nwv_.*\.rel$",
    ]
    arquivos_saida_relatorios += identifica_arquivos_via_regex(
        arquivos_entrada, regex_arquivos_saida_relatorios
    )
    zip_arquivos_paralelo(
        arquivos_saida_relatorios, "relatorios", numero_processadores
    )

    # Zipar recursos
    regex_arquivos_saida_recursos = [
        r"^energiaf.*\.dat$",
        r"^energiaaf.*\.dat$",
        r"^energiab.*\.dat$",
        r"^energiaxf.*\.dat$",
        r"^energias.*\.dat$",
        r"^energiaxs.*\.dat$",
        r"^energiap.*\.dat$",
        r"^energiaas.*\.csv$",
        r"^energiaasx.*\.csv$",
        r"^energiaf.*\.csv$",
        r"^energiaaf.*\.csv$",
        r"^energiax.*\.csv$",
        r"^energiaxf.*\.csv$",
        r"^energiaxs.*\.csv$",
        r"^energiap.*\.csv$",
        r"^eng.*\.dat$",
        r"^enavazf.*\.dat$",
        r"^enavazxf.*\.dat$",
        r"^enavazxs.*\.dat$",
        r"^enavazb.*\.dat$",
        r"^enavazs.*\.dat$",
        r"^enavazf.*\.csv$",
        r"^enavazxf.*\.csv$",
        r"^enavazaf.*\.csv$",
        r"^enavazb.*\.csv$",
        r"^enavazs.*\.csv$",
        r"^vazaof.*\.dat$",
        r"^vazaoaf.*\.dat$",
        r"^vazaob.*\.dat$",
        r"^vazaos.*\.dat$",
        r"^vazaoas.*\.dat$",
        r"^vazaoxs.*\.dat$",
        r"^vazaoxf.*\.dat$",
        r"^vazaop.*\.dat$",
        r"^vazaof.*\.csv$",
        r"^vazaoaf.*\.csv$",
        r"^vazaob.*\.csv$",
        r"^vazaos.*\.csv$",
        r"^vazthd.*\.dat$",
        r"^vazinat.*\.dat$",
        r"^ventos.*\.dat$",
        r"^vento.*\.csv$",
        r"^eolicaf.*\.dat$",
        r"^eolicab.*\.dat$",
        r"^eolicas.*\.dat$",
        r"^eolp.*\.dat$",
        r"^eolf.*\.csv$",
        r"^eolb.*\.csv$",
        r"^eolp.*\.csv$",
        r"^eols.*\.csv$",
    ]
    arquivos_saida_recursos = identifica_arquivos_via_regex(
        arquivos_entrada, regex_arquivos_saida_recursos
    )
    zip_arquivos_paralelo(
        arquivos_saida_recursos, "recursos", numero_processadores
    )

    # Zipar cortes e cabeçalhos
    arquivos_saida_cortes = [
        arquivos.cortesh,
        arquivos.cortes,
        "arquivos-nwlistcf.dat",
        "nwlistcf.rel",
    ]
    arquivos_saida_cortes += identifica_arquivos_via_regex(
        arquivos_entrada, [r"^cortes\-[0-9]*.*\.dat$"]
    )
    arquivos_saida_cortes = [a for a in arquivos_saida_cortes if a is not None]
    zip_arquivos_paralelo(arquivos_saida_cortes, "cortes", numero_processadores)

    # Zipar estados de construção dos cortes
    arquivos_saida_estados = ["cortese.dat", "estados.rel"]
    arquivos_saida_estados += identifica_arquivos_via_regex(
        arquivos_entrada, [r"^cortese\-[0-9]*.*\.dat$"]
    )
    zip_arquivos_paralelo(
        arquivos_saida_estados, "estados", numero_processadores
    )

    # Zipar arquivos de simulação
    arquivos_saida_simulacao = [
        arquivos.forward,
        arquivos.forwardh,
        arquivos.newdesp,
        "planej.dat",
        "daduhe.dat",
        "nwdant.dat",
        "saida.rel",
    ]
    arquivos_saida_simulacao = [
        a for a in arquivos_saida_simulacao if a is not None
    ]
    zip_arquivos_paralelo(
        arquivos_saida_simulacao, "simulacao", numero_processadores
    )

    # Apagar arquivos para limpar diretório pós execução com sucesso
    arquivos_manter = arquivos_entrada + [
        "newave.tim",
        arquivos.pmo,
        arquivos.dados_simulacao_final,
    ]
    arquivos_manter = [a for a in arquivos_manter if a is not None]
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
        r"^svc.*$",
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
