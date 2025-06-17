from time import time

import click
import pandas as pd  # type: ignore
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.caso import Caso
from idecomp.decomp.dadger import Dadger

from app.utils import (
    identifica_arquivos_via_regex,
    limpa_arquivos_saida,
    traz_conteudo_para_raiz,
    zip_arquivos,
)


@click.command("pos_processa_decomp")
def pos_processa_decomp():
    EXTENSAO: str = Caso.read("./caso.dat").arquivos

    ti = time()

    def identifica_arquivos_entrada() -> list[str]:
        arquivos = Arquivos.read("./" + EXTENSAO)
        arquivos_gerais = [
            arquivos.dadger,
            arquivos.vazoes,
            arquivos.hidr,
            arquivos.mlt,
            arquivos.perdas,
            arquivos.dadgnl,
        ]
        dadger = Dadger.read("./" + arquivos.dadger)
        arquivo_indice = [dadger.fa.arquivo] if dadger.fa is not None else []
        arquivo_polinjusdat = (
            [dadger.fj.arquivo] if dadger.fj is not None else []
        )
        arquivo_velocidade = (
            [dadger.vt.arquivo] if dadger.vt is not None else []
        )
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
            [a for a in arquivos_gerais if len(a) > 0]
            + arquivo_indice
            + arquivo_polinjusdat
            + arquivo_velocidade
            + ["caso.dat", EXTENSAO]
            + [a.strip() for a in arquivos_libs]
        )

        arquivos_entrada = [a for a in arquivos_entrada if a is not None]

        return arquivos_entrada

    # Zipar deck de entrada
    arquivos_entrada = identifica_arquivos_entrada()
    zip_arquivos(arquivos_entrada, "deck")

    # Traz arquivos LIBS para a raiz
    traz_conteudo_para_raiz("out")

    # Zipar csvs de saida com resultados da operação
    regex_arquivos_saida_csv = [
        r"^bengnl.*\.csv$",
        r"^dec_oper.*\.csv$",
        r"^energia_acopla.*\.csv$",
        r"^balsub.*\.csv$",
        r"^cei.*\.csv$",
        r"^cmar.*\.csv$",
        r"^contratos.*\.csv$",
        r"^ener.*\.csv$",
        r"^ever.*\.csv$",
        r"^evnt.*\.csv$",
        r"^flx.*\.csv$",
        r"^hidrpat.*\.csv$",
        r"^pdef.*\.csv$",
        r"^qnat.*\.csv$",
        r"^qtur.*\.csv$",
        r"^term.*\.csv$",
        r"^usina.*\.csv$",
        r"^ute.*\.csv$",
        r"^vert.*\.csv$",
        r"^vutil.*\.csv$",
        r"^oper_.*\.csv$",
    ]
    arquivos_saida_operacao = identifica_arquivos_via_regex(
        arquivos_entrada, regex_arquivos_saida_csv
    )
    zip_arquivos(arquivos_saida_operacao, "operacao")

    # Zipar demais relatorios de saída
    arquivos_saida_relatorios = [
        "decomp.tim",
        "relato." + EXTENSAO,
        "sumario." + EXTENSAO,
        "relato2." + EXTENSAO,
        "inviab_unic." + EXTENSAO,
        "inviab." + EXTENSAO,
        "relgnl." + EXTENSAO,
        "custos." + EXTENSAO,
        "avl_cortesfpha_dec." + EXTENSAO,
        "dec_desvfpha." + EXTENSAO,
        "dec_estatfpha." + EXTENSAO,
        "energia." + EXTENSAO,
        "log_desvfpha_dec." + EXTENSAO,
        "outgnl." + EXTENSAO,
        "memcal." + EXTENSAO,
        "runstate.dat",
        "runtrace.dat",
        "eco_fpha_." + EXTENSAO,
        "dec_eco_desvioagua.csv",
        "dec_eco_discr.csv",
        "dec_eco_evap.csv",
        "dec_eco_qlat.csv",
        "dec_eco_cotajus.csv",
        "avl_turb_max.csv",
        "dec_avl_evap.csv",
        "dec_cortes_evap.csv",
        "dec_estatevap.csv",
        "fcfnwi." + EXTENSAO,
        "fcfnwn." + EXTENSAO,
        "cmdeco." + EXTENSAO,
        "indice_saida.csv",
        "mensagens.csv",
        "mensagensErro.txt",
    ]
    regex_arquivos_relatorios = [
        r"^osl_.*$",
        r"^eco_.*\.csv$",
        r"^dec_fcf_cortes.*$",
        r"^avl_desvfpha_v_q_.*$",
        r"^avl_desvfpha_s_.*$",
    ]
    arquivos_saida_relatorios += identifica_arquivos_via_regex(
        arquivos_entrada, regex_arquivos_relatorios
    )
    zip_arquivos(arquivos_saida_relatorios, "relatorios")

    # Zipar cortdeco e mapcut
    arquivos_saida_cortes = [
        "cortdeco." + EXTENSAO,
        "mapcut." + EXTENSAO,
    ]
    zip_arquivos(arquivos_saida_cortes, "cortes")

    # Apagar arquivos para limpar diretório pós execução com sucesso
    arquivos_manter = arquivos_entrada + [
        "decomp.tim",
        "relato." + EXTENSAO,
        "sumario." + EXTENSAO,
        "relato2." + EXTENSAO,
        "inviab_unic." + EXTENSAO,
        "inviab." + EXTENSAO,
        "relgnl." + EXTENSAO,
        "custos." + EXTENSAO,
        "dec_oper_usih.csv",
        "dec_oper_usit.csv",
        "dec_oper_ree.csv",
    ]
    arquivos_zipados = (
        arquivos_entrada
        + arquivos_saida_operacao
        + arquivos_saida_relatorios
        + arquivos_saida_cortes
    )
    arquivos_limpar = [a for a in arquivos_zipados if a not in arquivos_manter]
    limpa_arquivos_saida(arquivos_limpar)

    # Apagar arquivos temporários para limpar diretório pós execução incompleta/inviavel
    arquivos_apagar_regex = [
        r"^dimpl_.*$",
        r"^cad.*$",
        r"^debug.*$",
        r"^inviab_0.*$",
        r"^svc.*$",
        r"^deco_.*\.msg$",
        r"^SAIDA_MENSAGENS.*$",
        r"^vazmsg.*$",
    ]
    arquivos_apagar = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_apagar_regex
    ) + [
        "decomp.lic",
        "cusfut." + EXTENSAO,
        "deconf." + EXTENSAO,
        "CONVERG.TMP",
    ]
    limpa_arquivos_saida(arquivos_apagar)

    tf = time()
    print(f"Pós-processamento do DECOMP feito em {tf - ti:.2f} segundos!")
