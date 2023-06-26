from idecomp.decomp.caso import Caso
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.dadger import Dadger
import pandas as pd
from time import time


from tuber.utils import (
    identifica_arquivos_via_regex,
    zip_arquivos,
    limpa_arquivos_saida,
)

EXTENSAO = Caso.read("./caso.dat").arquivos

if __name__ == "__main__":
    ti = time()

    def identifica_arquivos_entrada():
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
            pd.read_csv(arquivo_indice[0], delimiter=";", header=None)[2]
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
            + [a.strip() for a in arquivos_libs]
        )

        return arquivos_entrada

    # Zipar deck de entrada
    arquivos_entrada = identifica_arquivos_entrada()
    zip_arquivos(arquivos_entrada, "deck")

    # Zipar csvs de saida com resultados da operação
    arquivos_saida_csv_regex = [
        ["bengnl", r"^", r".*\.csv"],
        ["dec_oper", r"^", r".*\.csv"],
        ["energia_acopla", r"^", r".*\.csv"],
        ["balsub", r"^", r".*\.csv"],
        ["cei", r"^", r".*\.csv"],
        ["cmar", r"^", r".*\.csv"],
        ["contratos", r"^", r".*\.csv"],
        ["ener", r"^", r".*\.csv"],
        ["ever", r"^", r".*\.csv"],
        ["evnt", r"^", r".*\.csv"],
        ["flx", r"^", r".*\.csv"],
        ["hidrpat", r"^", r".*\.csv"],
        ["pdef", r"^", r".*\.csv"],
        ["qnat", r"^", r".*\.csv"],
        ["qtur", r"^", r".*\.csv"],
        ["term", r"^", r".*\.csv"],
        ["usina", r"^", r".*\.csv"],
        ["ute", r"^", r".*\.csv"],
        ["vert", r"^", r".*\.csv"],
        ["vutil", r"^", r".*\.csv"],
    ]
    arquivos_saida_operacao = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_csv_regex
    )
    zip_arquivos(arquivos_saida_operacao, "operacao")

    # Zipar demais relatorios de saída
    arquivos_saida_relatorios = [
        "decomp.tim",
        "relato." + EXTENSAO,
        "sumario." + EXTENSAO,
        "relato2." + EXTENSAO,
        "inviab_unic." + EXTENSAO,
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
        "avl_turb_max.csv",
        "dec_avl_evap.csv",
        "dec_cortes_evap.csv",
        "dec_estatevap.csv",
        "memcal." + EXTENSAO,
        "fcfnwi." + EXTENSAO,
        "fcfnwn." + EXTENSAO,
        "cmdeco." + EXTENSAO,
    ]
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
        ["dimpl_", "", ""],
        ["osl_", "", ""],
        ["cad", "", ""],
        ["debug", "", ""],
        ["debug", "", ""],
        ["inviab_0", "", ""],
        ["svc", "", ""],
        ["deco_", "", r".*\.msg"],
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
