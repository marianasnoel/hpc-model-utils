from idessem.dessem.dessemarq import DessemArq
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.dadger import Dadger
import pandas as pd
from time import time
import os


from tuber.utils import (
    identifica_arquivos_via_regex,
    zip_arquivos,
    limpa_arquivos_saida,
)

if __name__ == "__main__":
    ti = time()

    dessem_arq = DessemArq.read("./dessem.arq")
    EXTENSAO = dessem_arq.caso

    def identifica_arquivos_entrada():
        arquivos_gerais = [
            dessem_arq.vazoes.valor,
            dessem_arq.dadger.valor,
            dessem_arq.mapfcf.valor,
            dessem_arq.cortfcf.valor,
            dessem_arq.cadusih.valor,
            dessem_arq.operuh.valor,
            dessem_arq.deflant.valor,
            dessem_arq.cadterm.valor,
            dessem_arq.operut.valor,
            dessem_arq.indelet.valor,
            dessem_arq.ilstri.valor,
            dessem_arq.cotasr11.valor,
            dessem_arq.areacont.valor,
            dessem_arq.respot.valor,
            dessem_arq.mlt.valor,
            dessem_arq.curvtviag.valor,
            dessem_arq.ptoper.valor,
            dessem_arq.infofcf.valor,
            dessem_arq.ree.valor,
            dessem_arq.eolica.valor,
            dessem_arq.rampas.valor,
            dessem_arq.rstlpp.valor,
            dessem_arq.restseg.valor,
            dessem_arq.respotele.valor,
            dessem_arq.uch.valor,
        ]
        arquivo_indice = (
            [dessem_arq.ilibs.valor] if dessem_arq.ilibs is not None else []
        )
        arquivos_libs = (
            pd.read_csv(arquivo_indice[0], delimiter=";", header=None)[2]
            .unique()
            .tolist()
            if len(arquivo_indice) == 1
            else []
        )
        # TODO - obter os arquivos de rede de maneira dinâmica
        arquivos_rede = [
            a for a in os.listdir(os.curdir) if all(["pat" in a, ".afp" in a])
        ] + [a for a in os.listdir(os.curdir) if ".pwf" in a]

        arquivos_entrada = (
            [a for a in arquivos_gerais if len(a) > 0]
            + arquivo_indice
            + ["dessem.arq"]
            + [a.strip() for a in arquivos_libs]
            + arquivos_rede
        )

        return arquivos_entrada

    # Zipar deck de entrada
    arquivos_entrada = identifica_arquivos_entrada()
    zip_arquivos(arquivos_entrada, "deck")

    # Zipar csvs de saida com resultados da operação
    # TODO - terminar de adicionar os outros PDO e arquivos
    # de saída de operação com regex
    arquivos_saida_csv_regex = [
        ["PDO_OPER_", r"^", r".*"],
    ]
    arquivos_saida_operacao = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_csv_regex
    )
    # TODO - adicionar arquivos de saída de operação sem regex
    arquivos_saida_operacao += []
    zip_arquivos(arquivos_saida_operacao, "operacao")

    # Zipar demais relatorios de saída
    arquivos_saida_relatorios_regex = [
        ["AVL_", r"^", r".*"],
        ["DES_", r"^", r".*"],
        ["LOG_", r"^", r".*"],
        ["PDO_AVAL", r"^", r".*"],
        ["PDO_ECO", r"^", r".*"],
        ["PTOPER_", r"^", r".*\.PWF"],
    ]
    arquivos_saida_relatorios = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_csv_regex
    )
    # TODO - conferir se existem outros arquivos, sem regex, a serem
    # incluídos como relatórios
    arquivos_saida_relatorios += []
    zip_arquivos(arquivos_saida_relatorios, "relatorios")

    # Apagar arquivos para limpar diretório pós execução com sucesso
    # TODO - atualizar com arquivos que devem ser mantidos
    arquivos_manter = arquivos_entrada + [
        "DES_LOG_RELATO" + EXTENSAO,
    ]
    arquivos_zipados = (
        arquivos_entrada + arquivos_saida_operacao + arquivos_saida_relatorios
    )
    arquivos_limpar = [a for a in arquivos_zipados if a not in arquivos_manter]
    limpa_arquivos_saida(arquivos_limpar)

    # Apagar arquivos temporários para limpar diretório pós execução incompleta/inviavel
    # TODO - adicionar regex de arquivos a serem apagados
    # mesmo que não tenham sido zipados.
    arquivos_apagar_regex = [
        ["dimpl_", "", ""],
        ["osl_", "", ""],
        ["cad", "", ""],
        ["debug", "", ""],
        ["debug", "", ""],
        ["inviab_0", "", ""],
        ["svc", "", ""],
        ["deco_", "", r".*\.msg"],
        ["SAIDA_MENSAGENS", "", ""],
        ["vazmsg", "", ""],
    ]
    arquivos_apagar = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_apagar_regex
    ) + [
        "decomp.lic",
        "cusfut." + EXTENSAO,
        "deconf." + EXTENSAO,
        "CONVERG.TMP",
    ]  # TODO - adicionar arquivos sem regex para apagar, mesmo que não
    # tenham sido zipados.
    limpa_arquivos_saida(arquivos_apagar)

    tf = time()
    print(f"Pós-processamento do DESSEM feito em {tf - ti:.2f} segundos!")
