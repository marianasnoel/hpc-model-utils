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
    EXTENSAO = dessem_arq.caso.valor

    def identifica_arquivos_entrada():
        registros_arquivos_gerais = [
            dessem_arq.vazoes,
            dessem_arq.dadger,
            dessem_arq.mapfcf,
            dessem_arq.cortfcf,
            dessem_arq.cadusih,
            dessem_arq.operuh,
            dessem_arq.deflant,
            dessem_arq.cadterm,
            dessem_arq.operut,
            dessem_arq.indelet,
            dessem_arq.ilstri,
            dessem_arq.cotasr11,
            dessem_arq.areacont,
            dessem_arq.respot,
            dessem_arq.mlt,
            dessem_arq.curvtviag,
            dessem_arq.ptoper,
            dessem_arq.infofcf,
            dessem_arq.ree,
            dessem_arq.eolica,
            dessem_arq.rampas,
            dessem_arq.rstlpp,
            dessem_arq.restseg,
            dessem_arq.respotele,
            dessem_arq.uch,
        ]
        arquivos_gerais = []
        for reg in registros_arquivos_gerais:
            if reg is not None:
                arquivos_gerais.append(reg.valor)

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

        arquivos_entrada = list(set(arquivos_entrada))

        return arquivos_entrada

    # Zipar deck de entrada
    arquivos_entrada = identifica_arquivos_entrada()
    zip_arquivos(arquivos_entrada, "deck")

    # Zipar csvs de saida com resultados da operação
    arquivos_saida_csv_regex = [
        ["PDO_OPER", r"^", r".*"],
        ["PDO_AVAL_", r"^", r".*"],
        ["PDO_CMO", r"^", r".*"],
        ["PDO_CONTR", r"^", r".*"],
        ["PDO_DESV", r"^", r".*"],
        ["PDO_ELEV", r"^", r".*"],
        ["PDO_EOLICA", r"^", r".*"],
        ["PDO_FLUXLIN", r"^", r".*"],
        ["PDO_GERBARR", r"^", r".*"],
        ["PDO_HIDR", r"^", r".*"],
        ["PDO_INTER", r"^", r".*"],
        ["PDO_RESERVA", r"^", r".*"],
        ["PDO_REST", r"^", r".*"],
        ["PDO_SIST", r"^", r".*"],
        ["PDO_SOMFLUX", r"^", r".*"],
        ["PDO_STATREDE_ITER", r"^", r".*"],
        ["PDO_SUMAOPER", r"^", r".*"],
        ["PDO_TERM", r"^", r".*"],
        ["PDO_VAGUA", r"^", r".*"],
        ["PDO_VERT", r"^", r".*"],
    ]
    arquivos_saida_operacao = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_saida_csv_regex
    )
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
        arquivos_entrada, arquivos_saida_relatorios_regex
    )
    zip_arquivos(arquivos_saida_relatorios, "relatorios")

    # Apagar arquivos para limpar diretório pós execução com sucesso
    arquivos_manter = arquivos_entrada + [
        "DES_LOG_RELATO." + EXTENSAO,
        "PDO_CMOBAR." + EXTENSAO,
        "PDO_CMOSIST." + EXTENSAO,
        "PDO_CONTR." + EXTENSAO,
        "PDO_EOLICA." + EXTENSAO,
        "PDO_HIDR." + EXTENSAO,
        "PDO_OPER_CONTR." + EXTENSAO,
        "PDO_OPER_TERM." + EXTENSAO,
        "PDO_OPER_TITULACAO_CONTRATOS." + EXTENSAO,
        "PDO_OPER_TITULACAO_USINAS." + EXTENSAO,
        "PDO_OPERACAO." + EXTENSAO,
        "PDO_SIST." + EXTENSAO,
        "PDO_SOMFLUX." + EXTENSAO,
        "PDO_SUMAOPER." + EXTENSAO,
        "PDO_TERM." + EXTENSAO,
    ]
    arquivos_zipados = (
        arquivos_entrada + arquivos_saida_operacao + arquivos_saida_relatorios
    )
    arquivos_limpar = [a for a in arquivos_zipados if a not in arquivos_manter]
    limpa_arquivos_saida(arquivos_limpar)

    # Apagar arquivos temporários para limpar diretório pós execução incompleta/inviavel
    # mesmo que não tenham sido zipados.
    arquivos_apagar_regex = [
        ["fort", "", ""],
        ["fpha_", "", ""],
        ["SAVERADIAL", "", ""],
        ["SIM_ECO", "", ""],
        ["SVC_", "", ""],
    ]
    arquivos_apagar = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_apagar_regex
    )
    limpa_arquivos_saida(arquivos_apagar)

    tf = time()
    print(f"Pós-processamento do DESSEM feito em {tf - ti:.2f} segundos!")
