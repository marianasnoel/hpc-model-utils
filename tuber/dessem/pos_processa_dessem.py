import os
from time import time

import pandas as pd  # type: ignore
from idessem.dessem.dessemarq import DessemArq

from tuber.utils import (
    identifica_arquivos_via_regex,
    limpa_arquivos_saida,
    zip_arquivos,
)

if __name__ == "__main__":
    ti = time()

    dessem_arq = DessemArq.read("./dessem.arq")
    EXTENSAO = dessem_arq.caso.valor

    def identifica_arquivos_entrada() -> list[str]:
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
            dessem_arq.dessopc,
        ]
        arquivos_gerais = []
        for reg in registros_arquivos_gerais:
            if reg is not None:
                arquivos_gerais.append(reg.valor)

        arquivo_indice = (
            [dessem_arq.ilibs.valor] if dessem_arq.ilibs is not None else []
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
        arquivos_entrada = [a for a in arquivos_entrada if a is not None]

        return arquivos_entrada

    # Zipar deck de entrada
    arquivos_entrada = identifica_arquivos_entrada()
    zip_arquivos(arquivos_entrada, "deck")

    # Zipar csvs de saida com resultados da operação
    regex_arquivos_saida_csv = [
        r"^PDO_OPER.*$",
        r"^PDO_AVAL_.*$",
        r"^PDO_CMO.*$",
        r"^PDO_CONTR.*$",
        r"^PDO_DESV.*$",
        r"^PDO_ELEV.*$",
        r"^PDO_EOLICA.*$",
        r"^PDO_FLUXLIN.*$",
        r"^PDO_GERBARR.*$",
        r"^PDO_HIDR.*$",
        r"^PDO_INTER.*$",
        r"^PDO_RESERVA.*$",
        r"^PDO_REST.*$",
        r"^PDO_SIST.*$",
        r"^PDO_SOMFLUX.*$",
        r"^PDO_STATREDE_ITER.*$",
        r"^PDO_SUMAOPER.*$",
        r"^PDO_TERM.*$",
        r"^PDO_VAGUA.*$",
        r"^PDO_VERT.*$",
    ]
    arquivos_saida_operacao = identifica_arquivos_via_regex(
        arquivos_entrada, regex_arquivos_saida_csv
    )
    zip_arquivos(arquivos_saida_operacao, "operacao")

    # Zipar demais relatorios de saída
    regex_arquivos_saida_csv = [
        r"AVL_.*$",
        r"DES_.*$",
        r"LOG_.*$",
        r"PDO_AVAL.*$",
        r"PDO_ECO.*$",
        r"PTOPER.*\.PWF$",
    ]
    arquivos_saida_relatorios = identifica_arquivos_via_regex(
        arquivos_entrada, regex_arquivos_saida_csv
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
        "LOG_MATRIZ." + EXTENSAO,
        "AVL_ESTATFPHA." + EXTENSAO,
        "LOG_INVIAB." + EXTENSAO,
    ]
    arquivos_zipados = (
        arquivos_entrada + arquivos_saida_operacao + arquivos_saida_relatorios
    )
    arquivos_limpar = [a for a in arquivos_zipados if a not in arquivos_manter]
    limpa_arquivos_saida(arquivos_limpar)

    # Apagar arquivos temporários para limpar diretório pós execução incompleta/inviavel
    # mesmo que não tenham sido zipados.
    arquivos_apagar_regex = [
        r"^fort.*$",
        r"^fpha_.*$",
        r"^SAVERADIAL.*$",
        r"^SIM_ECO.*$",
        r"^SVC_.*$",
    ]
    arquivos_apagar = identifica_arquivos_via_regex(
        arquivos_entrada, arquivos_apagar_regex
    )
    limpa_arquivos_saida(arquivos_apagar)

    tf = time()
    print(f"Pós-processamento do DESSEM feito em {tf - ti:.2f} segundos!")
