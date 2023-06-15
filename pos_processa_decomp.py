from idecomp.decomp.caso import Caso
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.dadger import Dadger
import pandas as pd
from os import listdir
from os import curdir
from os.path import join
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import re
import os

EXTENSAO = Caso.read("./caso.dat").arquivos


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
    arquivo_polinjusdat = [dadger.fj.arquivo] if dadger.fj is not None else []
    arquivo_velocidade = [dadger.vt.arquivo] if dadger.vt is not None else []
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


def identifica_arquivos_saida_csv():
    prefixos_arquivos_saida_csv = [
        "avl_",
        "bengnl",
        "dec_",
        "energia_acopla",
        "balsubFC",
        "cei",
        "cmar",
        "contratos",
        "ener",
        "ever",
        "evnt",
        "flx",
        "hidrpat",
        "pdef",
        "qnat",
        "qtur",
        "term",
        "usina",
        "ute",
        "vert",
        "vutil",
    ]

    arquivos_entrada = identifica_arquivos_entrada()

    arquivos_saida_csv_regex = r"("
    lista = []
    for p in prefixos_arquivos_saida_csv:
        lista.append(r"^" + p + r".*\.csv")

    arquivos_saida_csv_regex = r"|".join(lista)
    arquivos_saida_csv_regex = r"(" + arquivos_saida_csv_regex + ")"

    arquivos_saida_csv = []
    for a in listdir(curdir):
        if a not in arquivos_entrada:
            if re.search(arquivos_saida_csv_regex, a) is not None:
                arquivos_saida_csv.append(a)

    return arquivos_saida_csv


def zip_arquivos(arquivos, nome_zip):
    diretorio_base = Path(curdir).resolve().parts[-1]

    with ZipFile(
        join(curdir, f"{nome_zip}_{diretorio_base}.zip"),
        "w",
        compresslevel=ZIP_DEFLATED,
    ) as arquivo_zip:
        print(f"Compactando arquivos para {nome_zip}_{diretorio_base}.zip")
        for a in arquivos:
            arquivo_zip.write(a)


def apaga_arquivos(arquivos):
    for a in arquivos:
        os.remove(a)


# Zipar deck de entrada
arquivos_entrada = identifica_arquivos_entrada()
zip_arquivos(arquivos_entrada, "deck")

# Zipar csvs de saida
arquivos_saida_csv = identifica_arquivos_saida_csv()
zip_arquivos(arquivos_saida_csv, "saidas_csv")

# Zipar principais relatorios: relato, sumario, inviab_unic
arquivos_saida_relatorios = [
    "relato." + EXTENSAO,
    "sumario." + EXTENSAO,
    "relato2." + EXTENSAO,
    "inviab_unic." + EXTENSAO,
    "relgnl." + EXTENSAO,
    "custos." + EXTENSAO,
]
zip_arquivos(arquivos_saida_relatorios, "saidas_relatorios")

# Zipar cortdeco e mapcut e apagar
arquivos_saida_cortes = [
    "cortdeco." + EXTENSAO,
    "mapcut." + EXTENSAO,
]
zip_arquivos(arquivos_saida_cortes, "saidas_cortes")

# Apagar arquivos saida csv, mas manter arquivos dec_*.csv
# Apagar inviab_0* e outros arquivos temporarios
# Apagar cortes do decomp
# Apagar arquivo de licenca
# Apagar ou manter memcal?
