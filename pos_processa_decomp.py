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

EXTENSAO = Caso.read("./caso.dat").arquivos


def zip_deck_entrada():
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

    diretorio_base = Path(curdir).resolve().parts[-1]

    with ZipFile(
        join(curdir, f"deck_{diretorio_base}.zip"),
        "w",
        compresslevel=ZIP_DEFLATED,
    ) as arquivo_zip:
        for a in arquivos_entrada:
            arquivo_zip.write(a)


# Zipar deck de entrada
zip_deck_entrada()


# Zipar todos csvs de saida
diretorio_base = Path(curdir).resolve().parts[-1]
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

arquivos_saida_csv_regex = r"("
for p in prefixos_arquivos_saida_csv:
    arquivos_saida_csv_regex = arquivos_saida_csv_regex + "^" + p + ".*\.csv|"
arquivos_saida_csv_regex = arquivos_saida_csv_regex + ")"

arquivos_saida_csv = []
for a in listdir(curdir):
    if re.search(arquivos_saida_csv_regex, a):
        arquivos_saida_csv.append(a)

# lembrar de retirar os arquivos de entrada
with ZipFile(
    join(curdir, f"saidas_csv_{diretorio_base}.zip"),
    "w",
    compresslevel=ZIP_DEFLATED,
) as arquivo_zip:
    for a in arquivos_saida_csv:
        arquivo_zip.write(a)

# Manter arquivos dec_*.csv

# Zipar cortdeco e mapcut

# Apagar inviab_0* e outros arquivos temporarios

# Zipar principais relatorios: relato, sumario, inviab_unic
