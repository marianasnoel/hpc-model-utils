from idecomp.decomp.caso import Caso
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.dadger import Dadger
import pandas as pd
from os import curdir
from os.path import join
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

extensao = Caso.read("./caso.dat").arquivos
arquivos = Arquivos.read("./" + extensao)
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
    arquivos.arquivos
    + arquivo_indice
    + arquivo_polinjusdat
    + arquivo_velocidade
    + arquivos_libs
)

diretorio_base = Path(curdir).resolve().parts[-1]

# Zipar deck de entrada (incluindo csvs + velocidades)
with ZipFile(
    join(curdir, f"deck_{diretorio_base}.zip"), "w", compresslevel=ZIP_DEFLATED
) as arquivo_zip:
    for a in arquivos_entrada:
        arquivo_zip.write(a)


# Manter arquivos dec_*.csv

# Zipar todos csv de saida

# Zipar cortdeco e mapcut

# Apagar inviab_0* e outros arquivos temporarios

# Zipar principais relatorios: relato, sumario, inviab_unic
