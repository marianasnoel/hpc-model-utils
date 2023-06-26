from os import listdir
from os import curdir
from os.path import join
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import re
import os
import asyncio
from typing import List, Tuple, Optional
from tuber.zipfileparallel import ZipFileParallel
from concurrent.futures import ThreadPoolExecutor, wait


RETRY_DEFAULT = 3
TIMEOUT_DEFAULT = 10


def identifica_arquivos_via_regex(arquivos_entrada, lista_regex):
    lista = []
    for e in lista_regex:
        lista.append(e[1] + e[0] + e[2])

    arquivos_regex = r"|".join(lista)
    arquivos_regex = r"(" + arquivos_regex + r")"
    arquivos = []
    for a in listdir(curdir):
        if a not in arquivos_entrada:
            if re.search(arquivos_regex, a) is not None:
                arquivos.append(a)

    return arquivos


def zip_arquivos(arquivos, nome_zip):
    diretorio_base = Path(curdir).resolve().parts[-1]

    with ZipFile(
        join(curdir, f"{nome_zip}_{diretorio_base}.zip"),
        "w",
        compresslevel=ZIP_DEFLATED,
    ) as arquivo_zip:
        print(f"Compactando arquivos para {nome_zip}_{diretorio_base}.zip")
        arquivos.sort()
        for a in arquivos:
            if os.path.isfile(join(curdir, a)):
                arquivo_zip.write(a)


def _adiciona_arquivo_zip_paralelo(handle: ZipFileParallel, filepath: Path):
    data = filepath.read_bytes()
    handle.writestr(str(filepath.name), data)


def zip_arquivos_paralelo(arquivos, nome_zip, numero_processadores):
    diretorio_base = Path(curdir).resolve().parts[-1]
    print(f"Compactando arquivos para {nome_zip}_{diretorio_base}.zip")
    print(f"Paralelizando em {numero_processadores} processos")
    caminhos_arquivos = [Path(a) for a in arquivos]
    with ZipFileParallel(
        join(curdir, f"{nome_zip}_{diretorio_base}.zip"),
        "w",
        compression=ZIP_DEFLATED,
    ) as handle:
        with ThreadPoolExecutor(numero_processadores) as exe:
            fs = [
                exe.submit(_adiciona_arquivo_zip_paralelo, handle, f)
                for f in caminhos_arquivos
            ]

        wait(fs)
        for future in fs:
            future.result()  # make sure we didn't get an exception


def limpa_arquivos_saida(arquivos):
    print("Excluindo arquivos...")
    for a in arquivos:
        if os.path.isfile(join(curdir, a)):
            os.remove(a)


async def run_terminal_retry(
    cmds: List[str],
    num_retry: int = RETRY_DEFAULT,
    timeout: float = TIMEOUT_DEFAULT,
) -> Tuple[int, str]:
    """
    Runs a command on the terminal (with retries) and returns.

    :param cmds: Commands and args to be executed
    :param num_retry: Max number of retries
    :param timeout: Timeout for giving up on the command
    :return: Return code and outputs
    :rtype: Tuple[int, str]
    """
    for _ in range(num_retry):
        cod, outputs = await run_terminal(cmds, timeout)
        if cod == 0:
            return cod, outputs
    return -1, ""


async def run_terminal(
    cmds: List[str], timeout: float = TIMEOUT_DEFAULT
) -> Tuple[Optional[int], str]:
    """
    Runs a command on the terminal and returns.

    :param cmds: Commands and args to be executed
    :param timeout: Timeout for giving up on the command
    :return: Return code and outputs
    :rtype: Tuple[int, str]
    """
    cmd = " ".join(cmds)
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await asyncio.wait_for(
        proc.communicate(), timeout=timeout
    )
    if stdout:
        return proc.returncode, stdout.decode("utf-8")
    if stderr:
        return proc.returncode, stderr.decode("utf-8")
    return -1, ""
