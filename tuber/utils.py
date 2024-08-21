import asyncio
import re
from concurrent.futures import ThreadPoolExecutor, wait
from os import curdir, listdir, remove
from os.path import isdir, isfile, join
from pathlib import Path
from shutil import move, rmtree
from zipfile import ZIP_DEFLATED, ZipFile

from tuber.zipfileparallel import ZipFileParallel

RETRY_DEFAULT = 3
TIMEOUT_DEFAULT = 10


def traz_conteudo_para_raiz(diretorio: str):
    if isdir(diretorio):
        for a in listdir(diretorio):
            if isfile(join(diretorio, a)):
                move(join(diretorio, a), a)
        rmtree(diretorio)


def identifica_arquivos_via_regex(
    arquivos_ignorar: list[str], lista_regex: list[str]
):
    regex = r"|".join(lista_regex)
    regex = r"(" + regex + r")"
    arquivos: list[str] = []
    for a in listdir(curdir):
        if a not in arquivos_ignorar:
            if re.search(regex, a) is not None:
                arquivos.append(a)

    return arquivos


def zip_arquivos(arquivos: list[str], nome_zip: str):
    diretorio_base = Path(curdir).resolve().parts[-1]
    with ZipFile(
        join(curdir, f"{nome_zip}_{diretorio_base}.zip"),
        "w",
        compression=ZIP_DEFLATED,
    ) as arquivo_zip:
        print(f"Compactando arquivos para {nome_zip}_{diretorio_base}.zip")
        for a in sorted(arquivos):
            if isfile(join(curdir, a)):
                arquivo_zip.write(a)


def _adiciona_arquivo_zip_paralelo(handle: ZipFileParallel, filepath: Path):
    data = filepath.read_bytes()
    handle.writestr(str(filepath.name), data)


def zip_arquivos_paralelo(
    arquivos: list[str], nome_zip: str, numero_processadores: int
):
    diretorio_base = Path(curdir).resolve().parts[-1]
    print(f"Compactando arquivos para {nome_zip}_{diretorio_base}.zip")
    print(f"Paralelizando em {numero_processadores} processos")
    arquivos = [a for a in arquivos if a is not None]
    caminhos_arquivos = [Path(a) for a in arquivos if isfile(a)]
    # TODO - pegar os tamanhos totais dos arquivos e distribuir de maneira
    # mais uniforme.
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
            future.result()


def limpa_arquivos_saida(arquivos: list[str]):
    print("Excluindo arquivos...")
    for a in arquivos:
        if isfile(join(curdir, a)):
            remove(a)


async def run_terminal_retry(
    cmds: list[str],
    num_retry: int = RETRY_DEFAULT,
    timeout: float = TIMEOUT_DEFAULT,
) -> tuple[int, str]:
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
    cmds: list[str], timeout: float = TIMEOUT_DEFAULT
) -> tuple[int | None, str]:
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
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    if stdout:
        return proc.returncode, stdout.decode("utf-8")
    if stderr:
        return proc.returncode, stderr.decode("utf-8")
    return -1, ""
