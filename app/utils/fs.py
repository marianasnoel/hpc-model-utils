import re
from concurrent.futures import ThreadPoolExecutor, wait
from os import chmod, curdir, listdir, remove
from os.path import isdir, isfile, join
from pathlib import Path
from shutil import move, rmtree
from zipfile import ZIP_DEFLATED, ZipFile

from app.utils.zipfileparallel import ZipFileParallel


def change_file_permission(filepath: str, permission_code: int):
    chmod(filepath, permission_code)


def extract_zip_content(filepath: str) -> list[str]:
    with ZipFile(filepath) as file:
        file.extractall()
        return file.namelist()


def moves_content_to_rootdir(subdir: str):
    if isdir(subdir):
        for a in listdir(subdir):
            if isfile(join(subdir, a)):
                move(join(subdir, a), a)
        rmtree(subdir)


def list_files_by_regexes(
    files_to_ignore: list[str], regexes: list[str], path: str = curdir
):
    regex = r"|".join(regexes)
    regex = r"(" + regex + r")"
    files: list[str] = []
    for a in listdir(path):
        if a not in files_to_ignore:
            if re.search(regex, a) is not None:
                files.append(a)

    return files


def compress_files_to_zip(filenames: list[str], compressed_filename: str):
    with ZipFile(
        join(curdir, f"{compressed_filename}.zip"),
        "w",
        compression=ZIP_DEFLATED,
    ) as zip_file:
        for a in sorted(filenames):
            if isfile(join(curdir, a)):
                zip_file.write(a)


def _add_file_to_parallel_zip(handle: ZipFileParallel, filepath: Path):
    data = filepath.read_bytes()
    handle.writestr(str(filepath.name), data)


def compress_files_to_zip_parallel(
    filenames: list[str], compressed_filename: str, num_cpus: int
):
    filepaths = [Path(a) for a in filenames if isfile(a)]
    # TODO - consider each file size while distributing
    # files in threads
    with ZipFileParallel(
        join(curdir, f"{compressed_filename}.zip"),
        "w",
        compression=ZIP_DEFLATED,
    ) as handle:
        with ThreadPoolExecutor(num_cpus) as exe:
            fs = [
                exe.submit(_add_file_to_parallel_zip, handle, f)
                for f in filepaths
            ]

        wait(fs)
        for future in fs:
            future.result()


def clean_files(files: list[str]):
    for a in files:
        if isfile(join(curdir, a)):
            remove(a)
