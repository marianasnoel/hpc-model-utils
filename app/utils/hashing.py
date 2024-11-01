import hashlib
from os import curdir, listdir, stat
from os.path import isfile, join

from app.utils.constants import FILE_SIZE_LIMIT_FOR_HASHING_BYTES


def hash_string(data: str) -> str:
    """
    Evaluates a hash string from another string.
    """
    return hashlib.md5(data.encode("utf-8")).hexdigest()


def hash_object(obj: object) -> str:
    """
    Evaluates a hash string from the object's __dict__ attribute. It
    is a simple but effective way for identifying uniquely an object
    that might be used for caching purposes.
    """
    return hashlib.md5(str(obj.__dict__).encode("utf-8")).hexdigest()


def hash_file(filepath: str) -> str:
    """
    Evaluates a hash string from the binary content of a file.
    """
    with open(filepath, "rb") as fp:
        return hashlib.md5(fp.read()).hexdigest()


def hash_all_files_in_path(
    path: str = curdir, file_size_limit_bytes=FILE_SIZE_LIMIT_FOR_HASHING_BYTES
) -> str:
    """
    Hashes all the files in a given directory and
    combine their hashes in a single string.
    """
    files_in_path = [join(path, f) for f in listdir(path) if isfile(f)]
    files_in_limit = [
        f for f in files_in_path if stat(f).st_size <= file_size_limit_bytes
    ]
    file_hashes = [hash_file(f) for f in files_in_limit]
    return hash_string("".join(file_hashes))
