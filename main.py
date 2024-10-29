import os
import pathlib

from app.cli import cli


def main():
    os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_BASEDIR"] = str(BASEDIR)
    cli()


if __name__ == "__main__":
    main()
