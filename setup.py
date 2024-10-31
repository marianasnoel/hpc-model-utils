from setuptools import find_packages, setup

from app import __version__

long_description = "hpc_model_utils"

requirements = []
with open("requirements.txt", "r") as fh:
    requirements = fh.readlines()

setup(
    name="hpc_model_utils",
    version=__version__,
    author="Mariana Noel",
    author_email="marianasimoesnoel@gmail.com",
    description="hpc_model_utils",
    long_description=long_description,
    packages=find_packages(),
    py_modules=["main", "app"],
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    entry_points="""
        [console_scripts]
        hpc-model-utils=main:main
    """,
    python_requires=">=3.10",
    install_requires=requirements,
)
