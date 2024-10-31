from abc import ABC, abstractmethod
from typing import Type

from app.utils.singleton import Singleton


class AbstractModel(ABC):
    """
    Abstract interface for an HPC model to be
    supported in the hpc-model-utils toolset. Any model
    implementation that wishes to meet this interface must
    implement the methods that will aid managing the
    workflow execution steps in the ModelOps context.

    HPC ModelOps General Workflow Steps:

    1 - Available model version validation and acquisition
    2 - Input validation, extraction and encoding sanitization
    3 - Input hashing and execution unique identification
    4 - Specific pre-processing steps required by each model
      - Existing execution check and job calling are not
        part of the model interface
    5 - Model execution status diagnosis, based on the obtained
        outputs
    6 - Specific post-processing steps required by each model
    7 - Output data compression, grouping and cleanup

    """

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def check_and_fetch_model_executables(self, version: str):
        raise NotImplementedError

    @abstractmethod
    def validate_extract_sanitize_inputs(self, compressed_input_file: str):
        raise NotImplementedError

    @abstractmethod
    def generate_unique_input_id(self):
        raise NotImplementedError

    @abstractmethod
    def preprocess(self):
        raise NotImplementedError

    @abstractmethod
    def generate_execution_status(self):
        raise NotImplementedError

    @abstractmethod
    def postprocess(self):
        raise NotImplementedError

    @abstractmethod
    def output_compression_and_cleanup(self):
        raise NotImplementedError


class ModelFactory(metaclass=Singleton):
    def __init__(self) -> None:
        self._models: dict[str, Type[AbstractModel]] = {}

    def register(self, model_name: str, model: Type[AbstractModel]) -> None:
        self._models[model_name] = model

    def factory(self, model: str) -> AbstractModel:
        if model in self._models:
            return self._models[model]()
        else:
            raise ValueError(f"Model with name {model} not found")
