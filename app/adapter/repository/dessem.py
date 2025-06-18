import json
from datetime import datetime
from os import curdir, getenv, listdir
from os.path import isdir, isfile, join
from pathlib import Path
from shutil import move
from typing import Any

import pandas as pd  # type: ignore
from cfinterface.components.defaultblock import DefaultBlock
from idessem.dessem import DesLogRelato, DessemArq, Dessopc, Entdados, Operut

from app.adapter.repository.abstractmodel import (
    AbstractModel,
    ModelFactory,
)
from app.adapter.repository.newave import NEWAVE
from app.models.runstatus import RunStatus
from app.utils.constants import (
    AWS_ACCESS_KEY_ID_ENV,
    AWS_SECRET_ACCESS_KEY_ENV,
    EXECUTION_ID_FILE,
    INPUTS_ECHO_PREFIX,
    INPUTS_PREFIX,
    METADATA_FILE,
    METADATA_JOB_ID,
    METADATA_MODEL_NAME,
    METADATA_MODEL_VERSION,
    METADATA_MODELOPS_ID,
    METADATA_PARENT_ID,
    METADATA_PARENT_STARTING_DATE,
    METADATA_STATUS,
    METADATA_STUDY_NAME,
    METADATA_STUDY_STARTING_DATE,
    MODEL_EXECUTABLE_DIRECTORY,
    MODEL_EXECUTABLE_PERMISSIONS,
    OUTPUTS_PREFIX,
    STATUS_DIAGNOSIS_FILE,
    SYNTHESIS_DIR,
    VERSION_PREFIX,
)
from app.utils.fs import (
    change_file_permission,
    clean_files,
    compress_files_to_zip,
    compress_files_to_zip_parallel,
    extract_zip_content,
    find_file_case_insensitive,
    list_files_by_regexes,
    moves_content_to_rootdir,
)
from app.utils.hashing import hash_all_files_in_path, hash_string
from app.utils.s3 import (
    check_and_delete_bucket_item,
    check_and_download_bucket_items,
    check_and_get_bucket_item,
    upload_file_to_bucket,
)
from app.utils.terminal import cast_encoding_to_utf8, run_in_terminal
from app.utils.timing import time_and_log


class DESSEM(AbstractModel):
    MODEL_NAME = "dessem"
    MODEL_ENTRY_FILE = "dessem.arq"
    NWLISTCF_ENTRY_FILE = "arquivos.dat"
    LIBS_ENTRY_FILE = "indices.csv"
    LICENSE_FILENAMES = ["dessem.lic", "ddsDESSEM.cep"]
    CUT_FILE = "cortes.zip"
    RESOURCES_FILE = "recursos.zip"
    SIMULATION_FILE = "simulacao.zip"
    DESSEM_PATH = "hpc-model-utils/assets/jobs/dessem.sh"
    DESSEM_TIMEOUT = 172800  # 48h
    DATA_ERROR_PATTERN = "ERRO(S) NA ENTRADA DE DADOS"

    DECK_DATA_CACHING: dict[str, Any] = {}

    CUT_HEADER_FILE_PATTERN = r"^mapcut.*$"
    CUT_FILE_PATTERN = r"^cortdeco.*$"

    @property
    def dessem_arq(self) -> DessemArq:
        name = "dessemarq"
        if name not in self.DECK_DATA_CACHING:
            self._log.info(f"Reading file: {self.MODEL_ENTRY_FILE}")
            self.DECK_DATA_CACHING[name] = DessemArq.read(self.MODEL_ENTRY_FILE)
        return self.DECK_DATA_CACHING[name]

    @property
    def entdados(self) -> Entdados:
        name = "entdados"
        if name not in self.DECK_DATA_CACHING:
            caso_register = self.dessem_arq.caso
            if not caso_register:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            extension = (
                caso_register.valor
                if caso_register.valor is not None
                else "DAT"
            )
            filename = f"ENTDADOS.{extension}"
            filename = find_file_case_insensitive(".", filename)
            self._log.info(f"Reading file: {filename}")
            Entdados.ENCODING = "iso-8859-1"
            self.DECK_DATA_CACHING[name] = Entdados.read(filename)
        return self.DECK_DATA_CACHING[name]

    @property
    def dessopc(self) -> Dessopc:
        name = "dessopc"
        if name not in self.DECK_DATA_CACHING:
            caso_register = self.dessem_arq.caso
            if not caso_register:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            extension = (
                caso_register.valor
                if caso_register.valor is not None
                else "DAT"
            )
            dessopc_register = self.dessem_arq.dessopc
            if not dessopc_register:
                msg = "No content found in DESSOPC"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            dessopc_filename = dessopc_register.valor or "DESSOPC"
            filename = f"{dessopc_filename}.{extension}"
            filename = find_file_case_insensitive(".", filename)
            self._log.info(f"Reading file: {filename}")
            Dessopc.ENCODING = "iso-8859-1"
            self.DECK_DATA_CACHING[name] = Dessopc.read(filename)
        return self.DECK_DATA_CACHING[name]

    @property
    def operut(self) -> Operut:
        name = "operut"
        if name not in self.DECK_DATA_CACHING:
            caso_register = self.dessem_arq.caso
            if not caso_register:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            extension = (
                caso_register.valor
                if caso_register.valor is not None
                else "DAT"
            )
            operut_register = self.dessem_arq.operut
            if not operut_register:
                msg = "No content found in OPERUT"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            oprut_filename = operut_register.valor or "OPERUT"
            filename = f"{oprut_filename}.{extension}"
            filename = find_file_case_insensitive(".", filename)
            self._log.info(f"Reading file: {filename}")
            Operut.ENCODING = "iso-8859-1"
            self.DECK_DATA_CACHING[name] = Operut.read(filename)
        return self.DECK_DATA_CACHING[name]

    @property
    def des_log_relato(self) -> DesLogRelato:
        name = "des_log_relato"
        if name not in self.DECK_DATA_CACHING:
            caso_register = self.dessem_arq.caso
            if not caso_register:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            extension = (
                caso_register.valor
                if caso_register.valor is not None
                else "DAT"
            )
            filename = f"DES_LOG_RELATO.{extension}"
            filename = find_file_case_insensitive(".", filename)
            self._log.info(f"Reading file: {filename}")
            self.DECK_DATA_CACHING[name] = DesLogRelato.read(filename)
        return self.DECK_DATA_CACHING[name]

    def _update_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        if isfile(METADATA_FILE):
            with open(METADATA_FILE, "r") as f:
                metadata = {**json.load(f), **metadata}
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f)
        return metadata

    def check_and_fetch_executables(self, version: str, bucket: str):
        self._log.info(
            f"Fetching executables in {bucket} for version {version}..."
        )
        prefix_with_version = join(VERSION_PREFIX, self.MODEL_NAME, version)
        downloaded_filepaths = check_and_download_bucket_items(
            bucket, MODEL_EXECUTABLE_DIRECTORY, prefix_with_version, self._log
        )
        for filepath in downloaded_filepaths:
            if any([
                license_filename in filepath
                for license_filename in self.LICENSE_FILENAMES
            ]):
                license_filename = filepath.split("/")[-1]
                move(filepath, join(curdir, license_filename))
                self._log.info(f"Moved {filepath} to {license_filename}")
            else:
                change_file_permission(filepath, MODEL_EXECUTABLE_PERMISSIONS)
                self._log.info(
                    f"Changed {filepath} permissions to"
                    + f" {MODEL_EXECUTABLE_PERMISSIONS:o}"
                )

        metadata = {
            METADATA_MODEL_NAME: self.MODEL_NAME.upper(),
            METADATA_MODEL_VERSION: version,
        }
        self._update_metadata(metadata)
        self._log.info("Executables successfully fetched and ready!")

    def check_and_fetch_inputs(
        self,
        filename: str,
        bucket: str,
        parent_id: str,
        delete: bool = True,
    ):
        filename = filename.split("/")[-1]
        self._log.info(
            f"Fetching {filename} in {join(bucket, INPUTS_PREFIX)}..."
        )
        remote_filepath = join(INPUTS_PREFIX, filename)
        check_and_download_bucket_items(
            bucket, str(Path(curdir).resolve()), remote_filepath, self._log
        )

        if delete:
            remote_filepath = join(INPUTS_PREFIX, filename)
            self._log.info(
                f"Removing {filename} from {join(bucket, INPUTS_PREFIX)}..."
            )
            check_and_delete_bucket_item(
                bucket, filename, remote_filepath, self._log
            )

        if len(parent_id) > 0:
            # Downloads parent metadata and check if is a DECOMP execution
            # with SUCCESS status
            self._log.info(
                f"Fetching parent data from ID {parent_id} in"
                + f" {join(bucket, OUTPUTS_PREFIX, parent_id)}..."
            )
            remote_filepath = join(OUTPUTS_PREFIX, parent_id, METADATA_FILE)
            parent_metadata = json.loads(
                check_and_get_bucket_item(bucket, remote_filepath, self._log)
            )
            if any([
                k not in parent_metadata
                for k in [
                    METADATA_MODEL_NAME,
                    METADATA_STATUS,
                    METADATA_STUDY_STARTING_DATE,
                ]
            ]):
                raise ValueError(
                    f"Parent metadata is incomplete [{parent_metadata}]"
                )
            if (
                parent_metadata[METADATA_MODEL_NAME]
                != NEWAVE.MODEL_NAME.upper()
            ):
                raise ValueError(
                    f"Parent model is not {NEWAVE.MODEL_NAME.upper()}"
                )
            success_status = RunStatus.SUCCESS.value
            if parent_metadata[METADATA_STATUS] != success_status:
                raise ValueError(
                    f"Parent execution status was not {success_status}"
                )

            # Downloads parent cut file
            remote_filepath = join(OUTPUTS_PREFIX, parent_id, self.CUT_FILE)
            self._log.info(f"Fetching parent file from {remote_filepath}")
            check_and_download_bucket_items(
                bucket,
                str(Path(curdir).resolve()),
                remote_filepath,
                self._log,
            )
            metadata = {
                METADATA_PARENT_ID: parent_id,
                METADATA_PARENT_STARTING_DATE: parent_metadata[
                    METADATA_STUDY_STARTING_DATE
                ],
            }
            self._update_metadata(metadata)
        else:
            self._log.info("No parent id was given!")

        self._log.info("Inputs successfully fetched!")

    def _get_cut_filepatterns_for_extraction(self) -> list[str]:
        return [self.CUT_FILE_PATTERN, self.CUT_HEADER_FILE_PATTERN]

    def extract_sanitize_inputs(self, compressed_input_file: str):
        compressed_input_file = compressed_input_file.split("/")[-1]
        extracted_files = (
            extract_zip_content(compressed_input_file)
            if isfile(compressed_input_file)
            else []
        )
        self._log.info(f"Extracted input files: {extracted_files}")

        self._log.info("Forcing encoding to utf-8")
        for f in listdir():
            cast_encoding_to_utf8(f)

        # Unzips the parent files
        for parent_file, patterns_to_extract in {
            self.CUT_FILE: self._get_cut_filepatterns_for_extraction(),
        }.items():
            if isfile(parent_file):
                extracted_files = extract_zip_content(
                    parent_file, patterns=patterns_to_extract
                )
                self._log.info(f"Extracted parent files: {extracted_files}")

    def generate_unique_input_id(self, version: str, parent_id: str):
        file_hash, hashed_files = hash_all_files_in_path(
            file_regexes_to_ignore=[
                r".*\.modelops",
                r".*\.log",
                r".*\.lic",
                r".*\.cep",
                r".*\.zip",
                r"cortdeco.*",
                r"mapcut.*",
            ]
        )
        self._log.info(f"Files considered for ID: {hashed_files}")
        unique_id = hash_string(
            "".join([
                self.MODEL_NAME,
                hash_string(version),
                parent_id,
                file_hash,
            ])
        )

        with open(EXECUTION_ID_FILE, "w") as f:
            f.write(unique_id)
        self._update_metadata({METADATA_MODELOPS_ID: unique_id})

        return unique_id

    def preprocess(self):
        dessem_arq = self.dessem_arq
        mapfcf = dessem_arq.mapfcf
        if mapfcf:
            mapcut_files = [f for f in listdir() if "mapcut" in f]
            if len(mapcut_files) == 0:
                self._log.info("Could not find mapcut file")
                raise RuntimeError()
            mapcut_file = mapcut_files[0]
            self._log.info(f"Overwriting MAPFCF register with {mapcut_file}")
        cortfcf = dessem_arq.cortfcf
        if cortfcf:
            cortdeco_files = [f for f in listdir() if "cortdeco" in f]
            if len(cortdeco_files) == 0:
                self._log.info("Could not find cortdeco file")
                raise RuntimeError()
            cortdeco_file = cortdeco_files[0]
            self._log.info(f"Overwriting CORTFCF register with {cortdeco_file}")
            cortfcf.valor = cortdeco_file
        dessem_arq.write(self.MODEL_ENTRY_FILE)

    def _evaluate_data_error(self, des_log_relato: DesLogRelato) -> bool:
        return any([
            self.DATA_ERROR_PATTERN in b.data
            for b in des_log_relato.data.of_type(DefaultBlock)
        ])

    # TODO - aguardando suporte do arquivo LOG_INVIAB na idessem (até a idessem 0.0.23 não possui)
    # def _evaluate_feasibility(self, log_inviab: LogInviab) -> bool:
    #     if not isinstance(log_inviab, LogInviab):
    #         return True
    #     inviabs = log_inviab.tabela
    #     if inviabs is None:
    #         return False
    #     elif inviabs.empty:
    #         return False
    #     else:
    #         return True

    def _evaluate_des_log_relato_outputs(
        self, des_log_relato: DesLogRelato
    ) -> bool:
        return des_log_relato.tempo_processamento is None

    def _edit_core_count_dessopc(self, core_count: int):
        dessopc = self.dessopc
        if dessopc.uctpar is not None:
            dessopc.uctpar = core_count
            dessopc_file = self.dessem_arq.dessopc
            if dessopc_file is not None:
                filename = self.dessem_arq.dessopc.valor
                if filename is not None:
                    dessopc.write(filename)
            else:
                self._log.info("No DESSOPC filename found")
        else:
            self._log.info("No UCTPAR found in DESSOPC")

    def _edit_core_count_operut(self, core_count: int):
        operut = self.operut
        if operut.uctpar is not None:
            operut.uctpar = core_count
            operut_file = self.dessem_arq.operut
            if operut_file is not None:
                filename = self.dessem_arq.operut.valor
                if filename is not None:
                    operut.write(filename)
        else:
            self._log.info("No UCTPAR found in OPERUT")

    def _edit_core_count(self, core_count: int):
        dessem_arq = self.dessem_arq
        if dessem_arq.dessopc is not None:
            self._edit_core_count_dessopc(core_count)
        else:
            self._edit_core_count_operut(core_count)

    def run(
        self, queue: str, core_count: int, mpich_path: str, slurm_path: str
    ):
        self._edit_core_count(core_count)
        self._log.info(f"Script file: {self.DESSEM_PATH}")
        run_in_terminal(
            [self.DESSEM_PATH], timeout=self.DESSEM_TIMEOUT, log_output=True
        )

    def generate_execution_status(self, job_id: str) -> str:
        self._log.info("Reading 'DES_LOG_RELATO' file for generating status...")
        des_log_relato = self.des_log_relato
        # TODO - aguardando suporte do arquivo LOG_INVIAB na idessem
        # (até a idessem 0.0.23 não possui)
        # self._log.info("Reading 'LOG_INVIAB' file for generating status...")
        # log_inviab = self.log_inviab

        status = RunStatus.SUCCESS

        if self._evaluate_data_error(des_log_relato):
            status = RunStatus.DATA_ERROR
        # elif self._evaluate_feasibility(log_inviab):
        #     status = RunStatus.INFEASIBLE
        elif self._evaluate_des_log_relato_outputs(des_log_relato):
            status = RunStatus.DATA_ERROR

        status_value = status.value
        with open(STATUS_DIAGNOSIS_FILE, "w") as f:
            f.write(status_value)
        metadata = {METADATA_JOB_ID: job_id, METADATA_STATUS: status_value}
        self._update_metadata(metadata)
        return status_value

    def postprocess(self):
        pass

    def _list_input_files(self) -> list[str]:
        dessem_arq = self.dessem_arq
        common_input_files: list[str | None] = [self.MODEL_ENTRY_FILE]
        common_input_registers = [
            dessem_arq.vazoes,
            dessem_arq.dadger,
            dessem_arq.mapfcf,
            dessem_arq.cortfcf,
            dessem_arq.cadusih,
            dessem_arq.operuh,
            dessem_arq.deflant,
            dessem_arq.cadterm,
            dessem_arq.operut,
            dessem_arq.indelet,
            dessem_arq.ilstri,
            dessem_arq.cotasr11,
            dessem_arq.areacont,
            dessem_arq.respot,
            dessem_arq.mlt,
            dessem_arq.curvtviag,
            dessem_arq.ptoper,
            dessem_arq.infofcf,
            dessem_arq.ree,
            dessem_arq.eolica,
            dessem_arq.rampas,
            dessem_arq.rstlpp,
            dessem_arq.restseg,
            dessem_arq.respotele,
            dessem_arq.uch,
            dessem_arq.dessopc,
        ]
        for reg in common_input_registers:
            if reg is not None:
                common_input_files.append(reg.valor)
        index_file = (
            [dessem_arq.ilibs.valor] if dessem_arq.ilibs is not None else []
        )
        libs_input_files = (
            pd.read_csv(index_file[0], delimiter=";", comment="&", header=None)[
                2
            ]
            .unique()
            .tolist()
            if len(index_file) == 1
            else []
        )
        network_files = [
            a for a in listdir(curdir) if all(["pat" in a, ".afp" in a])
        ] + [a for a in listdir(curdir) if ".pwf" in a]
        input_files = (
            [a for a in common_input_files if len(a) > 0]
            + index_file
            + [a.strip() for a in libs_input_files]
            + network_files
        )
        input_files = [a for a in input_files if a is not None]
        self._log.info(f"Files considered as input: {input_files}")
        return input_files

    def _list_report_files(self, input_files: list[str]) -> list[str]:
        report_output_files = []
        report_output_file_regex = [
            r"AVL_.*$",
            r"DES_.*$",
            r"LOG_.*$",
            r"PDO_AVAL.*$",
            r"PDO_ECO.*$",
            r"PTOPER.*\.PWF$",
        ]
        report_output_files += list_files_by_regexes(
            input_files, report_output_file_regex
        )
        self._log.info(f"Files considered as report: {report_output_files}")
        return report_output_files

    def _list_operation_files(self, input_files: list[str]) -> list[str]:
        operation_output_file_regex = [
            r"^PDO_OPER.*$",
            r"^PDO_AVAL_.*$",
            r"^PDO_CMO.*$",
            r"^PDO_CONTR.*$",
            r"^PDO_DESV.*$",
            r"^PDO_ELEV.*$",
            r"^PDO_EOLICA.*$",
            r"^PDO_FLUXLIN.*$",
            r"^PDO_GERBARR.*$",
            r"^PDO_HIDR.*$",
            r"^PDO_INTER.*$",
            r"^PDO_RESERVA.*$",
            r"^PDO_REST.*$",
            r"^PDO_SIST.*$",
            r"^PDO_SOMFLUX.*$",
            r"^PDO_STATREDE_ITER.*$",
            r"^PDO_SUMAOPER.*$",
            r"^PDO_TERM.*$",
            r"^PDO_VAGUA.*$",
            r"^PDO_VERT.*$",
        ]
        operation_output_files = list_files_by_regexes(
            input_files, operation_output_file_regex
        )
        self._log.info(
            f"Files considered as operation: {operation_output_files}"
        )
        return operation_output_files

    def _cleanup_files(
        self,
        input_files: list[str],
        report_files: list[str],
        operation_files: list[str],
    ):
        caso_register = self.dessem_arq.caso
        if not caso_register:
            msg = f"No content found in {self.MODEL_ENTRY_FILE}"
            self._log.error(msg)
            raise FileNotFoundError(msg)
        extension = (
            caso_register.valor if caso_register.valor is not None else "DAT"
        )
        keeping_files = input_files + [
            "DES_LOG_RELATO." + extension,
            "PDO_CMOBAR." + extension,
            "PDO_CMOSIST." + extension,
            "PDO_CONTR." + extension,
            "PDO_EOLICA." + extension,
            "PDO_HIDR." + extension,
            "PDO_OPER_CONTR." + extension,
            "PDO_OPER_TERM." + extension,
            "PDO_OPER_TITULACAO_CONTRATOS." + extension,
            "PDO_OPER_TITULACAO_USINAS." + extension,
            "PDO_OPERACAO." + extension,
            "PDO_SIST." + extension,
            "PDO_SOMFLUX." + extension,
            "PDO_SUMAOPER." + extension,
            "PDO_TERM." + extension,
            "LOG_MATRIZ." + extension,
            "AVL_ESTATFPHA." + extension,
            "LOG_INVIAB." + extension,
        ]
        keeping_files = [a for a in keeping_files if a is not None]
        compressed_files = input_files + report_files + operation_files
        cleaning_files = [a for a in compressed_files if a not in keeping_files]
        cleaning_files_regex = [
            r"^fort.*$",
            r"^fpha_.*$",
            r"^SAVERADIAL.*$",
            r"^SIM_ECO.*$",
            r"^SVC_.*$",
        ]
        cleaning_files += list_files_by_regexes(
            input_files, cleaning_files_regex
        )
        self._log.info(f"Cleaning files: {cleaning_files}")
        clean_files(cleaning_files)

    def metadata_generation(self) -> dict[str, Any]:
        titulo = self.dessem_arq.titulo
        if not titulo:
            raise ValueError("TITULO register not found in <dessem.arq>")
        study_name = titulo.valor

        # TODO - esperando suporte ao dadvaz.dat
        # dt = self.dadger.dt
        # if not dt:
        #     raise ValueError("DT register not found in <dadger>")
        # year = dt.ano
        # month = dt.mes
        # day = dt.dia
        # if year is None:
        #     raise ValueError("DT register with incomplete info (year)")
        # if month is None:
        #     raise ValueError("DT register with incomplete info (month)")
        # if day is None:
        #     raise ValueError("DT register with incomplete info (day)")
        # study_starting_date = datetime(year, month, day, tzinfo=UTC)
        study_starting_date = datetime.today()
        metadata = {
            METADATA_STUDY_STARTING_DATE: study_starting_date.isoformat(),
            METADATA_STUDY_NAME: study_name if study_name else "",
        }
        return self._update_metadata(metadata)

    def output_compression_and_cleanup(self, num_cpus: int):
        with time_and_log("Output compression and cleanup", logger=self._log):
            input_files = self._list_input_files()
            compress_files_to_zip(input_files, "deck")
            # Moves content from DESSEM subdirectories to root
            for d in ["out"]:
                moves_content_to_rootdir(d)
            # Parallel compression
            operation_files = self._list_operation_files(input_files)
            compress_files_to_zip_parallel(
                operation_files, "operacao", num_cpus
            )
            report_files = self._list_report_files(input_files)
            compress_files_to_zip_parallel(report_files, "relatorios", num_cpus)
            self._cleanup_files(input_files, report_files, operation_files)

    def _upload_input_echo(
        self, compressed_input_file: str, bucket: str, prefix: str
    ):
        with time_and_log("Time for uploading input echo", logger=self._log):
            compressed_input_file = compressed_input_file.split("/")[-1]
            upload_file_to_bucket(
                compressed_input_file,
                bucket,
                join(prefix, "raw.zip"),
                aws_access_key_id=getenv(AWS_ACCESS_KEY_ID_ENV),
                aws_secret_access_key=getenv(AWS_SECRET_ACCESS_KEY_ENV),
            )
            upload_file_to_bucket(
                "deck.zip",
                bucket,
                join(prefix, "deck.zip"),
                aws_access_key_id=getenv(AWS_ACCESS_KEY_ID_ENV),
                aws_secret_access_key=getenv(AWS_SECRET_ACCESS_KEY_ENV),
            )

    def _upload_outputs(self, bucket: str, prefix: str):
        with time_and_log("Time for uploading outputs", logger=self._log):
            output_files = [
                "decomp.tim",
                "cortes.zip",
                "relatorios.zip",
                "operacao.zip",
            ]
            output_files += list_files_by_regexes([], [r".*\.modelops"])
            for f in output_files:
                if isfile(f):
                    self._log.info(f"Uploading {f}")
                    upload_file_to_bucket(
                        f,
                        bucket,
                        join(prefix, f),
                        aws_access_key_id=getenv(AWS_ACCESS_KEY_ID_ENV),
                        aws_secret_access_key=getenv(AWS_SECRET_ACCESS_KEY_ENV),
                    )

    def _upload_synthesis(self, bucket: str, prefix: str):
        with time_and_log("Time for uploading synthesis", logger=self._log):
            output_files = listdir(SYNTHESIS_DIR)
            for f in output_files:
                if isfile(join(SYNTHESIS_DIR, f)):
                    self._log.info(f"Uploading {f}")
                    upload_file_to_bucket(
                        join(SYNTHESIS_DIR, f),
                        bucket,
                        join(prefix, f),
                        aws_access_key_id=getenv(AWS_ACCESS_KEY_ID_ENV),
                        aws_secret_access_key=getenv(AWS_SECRET_ACCESS_KEY_ENV),
                    )

    def result_upload(
        self,
        compressed_input_file: str,
        inputs_bucket: str,
        outputs_bucket: str,
    ):
        with open(EXECUTION_ID_FILE, "r") as f:
            unique_id = f.read().strip("\n")
        self._log.info(f"Uploading results for {self.MODEL_NAME} - {unique_id}")
        inputs_echo_prefix_with_id = join(INPUTS_ECHO_PREFIX, unique_id)
        outputs_prefix_with_id = join(OUTPUTS_PREFIX, unique_id)
        synthesis_prefix_with_id = join(
            OUTPUTS_PREFIX, unique_id, SYNTHESIS_DIR
        )
        self._upload_input_echo(
            compressed_input_file, inputs_bucket, inputs_echo_prefix_with_id
        )
        self._upload_outputs(outputs_bucket, outputs_prefix_with_id)
        self._upload_synthesis(
            outputs_bucket, synthesis_prefix_with_id
        ) if isdir(SYNTHESIS_DIR) else self._log.warning(
            "No synthesis directory found!"
        )


ModelFactory().register(DESSEM.MODEL_NAME, DESSEM)
