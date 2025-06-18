import json
from datetime import datetime, timedelta
from os import curdir, environ, getenv, listdir
from os.path import isdir, isfile, join
from pathlib import Path
from shutil import move
from typing import Any

import pandas as pd  # type: ignore
from cfinterface.components.defaultblock import DefaultBlock
from idecomp.decomp import Arquivos, Caso, Dadger, InviabUnic, Relato
from pytz import UTC  # type: ignore

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
from app.utils.scheduler import follow_submitted_job, submit_job
from app.utils.terminal import cast_encoding_to_utf8, run_in_terminal
from app.utils.timing import time_and_log


class DECOMP(AbstractModel):
    MODEL_NAME = "decomp"
    NAMECAST_PROGRAM_NAME = "convertenomesdecomp"
    MODEL_ENTRY_FILE = "caso.dat"
    NWLISTCF_ENTRY_FILE = "arquivos.dat"
    LIBS_ENTRY_FILE = "indices.csv"
    LICENSE_FILENAMES = ["decomp.lic", "ddsDECOMP.cep"]
    CUT_FILE = "cortes.zip"
    RESOURCES_FILE = "recursos.zip"
    SIMULATION_FILE = "simulacao.zip"
    DATA_ERROR_PATTERN = "ERRO(S) DE ENTRADA DE DADOS"
    NEGATIVE_GAP_PATTERN = "ATENCAO: GAP NEGATIVO"
    MAX_ITERATIONS_PATTERN = "CONVERGENCIA NAO ALCANCADA EM"
    DECOMP_JOB_PATH = "hpc-model-utils/assets/jobs/decomp.job"
    DECOMP_JOB_TIMEOUT = 172800  # 48h

    DECK_DATA_CACHING: dict[str, Any] = {}

    CUT_HEADER_FILE = "cortesh.dat"
    CUT_FULL_FILE = "cortes.dat"

    @property
    def caso_dat(self) -> Caso:
        name = "caso"
        if name not in self.DECK_DATA_CACHING:
            self._log.info(f"Reading file: {self.MODEL_ENTRY_FILE}")
            self.DECK_DATA_CACHING[name] = Caso.read(self.MODEL_ENTRY_FILE)
        return self.DECK_DATA_CACHING[name]

    @property
    def arquivos_dat(self) -> Arquivos:
        name = "arquivos"
        if name not in self.DECK_DATA_CACHING:
            filename = self.caso_dat.arquivos
            if not filename:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            self._log.info(f"Reading file: {filename}")
            self.DECK_DATA_CACHING[name] = Arquivos.read(filename)
        return self.DECK_DATA_CACHING[name]

    @property
    def dadger(self) -> Dadger:
        name = "dadger"
        if name not in self.DECK_DATA_CACHING:
            filename = self.arquivos_dat.dadger
            if not filename:
                msg = f"No <dadger> found in {self.caso_dat.arquivos}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            self._log.info(f"Reading file: {filename}")
            self.DECK_DATA_CACHING[name] = Dadger.read(filename)
        return self.DECK_DATA_CACHING[name]

    @property
    def relato(self) -> Relato:
        name = "relato"
        if name not in self.DECK_DATA_CACHING:
            filename = self.caso_dat.arquivos
            if not filename:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            self._log.info(f"Reading file: {filename}")
            self.DECK_DATA_CACHING[name] = Relato.read(f"relato.{filename}")
        return self.DECK_DATA_CACHING[name]

    @property
    def inviab_unic(self) -> InviabUnic:
        name = "inviab_unic"
        if name not in self.DECK_DATA_CACHING:
            filename = self.caso_dat.arquivos
            if not filename:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            self._log.info(f"Reading file: {filename}")
            self.DECK_DATA_CACHING[name] = InviabUnic.read(
                f"inviab_unic.{filename}"
            )
        return self.DECK_DATA_CACHING[name]

    @property
    def inviab(self) -> InviabUnic:
        name = "inviab"
        if name not in self.DECK_DATA_CACHING:
            filename = self.caso_dat.arquivos
            if not filename:
                msg = f"No content found in {self.MODEL_ENTRY_FILE}"
                self._log.error(msg)
                raise FileNotFoundError(msg)
            self._log.info(f"Reading file: {filename}")
            self.DECK_DATA_CACHING[name] = InviabUnic.read(f"inviab.{filename}")
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
            # Downloads parent metadata and check if is a NEWAVE execution
            # with SUCCESS status
            self._log.info(
                f"Fetching parent data from ID {parent_id} in"
                + f" {join(bucket, OUTPUTS_PREFIX, parent_id)}..."
            )
            remote_filepath = join(OUTPUTS_PREFIX, parent_id, METADATA_FILE)
            parent_metadata = json.loads(
                check_and_get_bucket_item(bucket, remote_filepath, self._log)
            )
            if any(
                [
                    k not in parent_metadata
                    for k in [
                        METADATA_MODEL_NAME,
                        METADATA_STATUS,
                        METADATA_STUDY_STARTING_DATE,
                    ]
                ]
            ):
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

    def _cut_by_stage_filename(self, ending_date: datetime) -> str:
        def diff_month(d1: datetime, d2: datetime) -> int:
            return (d1.year - d2.year) * 12 + d1.month - d2.month

        metadata = self._update_metadata({})
        if METADATA_PARENT_STARTING_DATE in metadata:
            parent_starting_date_str = metadata[METADATA_PARENT_STARTING_DATE]
        else:
            self._log.warning("Parent starting date could not be obtained")
            return ""

        parent_starting_date = datetime.fromisoformat(parent_starting_date_str)
        parent_starting_month = parent_starting_date.month
        months_between_cases = diff_month(ending_date, parent_starting_date)
        ending_month = parent_starting_month + months_between_cases - 1
        filename = f"cortes-{str(ending_month).zfill(3)}.dat"
        if ending_month <= 0:
            self._log.warning(
                f"Error obtaining cut by stage filename: {filename}"
            )
        return filename

    def _get_cut_filenames_for_extraction(self) -> list[str]:
        # Considers NEWAVE naming rule of cortes-<stage>.dat,
        # where <stage> is actually the calendar month counter
        dadger = self.dadger
        dt = dadger.dt
        if not dt:
            raise ValueError("Dadger file without DT")
        year = dt.ano
        month = dt.mes
        day = dt.dia
        if not year or not month or not day:
            raise ValueError("Error processing dadger DT register")
        starting_date = datetime(year=year, month=month, day=day)
        dp: pd.DataFrame = dadger.dp(df=True)
        dp = dp.drop_duplicates(["estagio"])
        hour_cols = [c for c in dp.columns if "duracao" in c]
        total_hours = dp[hour_cols].to_numpy().flatten().sum()
        ending_date = starting_date + timedelta(hours=total_hours)
        return [
            self.CUT_HEADER_FILE,
            self.CUT_FULL_FILE,
            self._cut_by_stage_filename(ending_date),
        ]

    def extract_sanitize_inputs(self, compressed_input_file: str):
        compressed_input_file = compressed_input_file.split("/")[-1]
        extracted_files = (
            extract_zip_content(compressed_input_file)
            if isfile(compressed_input_file)
            else []
        )
        self._log.info(f"Extracted input files: {extracted_files}")
        code, _ = run_in_terminal(
            [join(MODEL_EXECUTABLE_DIRECTORY, self.NAMECAST_PROGRAM_NAME)],
            log_output=True,
        )
        if code != 0:
            self._log.warning(
                f"Running {self.NAMECAST_PROGRAM_NAME} resulted in:"
            )

        self._log.info("Forcing encoding to utf-8")
        for f in listdir():
            cast_encoding_to_utf8(f)

        # Unzips the parent files
        for parent_file, files_to_extract in {
            self.CUT_FILE: self._get_cut_filenames_for_extraction(),
        }.items():
            if isfile(parent_file):
                extracted_files = extract_zip_content(
                    parent_file, members=files_to_extract
                )
                self._log.info(f"Extracted parent files: {extracted_files}")

    def generate_unique_input_id(self, version: str, parent_id: str):
        file_hash, hashed_files = hash_all_files_in_path(
            file_regexes_to_ignore=[
                r".*\.modelops",
                r".*\.log",
                r".*\.lic",
                r".*\.zip",
                r"cortes.*\.dat",
            ]
        )
        self._log.info(f"Files considered for ID: {hashed_files}")
        unique_id = hash_string(
            "".join(
                [
                    self.MODEL_NAME,
                    hash_string(version),
                    parent_id,
                    file_hash,
                ]
            )
        )

        with open(EXECUTION_ID_FILE, "w") as f:
            f.write(unique_id)
        self._update_metadata({METADATA_MODELOPS_ID: unique_id})

        return unique_id

    def preprocess(self):
        dadger = self.dadger
        if isfile(self.CUT_HEADER_FILE):
            dadger.fc(tipo="NEWV21").caminho = self.CUT_HEADER_FILE
            self._log.info(
                f"Overwriting cut header path: {self.CUT_HEADER_FILE}"
            )
            cut_by_stage_files = [f for f in listdir() if "cortes-" in f]
            if len(cut_by_stage_files) > 0:
                cut_file = cut_by_stage_files[0]
            else:
                cut_file = self.CUT_FULL_FILE
            dadger.fc(tipo="NEWCUT").caminho = cut_file
            self._log.info(f"Overwriting cut path: {cut_file}")
        dadger.write(self.arquivos_dat.dadger)

    def _evaluate_data_error(self, relato: Relato) -> bool:
        return any(
            [
                self.DATA_ERROR_PATTERN in b.data
                for b in relato.data.of_type(DefaultBlock)
            ]
        )

    def _evaluate_max_iterations(self, relato: Relato) -> bool:
        return any(
            [
                self.MAX_ITERATIONS_PATTERN in b.data
                for b in relato.data.of_type(DefaultBlock)
            ]
        )

    def _evaluate_relato_outputs(self, relato: Relato) -> bool:
        return relato.cmo_medio_submercado is None

    def _evaluate_negative_gap(self, relato: Relato) -> bool:
        return any(
            [
                self.NEGATIVE_GAP_PATTERN in b.data
                for b in relato.data.of_type(DefaultBlock)
            ]
        )

    def _evaluate_feasibility(
        self, inviab_file: InviabUnic, dadger: Dadger
    ) -> bool:
        if not isinstance(inviab_file, InviabUnic):
            return True
        inviabs_sf = inviab_file.inviabilidades_simulacao_final
        if inviabs_sf is None:
            return False
        elif inviabs_sf.empty:
            return False
        else:
            # PREMISSA: So é inviável se tem déficit e tem RHE
            mensagens = inviabs_sf["restricao"].tolist()
            deficits = ["DEFICIT" in m for m in mensagens]
            if len(deficits) > 0 and all(deficits):
                return dadger.he() is not None
            else:
                return True

    def run(
        self, queue: str, core_count: int, mpich_path: str, slurm_path: str
    ):
        self._log.info(f"Job script file: {self.DECOMP_JOB_PATH}")
        environ["PATH"] += ":" + ":".join([mpich_path, slurm_path])
        job_id = submit_job(queue, core_count, self.DECOMP_JOB_PATH)
        if job_id:
            follow_submitted_job(job_id, self.DECOMP_JOB_TIMEOUT)

    def generate_execution_status(self, job_id: str) -> str:
        self._log.info("Reading 'dadger' file for generating status...")
        dadger = self.dadger
        self._log.info("Reading 'relato' file for generating status...")
        relato = self.relato
        self._log.info(
            "Reading 'inviab_unic' or 'inviab' file for generating status..."
        )
        inviab_unic = self.inviab_unic
        inviab = self.inviab

        status = RunStatus.SUCCESS

        if self._evaluate_data_error(relato):
            status = RunStatus.DATA_ERROR
        elif self._evaluate_max_iterations(relato):
            status = RunStatus.RUNTIME_ERROR
        elif self._evaluate_feasibility(inviab_unic, dadger) or (
            self._evaluate_feasibility(inviab, dadger)
        ):
            status = RunStatus.INFEASIBLE
        elif self._evaluate_negative_gap(relato):
            status = RunStatus.RUNTIME_ERROR
        elif self._evaluate_relato_outputs(relato):
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
        caso_dat = self.caso_dat
        arquivos_dat = self.arquivos_dat
        dadger = self.dadger
        common_input_files = [
            self.MODEL_ENTRY_FILE,
            caso_dat.arquivos,
            arquivos_dat.dadger,
            arquivos_dat.dadgnl,
            arquivos_dat.vazoes,
            arquivos_dat.mlt,
            arquivos_dat.hidr,
            arquivos_dat.perdas,
        ]
        index_file = [dadger.fa.arquivo] if dadger.fa else []
        libs_input_files = (
            pd.read_csv(index_file[0], delimiter=";", comment="&", header=None)[
                2
            ]
            .unique()
            .tolist()
            if len(index_file) == 1
            else []
        )
        polinjusdat_file = [dadger.fj.arquivo] if dadger.fj else []
        speed_file = [dadger.vt.arquivo] if dadger.vt else []

        input_files = (
            common_input_files
            + index_file
            + polinjusdat_file
            + speed_file
            + [a.strip() for a in libs_input_files]
        )
        input_files = [a for a in input_files if a is not None]
        self._log.info(f"Files considered as input: {input_files}")
        return input_files

    def _list_report_files(self, input_files: list[str]) -> list[str]:
        extension = self.caso_dat.arquivos
        if extension is None:
            raise ValueError("File extension not found")

        report_output_files = [
            "decomp.tim",
            "relato." + extension,
            "sumario." + extension,
            "relato2." + extension,
            "inviab_unic." + extension,
            "inviab." + extension,
            "relgnl." + extension,
            "custos." + extension,
            "avl_cortesfpha_dec." + extension,
            "dec_desvfpha." + extension,
            "dec_estatfpha." + extension,
            "energia." + extension,
            "log_desvfpha_dec." + extension,
            "outgnl." + extension,
            "memcal." + extension,
            "runstate.dat",
            "runtrace.dat",
            "eco_fpha_." + extension,
            "dec_eco_desvioagua.csv",
            "dec_eco_discr.csv",
            "dec_eco_evap.csv",
            "dec_eco_qlat.csv",
            "dec_eco_cotajus.csv",
            "avl_turb_max.csv",
            "dec_avl_evap.csv",
            "dec_cortes_evap.csv",
            "dec_estatevap.csv",
            "fcfnwi." + extension,
            "fcfnwn." + extension,
            "cmdeco." + extension,
            "indice_saida.csv",
            "mensagens.csv",
            "mensagensErro.txt",
        ]
        report_output_file_regex = [
            r"^osl_.*$",
            r"^eco_.*\.csv$",
            r"^dec_fcf_cortes.*$",
            r"^avl_desvfpha_v_q_.*$",
            r"^avl_desvfpha_s_.*$",
        ]
        report_output_files = [a for a in report_output_files if a is not None]
        report_output_files += list_files_by_regexes(
            input_files, report_output_file_regex
        )
        self._log.info(f"Files considered as report: {report_output_files}")
        return report_output_files

    def _list_operation_files(self, input_files: list[str]) -> list[str]:
        operation_output_file_regex = [
            r"^bengnl.*\.csv$",
            r"^dec_oper.*\.csv$",
            r"^energia_acopla.*\.csv$",
            r"^balsub.*\.csv$",
            r"^cei.*\.csv$",
            r"^cmar.*\.csv$",
            r"^contratos.*\.csv$",
            r"^ener.*\.csv$",
            r"^ever.*\.csv$",
            r"^evnt.*\.csv$",
            r"^flx.*\.csv$",
            r"^hidrpat.*\.csv$",
            r"^pdef.*\.csv$",
            r"^qnat.*\.csv$",
            r"^qtur.*\.csv$",
            r"^term.*\.csv$",
            r"^usina.*\.csv$",
            r"^ute.*\.csv$",
            r"^vert.*\.csv$",
            r"^vutil.*\.csv$",
            r"^oper_.*\.csv$",
        ]
        operation_output_files = list_files_by_regexes(
            input_files, operation_output_file_regex
        )
        self._log.info(
            f"Files considered as operation: {operation_output_files}"
        )
        return operation_output_files

    def _list_cuts_files(self, input_files: list[str]) -> list[str]:
        extension = self.caso_dat.arquivos
        if extension is None:
            raise ValueError("File extension not found")

        cuts_output_files = [
            "cortdeco." + extension,
            "mapcut." + extension,
        ]
        self._log.info(f"Files considered as cuts: {cuts_output_files}")
        return cuts_output_files

    def _cleanup_files(
        self,
        input_files: list[str],
        report_files: list[str],
        operation_files: list[str],
        cuts_files: list[str],
    ):
        extension = self.caso_dat.arquivos
        if extension is None:
            raise ValueError("File extension not found")

        keeping_files = input_files + [
            "decomp.tim",
            "relato." + extension,
            "sumario." + extension,
            "relato2." + extension,
            "inviab_unic." + extension,
            "inviab." + extension,
            "relgnl." + extension,
            "custos." + extension,
            "dec_oper_usih.csv",
            "dec_oper_usit.csv",
            "dec_oper_ree.csv",
            "dec_oper_sist.csv",
        ]
        keeping_files = [a for a in keeping_files if a is not None]
        compressed_files = (
            input_files + report_files + operation_files + cuts_files
        )
        cleaning_files = [a for a in compressed_files if a not in keeping_files]
        cleaning_files_regex = [
            r"^dimpl_.*$",
            r"^cad.*$",
            r"^debug.*$",
            r"^inviab_0.*$",
            r"^svc.*$",
            r"^deco_.*\.msg$",
            r"^SAIDA_MENSAGENS.*$",
            r"^vazmsg.*$",
        ]
        cleaning_files += list_files_by_regexes(
            input_files, cleaning_files_regex
        )
        cleaning_files += [
            "decomp.lic",
            "cusfut." + extension,
            "deconf." + extension,
            "CONVERG.TMP",
        ]
        self._log.info(f"Cleaning files: {cleaning_files}")
        clean_files(cleaning_files)

    def metadata_generation(self) -> dict[str, Any]:
        te = self.dadger.te
        dt = self.dadger.dt
        if not te:
            raise ValueError("TE register not found in <dadger>")
        if not dt:
            raise ValueError("DT register not found in <dadger>")
        year = dt.ano
        month = dt.mes
        day = dt.dia
        if year is None:
            raise ValueError("DT register with incomplete info (year)")
        if month is None:
            raise ValueError("DT register with incomplete info (month)")
        if day is None:
            raise ValueError("DT register with incomplete info (day)")
        study_starting_date = datetime(year, month, day, tzinfo=UTC)
        study_name = te.titulo

        metadata = {
            METADATA_STUDY_STARTING_DATE: study_starting_date.isoformat(),
            METADATA_STUDY_NAME: study_name if study_name else "",
        }
        return self._update_metadata(metadata)

    def output_compression_and_cleanup(self, num_cpus: int):
        with time_and_log("Output compression and cleanup", logger=self._log):
            input_files = self._list_input_files()
            compress_files_to_zip(input_files, "deck")
            # Moves content from DECOMP subdirectories to root
            for d in ["out"]:
                moves_content_to_rootdir(d)
            # Parallel compression
            operation_files = self._list_operation_files(input_files)
            compress_files_to_zip_parallel(
                operation_files, "operacao", num_cpus
            )
            report_files = self._list_report_files(input_files)
            compress_files_to_zip_parallel(report_files, "relatorios", num_cpus)
            cuts_files = self._list_cuts_files(input_files)
            self._cleanup_files(
                input_files, report_files, operation_files, cuts_files
            )

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


ModelFactory().register(DECOMP.MODEL_NAME, DECOMP)
