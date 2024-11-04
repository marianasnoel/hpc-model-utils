from os import getenv, listdir
from os.path import join
from pathlib import Path
from shutil import move

import pandas as pd
from inewave.newave import Arquivos, Caso, Dger, Pmo

from app.adapter.repository.abstractmodel import (
    AbstractModel,
    ModelFactory,
)
from app.models.runstatus import RunStatus
from app.utils.constants import (
    AWS_ACCESS_KEY_ID_ENV,
    AWS_SECRET_ACCESS_KEY_ENV,
    MODEL_EXECUTABLE_DIRECTORY,
    MODEL_EXECUTABLE_PERMISSIONS,
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
from app.utils.s3 import check_items_in_bucket, download_bucket_items
from app.utils.terminal import cast_encoding_to_utf8, run_in_terminal
from app.utils.timing import time_and_log


class NEWAVE(AbstractModel):
    MODEL_NAME = "newave"
    VERSION_BUCKET = "hpc-ons-arquivos-newave"
    VERSION_PREFIX = "versoes"
    NAMECAST_PROGRAM_NAME = "ConverteNomesArquivos"
    MODEL_ENTRY_FILE = "caso.dat"
    NWLISTCF_ENTRY_FILE = "arquivos.dat"
    LIBS_ENTRY_FILE = "indices.csv"
    STATUS_DIAGNOSIS_FILE = "model_status.tmp"
    NWLISTCF_EXECUTABLE = join(MODEL_EXECUTABLE_DIRECTORY, "nwlistcf")
    NWLISTOP_EXECUTABLE = join(MODEL_EXECUTABLE_DIRECTORY, "nwlistop")
    NWLISTCF_NWLISTOP_TIMEOUT = 600

    def check_and_fetch_executables(self, version: str):
        self._log.info(f"Fetching executables for version {version}...")
        prefix_with_version = join(self.VERSION_PREFIX, version)
        item_prefixes = check_items_in_bucket(
            self.VERSION_BUCKET,
            prefix_with_version,
            aws_access_key_id=getenv(AWS_ACCESS_KEY_ID_ENV),
            aws_secret_access_key=getenv(AWS_SECRET_ACCESS_KEY_ENV),
        )
        if len(item_prefixes) == 0:
            self._log.warning(f"No executables found for version {version}")
            return
        else:
            self._log.debug(f"Found items: {item_prefixes}")

        downloaded_filepaths = download_bucket_items(
            self.VERSION_BUCKET,
            item_prefixes,
            MODEL_EXECUTABLE_DIRECTORY,
            aws_access_key_id=getenv(AWS_ACCESS_KEY_ID_ENV),
            aws_secret_access_key=getenv(AWS_SECRET_ACCESS_KEY_ENV),
        )
        if len(downloaded_filepaths) != len(item_prefixes):
            self._log.warning("Failed to download some of the executables!")
            return
        else:
            self._log.debug(f"Downloaded items to: {downloaded_filepaths}")

        for filepath in downloaded_filepaths:
            change_file_permission(filepath, MODEL_EXECUTABLE_PERMISSIONS)
            self._log.debug(
                f"Changed {filepath} permissions to"
                + f" {MODEL_EXECUTABLE_PERMISSIONS:o}"
            )
        self._log.info("Executables successfully fetched and ready!")

    def extract_sanitize_inputs(self, compressed_input_file: str):
        extracted_files = extract_zip_content(compressed_input_file)
        self._log.debug(f"Extracted input files: {extracted_files}")
        code, output = run_in_terminal([
            join(MODEL_EXECUTABLE_DIRECTORY, self.NAMECAST_PROGRAM_NAME)
        ])
        if code != 0:
            self._log.warning(
                f"Running {self.NAMECAST_PROGRAM_NAME} resulted in:"
            )
            for o in output:
                self._log.warning(o)
        else:
            for o in output:
                self._log.info(o)

        self._log.debug("Forcing encoding to utf-8")
        for f in listdir():
            cast_encoding_to_utf8(f)

    def generate_unique_input_id(self, version: str):
        return hash_string(
            "".join([
                self.MODEL_NAME,
                hash_string(version),
                hash_all_files_in_path(),
            ])
        )

    def preprocess(self):
        # LP manager path
        path = str(Path(MODEL_EXECUTABLE_DIRECTORY).resolve())
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        caso_dat.gerenciador_processos = path
        caso_dat.write(self.MODEL_ENTRY_FILE)

    def generate_execution_status(self) -> str:
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        pmo_dat = Pmo.read(arquivos_dat.pmo)
        # TODO - make status generation dependent on
        # what execution kind was selected
        status = RunStatus.SUCCESS
        if pmo_dat.custo_operacao_series_simuladas is None:
            status = RunStatus.DATA_ERROR

        status_value = status.value
        with open(self.STATUS_DIAGNOSIS_FILE, "w") as f:
            f.write(status_value)
        return status_value

    def _generate_nwlistcf_arquivos_dat_file(self):
        lines = [
            "ARQUIVO DE DADOS GERAIS     : nwlistcf.dat\n",
            "ARQUIVO DE CORTES DE BENDERS: cortes.dat\n",
            "ARQUIVO DE CABECALHO CORTES : cortesh.dat\n",
            "ARQUIVO P/DESPACHO HIDROTERM: newdesp.dat\n",
            "ARQUIVO DE ESTADOS CORTES   : cortese.dat\n",
            "ARQUIVO DE ENERGIAS FORWARD : energiaf.dat\n",
            "ARQUIVO DE RESTRICOES SAR   : rsar.dat\n",
            "ARQUIVO DE CABECALHO SAR    : rsarh.dat\n",
            "ARQUIVO DE INDICE SAR       : rsari.dat\n",
            "ARQUIVO LISTAGEM CORTES     : nwlistcf.rel\n",
            "ARQUIVO LISTAGEM ESTADOS FCF: estados.rel\n",
            "ARQUIVO LISTAGEM SAR        : rsar.rel\n",
            "ARQUIVO DE ENERGIAS X FORW  : energiaxf.dat\n",
            "ARQUIVO DE VAZAO FORWARD    : vazaof.dat\n",
            "ARQUIVO DE VAZAO X FORWARD  : vazaoxf.dat\n",
        ]
        with open(self.NWLISTCF_ENTRY_FILE, "w") as arq:
            arq.writelines(lines)

    def _generate_nwlistcf_dat_file(self, stage: int):
        previous_lines = [
            " INI FIM FC (FC = 1: IMPRIME TODOS CORTES, FC = 0: IMPRIME APENAS CORTES VALIDOS NA ULTIMA ITERACAO)\n",
            " XXX XXX X\n",
        ]
        following_lines = [
            " OPCOES DE IMPRESSAO : 01 - CORTES FCF  02 - ESTADOS FCF  03 - RESTRICAO SAR\n",
            " XX XX XX (SE 99 CONSIDERA TODAS)\n",
            " 01 02\n",
        ]
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        dger_dat = Dger.read(arquivos_dat.dger)
        if stage < 0:
            stage = (dger_dat.num_anos_estudo * 12) - (stage + 1)
        else:
            stage = dger_dat.mes_inicio_estudo + stage - 1
        month = str(stage).zfill(2)
        self._log.info(f"Generating nwlistcf.dat for month: {month}")
        with open("nwlistcf.dat", "w") as arq:
            arq.writelines(previous_lines)
            arq.write(f"  {month}  {month} 1\n")
            arq.writelines(following_lines)

    def _generate_nwlistop_dat_file(self, option: int):
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        dger = Dger.read(arquivos_dat.dger)
        initial_stage = dger.num_anos_pre_estudo * 12 + 1
        final_stage = (
            dger.num_anos_estudo * 12
            + dger.num_anos_pos_sim_final * 12
            - (dger.mes_inicio_estudo - 1)
        )
        self._log.info(
            f"Generating nwlistop.dat option {option} between "
            + f"stages: {initial_stage} - {final_stage}"
        )
        lines = [
            f" {option}\n",
            "FORWARD  (ARQ. DE DADOS)    : forward.dat\n",
            "FORWARDH (ARQ. CABECALHOS)  : forwarh.dat\n",
            "NEWDESP  (REL. CONFIGS)     : newdesp.dat\n",
            "-----------------------------------------\n",
            " XXX XXX    PERIODOS INICIAL E FINAL\n",
            f" {str(initial_stage).zfill(3)} {str(final_stage).zfill(3)}\n",
            " 1-CMO           2-DEFICITS         3-ENA CONTROL.   4-EARM FINAL       5-ENA FIO BRUTA 6-EVAPORACAO    7-VERTIMENTO\n",
            " 8-VAZAO MIN.    9-GER.HIDR.CONT   10-GER. TERMICA  11-INTERCAMBIOS    12-MERC.LIQ.    13-VALOR AGUA   14-VOLUME MORTO\n",
            "15-EXCESSO      16-GHMAX           17-OUTROS USOS   18-BENEF.INT/AGR   19-F.CORR.EC    20-GHTOTAL      21-ENA BRUTA\n",
            "22-ACOPLAMENTO  23-INVASAO CG      24-PENAL.INV.CG. 25-ACIONAMENTO MAR 26-COPER        27-CTERM        28-CDEFICIT\n",
            "29-GER.FIO LIQ. 30-PERDA FIO       31-ENA FIO LIQ.  32- BENEF. GNL     33-VIOL.GHMIN   34-PERDAS       37-GEE             38-SOMA AFL.PAS.\n",
            " XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX (SE 99 CONSIDERA TODAS)\n",
            " 99\n",
            "-----------------------------------------------------------------------------------------------------------------------\n",
            " 1-VOL.ARMAZ       2-GER.HID         3-VOL.TURB.     4-VOL. VERT.      5-VIOL.GHMIN    6-ENCH.MORTO   7-FOLGA DEPMIN.\n",
            " 8-DESV. AGUA      9-DESV. POS.      10-DESVIO NEG.  11-FOLGA FPGHA   12-VAZAO AFL.  13-VAZAO INCREM. 14-VARM PCT.\n",
            " XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX (SE 99 CONSIDERA TODAS)\n",
            " 99\n",
            " XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX  (SE 999 CONSIDERA TODAS AS USINAS)\n",
            " 999\n",
        ]
        with open("nwlistop.dat", "w") as arq:
            arq.writelines(lines)

    def _run_nwlistcf(self, stage: int):
        try:
            tmp_filename = "arquivos.dat.bkp"
            caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
            self._generate_nwlistcf_dat_file(stage)
            move(caso_dat.arquivos, tmp_filename)
            self._generate_nwlistcf_arquivos_dat_file()
            self._log.info("Running NWLISTCF")
            status_code, output = run_in_terminal(
                [self.NWLISTCF_EXECUTABLE],
                timeout=self.NWLISTCF_NWLISTOP_TIMEOUT,
            )
            if status_code != 0:
                for o in output:
                    self._log.warning(o)

        except Exception as e:
            self._log.warning(f"Error running NWLISTCF: {str(e)}")
        finally:
            move(self.NWLISTCF_ENTRY_FILE, "arquivos-nwlistcf.dat")
            move(tmp_filename, caso_dat.arquivos)

    def _run_nwlistop(self, option: int):
        try:
            self._generate_nwlistop_dat_file(option)
            self._log.info(f"Running NWLISTOP option {option}")
            status_code, output = run_in_terminal(
                [self.NWLISTOP_EXECUTABLE],
                timeout=self.NWLISTCF_NWLISTOP_TIMEOUT,
            )
            if status_code != 0:
                for o in output:
                    self._log.warning(o)

        except Exception as e:
            self._log.warning(f"Error running NWLISTOP: {str(e)}")

    def postprocess(self):
        with time_and_log("Running NWLISTCF and NWLISTOP", logger=self._log):
            self._run_nwlistcf(stage=2)
            self._run_nwlistop(option=2)
            self._run_nwlistop(option=4)

    def _list_input_files(self) -> list[str]:
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        common_input_files = [
            self.MODEL_ENTRY_FILE,
            caso_dat.arquivos,
            arquivos_dat.adterm,
            arquivos_dat.agrint,
            arquivos_dat.c_adic,
            arquivos_dat.cvar,
            arquivos_dat.sar,
            arquivos_dat.clast,
            arquivos_dat.confhd,
            arquivos_dat.conft,
            arquivos_dat.curva,
            arquivos_dat.dger,
            arquivos_dat.dsvagua,
            arquivos_dat.vazpast,
            arquivos_dat.exph,
            arquivos_dat.expt,
            arquivos_dat.ghmin,
            arquivos_dat.gtminpat,
            "hidr.dat",
            arquivos_dat.perda,
            arquivos_dat.manutt,
            arquivos_dat.modif,
            arquivos_dat.patamar,
            arquivos_dat.penalid,
            "postos.dat",
            arquivos_dat.shist,
            arquivos_dat.sistema,
            arquivos_dat.term,
            "vazoes.dat",
            arquivos_dat.tecno,
            "selcor.dat",
            arquivos_dat.re,
            arquivos_dat.ree,
            arquivos_dat.clasgas,
            arquivos_dat.abertura,
            arquivos_dat.gee,
            "dbgcortes.dat",
            "volref_saz.dat",
            arquivos_dat.cortesh_pos_estudo,
            arquivos_dat.cortes_pos_estudo,
        ]
        index_file = (
            [self.LIBS_ENTRY_FILE] if self.LIBS_ENTRY_FILE in listdir() else []
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

        input_files = (
            common_input_files
            + index_file
            + [a.strip() for a in libs_input_files]
        )
        input_files = [a for a in input_files if a is not None]

        return input_files

    def _list_nwlistop_files(self, input_files: list[str]) -> list[str]:
        nwlistop_output_file_regex = [
            r"^.*\.CSV$",
            r"^.*\.out$",
        ]
        nwlistop_files = ["nwlistop.dat"]
        nwlistop_files += list_files_by_regexes(
            input_files, nwlistop_output_file_regex
        )
        return nwlistop_files

    def _list_report_files(self, input_files: list[str]) -> list[str]:
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        report_output_files = [
            arquivos_dat.pmo,
            arquivos_dat.parp,
            arquivos_dat.dados_simulacao_final,
            "newave.tim",
            "nwv_avl_evap.csv",
            "nwv_cortes_evap.csv",
            "nwv_eco_evap.csv",
            "evap_avl_desv.csv",
            "evap_eco.csv",
            "evap_cortes.csv",
            "boots.rel",
            "consultafcf.rel",
            "eco_fpha_.dat",
            "eco_fpha.csv",
            "fpha_eco.csv",
            "fpha_cortes.csv",
            "avl_cortesfpha_nwv.dat",
            "avl_cortesfpha_nwv.csv",
            "parpeol.dat",
            "parpvaz.dat",
            "runtrace.dat",
            "runstate.dat",
            "prociter.rel",
            "CONVERG.TMP",
            "ETAPA.TMP",
            "TAREFA.TMP",
        ]
        report_output_file_regex = [
            r"^alertainv.*\.rel$",
            r"^cativo_.*\.rel$",
            r"^avl_desvfpha.*\.dat$",
            r"^avl_desvfpha.*\.csv$",
            r"^newave_.*\.log$",
            r"^nwv_.*\.rel$",
        ]
        report_output_files = [a for a in report_output_files if a is not None]
        report_output_files += list_files_by_regexes(
            input_files, report_output_file_regex
        )
        return report_output_files

    def _list_resource_files(self, input_files: list[str]) -> list[str]:
        resource_output_file_regex = [
            r"^energiaf.*\.dat$",
            r"^energiaaf.*\.dat$",
            r"^energiab.*\.dat$",
            r"^energiaxf.*\.dat$",
            r"^energias.*\.dat$",
            r"^energiaxs.*\.dat$",
            r"^energiap.*\.dat$",
            r"^energiaas.*\.csv$",
            r"^energiaasx.*\.csv$",
            r"^energiaf.*\.csv$",
            r"^energiaaf.*\.csv$",
            r"^energiax.*\.csv$",
            r"^energiaxf.*\.csv$",
            r"^energiaxs.*\.csv$",
            r"^energiap.*\.csv$",
            r"^eng.*\.dat$",
            r"^enavazf.*\.dat$",
            r"^enavazxf.*\.dat$",
            r"^enavazxs.*\.dat$",
            r"^enavazb.*\.dat$",
            r"^enavazs.*\.dat$",
            r"^enavazf.*\.csv$",
            r"^enavazxf.*\.csv$",
            r"^enavazaf.*\.csv$",
            r"^enavazb.*\.csv$",
            r"^enavazs.*\.csv$",
            r"^vazaof.*\.dat$",
            r"^vazaoaf.*\.dat$",
            r"^vazaob.*\.dat$",
            r"^vazaos.*\.dat$",
            r"^vazaoas.*\.dat$",
            r"^vazaoxs.*\.dat$",
            r"^vazaoxf.*\.dat$",
            r"^vazaop.*\.dat$",
            r"^vazaof.*\.csv$",
            r"^vazaoaf.*\.csv$",
            r"^vazaob.*\.csv$",
            r"^vazaos.*\.csv$",
            r"^vazthd.*\.dat$",
            r"^vazinat.*\.dat$",
            r"^ventos.*\.dat$",
            r"^vento.*\.csv$",
            r"^eolicaf.*\.dat$",
            r"^eolicab.*\.dat$",
            r"^eolicas.*\.dat$",
            r"^eolp.*\.dat$",
            r"^eolf.*\.csv$",
            r"^eolb.*\.csv$",
            r"^eolp.*\.csv$",
            r"^eols.*\.csv$",
        ]
        resource_output_files = list_files_by_regexes(
            input_files, resource_output_file_regex
        )
        return resource_output_files

    def _list_cuts_files(self, input_files: list[str]) -> list[str]:
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        cuts_output_files = [
            arquivos_dat.cortesh,
            arquivos_dat.cortes,
            "arquivos-nwlistcf.dat",
            "nwlistcf.rel",
        ]
        cuts_output_file_regex = [r"^cortes\-[0-9]*.*\.dat$"]
        cuts_output_files += list_files_by_regexes(
            input_files, cuts_output_file_regex
        )
        return cuts_output_files

    def _list_states_files(self, input_files: list[str]) -> list[str]:
        states_output_files = ["cortese.dat", "estados.rel"]
        states_output_file_regex = [r"^cortese\-[0-9]*.*\.dat$"]
        states_output_files += list_files_by_regexes(
            input_files, states_output_file_regex
        )
        return states_output_files

    def _list_simulation_files(self, input_files: list[str]) -> list[str]:
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        simulation_output_files = [
            arquivos_dat.forward,
            arquivos_dat.forwardh,
            arquivos_dat.newdesp,
            "planej.dat",
            "daduhe.dat",
            "nwdant.dat",
            "saida.rel",
        ]
        return simulation_output_files

    def _cleanup_files(
        self,
        input_files: list[str],
        nwlistop_files: list[str],
        report_files: list[str],
        resource_files: list[str],
        cuts_files: list[str],
        states_files: list[str],
        simulation_files: list[str],
    ):
        caso_dat = Caso.read(self.MODEL_ENTRY_FILE)
        arquivos_dat = Arquivos.read(caso_dat.arquivos)
        keeping_files = [
            "newave.tim",
            arquivos_dat.pmo,
            arquivos_dat.dados_simulacao_final,
        ]
        keeping_files = [a for a in keeping_files if a is not None]
        compressed_files = (
            input_files
            + nwlistop_files
            + report_files
            + resource_files
            + cuts_files
            + states_files
            + simulation_files
        )
        cleaning_files = [a for a in compressed_files if a not in keeping_files]
        cleaning_files_regex = [r"^svc.*$", r"^fort\..*"]
        cleaning_files += list_files_by_regexes(
            input_files, cleaning_files_regex
        )
        cleaning_files += [
            "nwlistcf.dat",
            "nwlistop.dat",
            "format.tmp",
            "mensag.tmp",
            "NewaveMsgPortug.txt",
            "ConvNomeArqsDados.log",
            "ETAPA.TMP",
            "LEITURA.TMP",
        ]
        clean_files(cleaning_files)

    def output_compression_and_cleanup(self, num_cpus: int):
        with time_and_log("Output compression and cleanup", logger=self._log):
            input_files = self._list_input_files()
            compress_files_to_zip(input_files, "deck")
            # Moves content from NEWAVE subdirectories to root
            for d in ["out", "evaporacao", "fpha", "log"]:
                moves_content_to_rootdir(d)
            # Parallel compression
            nwlistop_files = self._list_nwlistop_files(input_files)
            compress_files_to_zip_parallel(nwlistop_files, "operacao", num_cpus)
            report_files = self._list_report_files(input_files)
            compress_files_to_zip_parallel(report_files, "relatorios", num_cpus)
            resource_files = self._list_resource_files(input_files)
            compress_files_to_zip_parallel(resource_files, "recursos", num_cpus)
            cuts_files = self._list_cuts_files(input_files)
            compress_files_to_zip_parallel(cuts_files, "cortes", num_cpus)
            states_files = self._list_states_files(input_files)
            compress_files_to_zip_parallel(states_files, "estados", num_cpus)
            simulation_files = self._list_simulation_files(input_files)
            compress_files_to_zip_parallel(
                simulation_files, "simulacao", num_cpus
            )
            self._cleanup_files(
                input_files,
                nwlistop_files,
                report_files,
                resource_files,
                cuts_files,
                states_files,
                simulation_files,
            )


ModelFactory().register(NEWAVE.MODEL_NAME, NEWAVE)
