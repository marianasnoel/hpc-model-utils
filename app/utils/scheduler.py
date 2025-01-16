import re
from logging import Logger

from app.utils.constants import SLURM_SUBMISSION_REGEX_PATTERN
from app.utils.terminal import run_in_terminal


def submit_job(
    queue: str, core_count: int, job_path: str, logger: Logger
) -> str:
    status_code, output = run_in_terminal(
        [
            "sbatch",
            f"--partition={queue}",
            "--contiguous",
            "--job-name=`basename $PWD`",
            '--output="stdout.modelops"',
            '--error="stderr.modelops"',
            "--cpus-per-task=1",
            "--ntasks",
            str(core_count),
            job_path,
            str(core_count),
            "2>&1",
        ],
        logger=logger,
    )
    if status_code == 0:
        groups = re.match(SLURM_SUBMISSION_REGEX_PATTERN, output[0]).groups()
        if len(groups) == 0:
            raise RuntimeError("Error matching job submission output")
        return groups[0]


def follow_submitted_job(job_id: str, timeout: float):
    status_code, _ = run_in_terminal(
        [
            f"while squeue | grep {job_id} > /dev/null ;do",
            "if [ -e stdout.modelops ];",
            "then tail -n 10 stdout.modelops;",
            f"else squeue -a -j {job_id};  fi; sleep 5; done 2>&1",
        ],
        timeout=timeout,
        last_lines_diff=50,
    )
    if status_code != 0:
        raise RuntimeError(f"Error following submitted job: {status_code}")
