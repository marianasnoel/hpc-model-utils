from logging import Logger

from app.utils.terminal import run_in_terminal


def follow_submitted_job(job_id: str, timeout: float, logger: Logger):
    run_in_terminal()
