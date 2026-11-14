"""Slack notifications for Airflow DAG failures.

The webhook URL is a credential, so it comes from the environment and never from
the repo.
"""

import json
import os
from urllib.error import URLError
from urllib.request import Request, urlopen

TIMEOUT_SECONDS = 10


def send_slack_notification(message: str, webhook_url: str | None = None) -> None:
    """POST a plain-text message to a Slack incoming webhook.

    Raises ValueError when there is no webhook URL or the message is empty, and
    RuntimeError when Slack rejects it. Callers that must not fail -- Airflow
    callbacks -- should catch.
    """
    url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        raise ValueError("SLACK_WEBHOOK_URL is not set")
    if not message.strip():
        raise ValueError("message is empty")

    request = Request(
        url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            result = response.read().decode("utf-8").strip()
    except URLError as exc:
        raise RuntimeError(f"Slack request failed: {exc}") from exc

    # A 200 with a non-"ok" body is a failure too: Slack reports some errors
    # that way instead of with an HTTP status.
    if result.lower() != "ok":
        raise RuntimeError(f"Slack rejected the message: {result}")


def notify_failure(context: dict) -> None:
    """Airflow callback: send one message per failed task or DAG run.

    Never raises. It runs while Airflow is already handling a failure, and a
    second traceback in that log only buries the first.
    """
    try:
        send_slack_notification(_describe_failure(context))
    except Exception as exc:
        print(f"Slack alert not sent: {exc}")


def _describe_failure(context: dict) -> str:
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")

    # run_after, not logical_date: manual runs have no logical_date.
    run_after = getattr(dag_run, "run_after", None)
    when = f"{run_after:%Y-%m-%d %H:%M UTC}" if run_after else "unknown time"

    where = getattr(task_instance, "task_id", None) or "dag run"
    map_index = getattr(task_instance, "map_index", -1)
    if map_index is not None and map_index >= 0:
        where = f"{where}[{map_index}]"  # mapped task: which table failed
    attempt = getattr(task_instance, "try_number", None)

    lines = [
        f":red_circle: *{getattr(dag_run, 'dag_id', 'pipeline')}* failed",
        f"task: `{where}`" + (f" (attempt {attempt})" if attempt else ""),
        f"run: `{getattr(dag_run, 'run_id', 'unknown')}`",
        f"at: {when}",
    ]

    error = context.get("exception")
    if error is not None:
        lines.append(f"error: {type(error).__name__}: {str(error)[:200]}")

    return "\n".join(lines)
