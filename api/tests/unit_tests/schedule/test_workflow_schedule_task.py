"""The schedule poller dispatches every due plan whose next run it can calculate."""

import logging
from collections.abc import Callable, Iterable
from contextlib import nullcontext
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from celery.canvas import Signature
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from libs.datetime_utils import naive_utc_now
from models.enums import AppTriggerType
from models.trigger import AppTrigger, WorkflowSchedulePlan
from schedule import workflow_schedule_task


@pytest.fixture
def dispatched_schedule_ids(monkeypatch: pytest.MonkeyPatch, sqlite_engine: Engine) -> list[str]:
    """Bind the poller to the SQLite database and record the schedule ids it dispatches instead of publishing."""
    dispatched: list[str] = []

    def group(signatures: Iterable[Signature[None]]) -> SimpleNamespace:
        schedule_ids = [signature.args[0] for signature in signatures]
        return SimpleNamespace(apply_async=lambda **_options: dispatched.extend(schedule_ids))

    monkeypatch.setattr(workflow_schedule_task, "db", SimpleNamespace(engine=sqlite_engine))
    monkeypatch.setattr(workflow_schedule_task, "current_app", SimpleNamespace(producer_or_acquire=nullcontext))
    monkeypatch.setattr(workflow_schedule_task, "group", group)
    return dispatched


def _add_due_plan(session: Session, *, timezone: str, next_run_at: datetime) -> WorkflowSchedulePlan:
    tenant_id, app_id, node_id = str(uuid4()), str(uuid4()), "schedule"
    session.add(
        AppTrigger(
            tenant_id=tenant_id,
            app_id=app_id,
            node_id=node_id,
            trigger_type=AppTriggerType.TRIGGER_SCHEDULE,
            title="Schedule",
        )
    )
    plan = WorkflowSchedulePlan(
        app_id=app_id,
        node_id=node_id,
        tenant_id=tenant_id,
        cron_expression="*/5 * * * *",
        timezone=timezone,
        next_run_at=next_run_at,
    )
    session.add(plan)
    session.commit()
    return plan


@pytest.mark.parametrize("batch_size", [100, 1], ids=["same-batch", "own-batch"])
def test_plan_whose_next_run_cannot_be_calculated_does_not_block_other_due_plans(
    batch_size: int,
    dispatched_schedule_ids: list[str],
    sqlite_session: Session,
    config_overrides: Callable[..., None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    config_overrides(WORKFLOW_SCHEDULE_POLLER_BATCH_SIZE=batch_size)
    caplog.set_level(logging.WARNING, logger=workflow_schedule_task.logger.name)
    now = naive_utc_now()
    # The most overdue plan is fetched first; with a batch size of 1 it fills the first batch alone.
    broken_due_at = now - timedelta(minutes=10)
    broken = _add_due_plan(sqlite_session, timezone="Invalid/Timezone", next_run_at=broken_due_at)
    utc = _add_due_plan(sqlite_session, timezone="UTC", next_run_at=now - timedelta(minutes=5))

    workflow_schedule_task.poll_workflow_schedules.run()

    assert dispatched_schedule_ids == [utc.id]
    sqlite_session.expire_all()
    utc_next_run_at = utc.next_run_at
    assert utc_next_run_at is not None
    assert utc_next_run_at > now
    # Skipped for this poll only: it stays due, so the next poll tries it again.
    assert broken.next_run_at == broken_due_at
    assert any(broken.id in record.getMessage() for record in caplog.records)
