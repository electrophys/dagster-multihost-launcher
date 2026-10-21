"""
Postgres-backed compute log manager.

Captured stdout/stderr ("compute logs") are written by the process that runs a
step -- a run container on Host B/C, or the code server on Host D -- while the
webserver that displays them lives on Host A. Dagster's default
LocalComputeLogManager keeps them on the writer's own disk, so the webserver
never sees them. Every run worker already reaches Host A's Postgres for event
log storage, so storing compute logs there makes them visible without any
extra infrastructure (S3, a shared filesystem).

Logs are staged locally while a step runs (as with S3ComputeLogManager) and
uploaded when the step finishes, plus every ``upload_interval`` seconds if set.

    compute_logs:
      module: dagster_multihost_launcher.compute_log_manager
      class: PostgresComputeLogManager
      config:
        postgres_db: {...}     # same shape as storage.postgres.postgres_db
        upload_interval: 30
        max_log_bytes: 10485760
"""

import os
import tempfile
import threading
from collections.abc import Mapping, Sequence
from typing import Any, Optional
from urllib.parse import quote, urlencode

import sqlalchemy as db
from sqlalchemy.pool import NullPool

from dagster import (
    Field,
    Noneable,
    StringSource,
    _check as check,
)
from dagster._core.storage.cloud_storage_compute_log_manager import (
    CloudStorageComputeLogManager,
    PollingComputeLogSubscriptionManager,
)
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.storage.config import pg_config
from dagster._core.storage.local_compute_log_manager import (
    IO_TYPE_EXTENSION,
    LocalComputeLogManager,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils import ensure_dir, ensure_file

TABLE_NAME = "multihost_compute_logs"

# Logs past this size keep only their tail (where failures usually are), so
# one chatty step cannot bloat the database.
DEFAULT_MAX_LOG_BYTES = 10 * 1024 * 1024

_metadata = db.MetaData()

ComputeLogsTable = db.Table(
    TABLE_NAME,
    _metadata,
    db.Column("log_key", db.Text, primary_key=True),
    db.Column("io_type", db.String(16), primary_key=True),
    db.Column("partial", db.Boolean, primary_key=True),
    db.Column("data", db.LargeBinary, nullable=False),
    db.Column("original_size", db.BigInteger, nullable=False),
    db.Column("updated_at", db.DateTime, server_default=db.func.now()),
)


def _url_from_config(config_value: Mapping[str, Any]) -> str:
    """A SQLAlchemy URL from ``postgres_url`` or ``postgres_db`` -- the same
    shape as ``storage.postgres``, so its config can be reused verbatim."""
    if config_value.get("postgres_url"):
        check.invariant(
            "postgres_db" not in config_value,
            "compute_logs config must have exactly one of `postgres_url` or `postgres_db`",
        )
        return config_value["postgres_url"]

    check.invariant(
        "postgres_db" in config_value,
        "compute_logs config must have exactly one of `postgres_url` or `postgres_db`",
    )
    pg = config_value["postgres_db"]
    scheme = pg.get("scheme") or "postgresql"
    url = (
        f"{scheme}://{quote(pg['username'], safe='')}:{quote(pg['password'], safe='')}"
        f"@{pg['hostname']}:{pg.get('port', 5432)}/{pg['db_name']}"
    )
    if pg.get("params"):
        url = f"{url}?{urlencode(pg['params'], quote_via=quote)}"
    return url


class PostgresComputeLogManager(CloudStorageComputeLogManager, ConfigurableClass):
    """Stores compute function stdout/stderr in Postgres so the webserver can
    show logs from runs that executed on other hosts.

    Args:
        postgres_url / postgres_db: Connection, as for ``storage.postgres``.
        should_autocreate_tables: Create the logs table on first use.
        local_dir: Where logs are staged while a step runs. Default: the
            system temp directory.
        upload_interval: Seconds between uploads of in-progress logs, so a
            running step's output shows in the UI (and survives a crash).
            Default: upload only when the step finishes.
        max_log_bytes: Per-file cap; larger logs keep only their tail.
    """

    def __init__(
        self,
        postgres_url: str,
        inst_data: Optional[ConfigurableClassData] = None,
        local_dir: Optional[str] = None,
        upload_interval: Optional[int] = None,
        max_log_bytes: int = DEFAULT_MAX_LOG_BYTES,
        should_autocreate_tables: bool = True,
    ):
        self._inst_data = check.opt_inst_param(
            inst_data, "inst_data", ConfigurableClassData
        )
        self._url = check.str_param(postgres_url, "postgres_url")
        self._upload_interval = check.opt_int_param(upload_interval, "upload_interval")
        self._max_log_bytes = check.int_param(max_log_bytes, "max_log_bytes")
        self._should_autocreate_tables = should_autocreate_tables

        self._local_manager = LocalComputeLogManager(
            local_dir or os.path.realpath(tempfile.gettempdir())
        )
        self._subscription_manager = PollingComputeLogSubscriptionManager(self)

        # Connect lazily: the webserver, daemon and every run worker build this
        # manager, and none should fail to start just because Postgres is slow.
        self._engine: Optional[db.engine.Engine] = None
        self._engine_lock = threading.Lock()

    # -- ConfigurableClass ------------------------------------------------------

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            **pg_config(),
            "local_dir": Field(StringSource, is_required=False),
            "upload_interval": Field(
                Noneable(int), is_required=False, default_value=None
            ),
            "max_log_bytes": Field(
                int, is_required=False, default_value=DEFAULT_MAX_LOG_BYTES
            ),
        }

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData, config_value: Mapping[str, Any]
    ) -> "PostgresComputeLogManager":
        return cls(
            postgres_url=_url_from_config(config_value),
            inst_data=inst_data,
            local_dir=config_value.get("local_dir"),
            upload_interval=config_value.get("upload_interval"),
            max_log_bytes=config_value.get("max_log_bytes", DEFAULT_MAX_LOG_BYTES),
            should_autocreate_tables=config_value.get("should_autocreate_tables", True),
        )

    # -- Database -------------------------------------------------------------

    @property
    def engine(self) -> db.engine.Engine:
        with self._engine_lock:
            if self._engine is None:
                engine = db.create_engine(self._url, poolclass=NullPool)
                if self._should_autocreate_tables:
                    self._create_table(engine)
                self._engine = engine
            return self._engine

    @staticmethod
    def _create_table(engine: db.engine.Engine) -> None:
        # Many processes start at once (webserver, daemon, concurrent runs), so
        # another may win the race to create the table; that's fine.
        try:
            _metadata.create_all(engine, checkfirst=True)
        except db.exc.DBAPIError:
            if not db.inspect(engine).has_table(TABLE_NAME):
                raise

    @staticmethod
    def _key(log_key: Sequence[str]) -> str:
        return "/".join(log_key)

    @staticmethod
    def _row_filter(log_key: Sequence[str], io_type: ComputeIOType, partial: bool):
        return db.and_(
            ComputeLogsTable.c.log_key == PostgresComputeLogManager._key(log_key),
            ComputeLogsTable.c.io_type == IO_TYPE_EXTENSION[io_type],
            ComputeLogsTable.c.partial == partial,
        )

    def _read_capped(self, path: str) -> tuple[bytes, int]:
        """The file's bytes, keeping only the tail past ``max_log_bytes``, and
        its original size."""
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size <= self._max_log_bytes:
                return f.read(), size
            f.seek(size - self._max_log_bytes)
            tail = f.read()
        marker = (
            f"[dagster-multihost: log truncated, showing last {len(tail)} "
            f"of {size} bytes]\n"
        ).encode()
        return marker + tail, size

    # -- CloudStorageComputeLogManager ------------------------------------------

    @property
    def local_manager(self) -> LocalComputeLogManager:
        return self._local_manager

    @property
    def upload_interval(self) -> Optional[int]:
        return self._upload_interval if self._upload_interval else None

    def upload_to_cloud_storage(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> None:
        path = self.local_manager.get_captured_local_path(
            log_key, IO_TYPE_EXTENSION[io_type]
        )
        ensure_file(path)
        if partial and os.stat(path).st_size == 0:
            return

        data, size = self._read_capped(path)
        with self.engine.begin() as conn:
            conn.execute(
                ComputeLogsTable.delete().where(
                    self._row_filter(log_key, io_type, partial)
                )
            )
            if not partial:
                # The finished log supersedes any in-progress copy.
                conn.execute(
                    ComputeLogsTable.delete().where(
                        self._row_filter(log_key, io_type, True)
                    )
                )
            conn.execute(
                ComputeLogsTable.insert().values(
                    log_key=self._key(log_key),
                    io_type=IO_TYPE_EXTENSION[io_type],
                    partial=partial,
                    data=data,
                    original_size=size,
                )
            )

    def download_from_cloud_storage(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> None:
        with self.engine.connect() as conn:
            data = conn.execute(
                db.select(ComputeLogsTable.c.data).where(
                    self._row_filter(log_key, io_type, partial)
                )
            ).scalar()
        if data is None:
            return

        path = self.local_manager.get_captured_local_path(
            log_key, IO_TYPE_EXTENSION[io_type], partial=partial
        )
        ensure_dir(os.path.dirname(path))
        with open(path, "wb") as f:
            f.write(data)

    def cloud_storage_has_logs(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                db.select(ComputeLogsTable.c.log_key).where(
                    self._row_filter(log_key, io_type, partial)
                )
            ).first()
        return row is not None

    def delete_logs(
        self,
        log_key: Optional[Sequence[str]] = None,
        prefix: Optional[Sequence[str]] = None,
    ) -> None:
        self.local_manager.delete_logs(log_key=log_key, prefix=prefix)

        if log_key:
            condition = ComputeLogsTable.c.log_key == self._key(log_key)
        elif prefix:
            # The trailing '/' keeps ['a'] from matching ['apple'].
            condition = ComputeLogsTable.c.log_key.startswith(
                self._key(prefix) + "/", autoescape=True
            )
        else:
            check.failed(
                "Must pass in either `log_key` or `prefix` argument to delete_logs"
            )

        with self.engine.begin() as conn:
            conn.execute(ComputeLogsTable.delete().where(condition))

    def download_url_for_type(self, log_key: Sequence[str], io_type: ComputeIOType):
        # Logs are served through the webserver's own log view; there is no
        # separate URL to download them from.
        return None

    def display_path_for_type(self, log_key: Sequence[str], io_type: ComputeIOType):
        if not self.is_capture_complete(log_key):
            return None
        return (
            f"postgres:{TABLE_NAME}/{self._key(log_key)}.{IO_TYPE_EXTENSION[io_type]}"
        )

    def get_log_keys_for_log_key_prefix(
        self, log_key_prefix: Sequence[str], io_type: ComputeIOType
    ) -> Sequence[Sequence[str]]:
        prefix = self._key(log_key_prefix) + "/"
        with self.engine.connect() as conn:
            rows = conn.execute(
                db.select(ComputeLogsTable.c.log_key).where(
                    ComputeLogsTable.c.log_key.startswith(prefix, autoescape=True),
                    ComputeLogsTable.c.io_type == IO_TYPE_EXTENSION[io_type],
                    ComputeLogsTable.c.partial == False,  # noqa: E712
                )
            ).all()

        # Only direct children of the prefix, as with a directory listing.
        names = (row.log_key[len(prefix) :] for row in rows)
        return [[*log_key_prefix, name] for name in names if "/" not in name]

    def on_subscribe(self, subscription):
        self._subscription_manager.add_subscription(subscription)

    def on_unsubscribe(self, subscription):
        self._subscription_manager.remove_subscription(subscription)

    def dispose(self):
        self._subscription_manager.dispose()
        self._local_manager.dispose()
        if self._engine is not None:
            self._engine.dispose()
