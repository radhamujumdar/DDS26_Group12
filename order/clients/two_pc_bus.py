# 2pc message queue change: reuse the Saga Redis Streams request/reply bus shape
# with a separate keyspace so 2PC gets the same transport behavior without
# changing the existing Saga topology.
from clients.saga_bus import SagaCommandBus
from logging_utils import log_event


class TwoPCCommandBus(SagaCommandBus):
    COMMAND_STREAM_PREFIX = "two_pc:cmd:"
    RESULT_STREAM_PREFIX = "two_pc:res:"
    PENDING_PREFIX = "two_pc:mq:pending:"
    CURSOR_PREFIX = "two_pc:mq:cursor:"
    LEASE_PREFIX = "two_pc:mq:lease:p"
    METRIC_PREFIX = "two_pc:mq:metric:"

    # 2pc message queue change: parse two_pc result streams explicitly so the
    # dispatcher accepts replies from two_pc workers instead of treating them
    # as unknown Saga streams.
    def _parse_result_stream(self, stream_name: str) -> tuple[str | None, int]:
        # Expected shape: two_pc:res:<participant>:p<partition>
        parts = stream_name.split(":")
        if len(parts) != 4 or parts[0] != "two_pc" or parts[1] != "res":
            self._log("two_pc_mq_unknown_result_stream", level="warning", stream=stream_name)
            return None, -1
        participant = parts[2]
        partition_part = parts[3]
        if not partition_part.startswith("p"):
            self._log("two_pc_mq_invalid_result_partition", level="warning", stream=stream_name)
            return None, -1
        try:
            return participant, int(partition_part[1:])
        except ValueError:
            self._log("two_pc_mq_invalid_result_partition", level="warning", stream=stream_name)
            return None, -1

    def _log(self, event: str, level: str = "info", **fields):
        if event.startswith("saga_mq_"):
            event = "two_pc_mq_" + event[len("saga_mq_") :]
        log_event(
            self.logger,
            event=event,
            level=level,
            service="order-service",
            component="two-pc-mq",
            **fields,
        )
