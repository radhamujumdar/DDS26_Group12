"""Lua transition scripts for the Fluxi engine."""

from __future__ import annotations


START_OR_ATTACH_WORKFLOW = r"""
local control_key = KEYS[1]
local run_state_key = KEYS[2]
local history_key = KEYS[3]
local workflow_queue_key = KEYS[4]
local timers_key = KEYS[5]

local now_ms = tonumber(ARGV[1])
local workflow_id = ARGV[2]
local workflow_name = ARGV[3]
local workflow_task_queue = ARGV[4]
local start_policy = ARGV[5]
local input_payload = ARGV[6]
local run_id = ARGV[7]
local workflow_task_timeout_ms = tonumber(ARGV[8])

local function pack(value)
    return cmsgpack.pack(value)
end

if redis.call('EXISTS', control_key) == 1 then
    local status = redis.call('HGET', control_key, 'status')
    local current_run_id = redis.call('HGET', control_key, 'current_run_id')
    local run_no = redis.call('HGET', control_key, 'run_no')
    local result_payload = redis.call('HGET', control_key, 'result_payload')
    local error_payload = redis.call('HGET', control_key, 'error_payload')

    if status == 'running' then
        if start_policy == 'reject_duplicate' then
            return {'duplicate_rejected', current_run_id or '', status or '', run_no or '', result_payload or '', error_payload or ''}
        end
        return {'attached', current_run_id or '', status or '', run_no or '', result_payload or '', error_payload or ''}
    end

    if start_policy == 'attach_or_start' then
        return {'existing_terminal', current_run_id or '', status or '', run_no or '', result_payload or '', error_payload or ''}
    end

    if start_policy == 'reject_duplicate' then
        return {'duplicate_rejected', current_run_id or '', status or '', run_no or '', result_payload or '', error_payload or ''}
    end
end

local existing_run_no = tonumber(redis.call('HGET', control_key, 'run_no') or '0')
local new_run_no = existing_run_no + 1
local workflow_task_id = 'wft-1'
local workflow_task_attempt_no = 1
local workflow_timer_member = 'workflow-timeout:' .. run_id .. ':' .. workflow_task_id .. ':' .. workflow_task_attempt_no
local workflow_deadline_ms = now_ms + workflow_task_timeout_ms

local started_event = pack({
    event_id = 1,
    event_type = 'WorkflowStarted',
    timestamp_ms = now_ms,
    workflow_id = workflow_id,
    run_id = run_id,
    workflow_name = workflow_name,
    workflow_task_queue = workflow_task_queue,
    input_payload = input_payload,
})

local workflow_task = pack({
    kind = 'workflow_task',
    workflow_id = workflow_id,
    workflow_name = workflow_name,
    run_id = run_id,
    task_queue = workflow_task_queue,
    workflow_task_id = workflow_task_id,
    attempt_no = workflow_task_attempt_no,
})

redis.call(
    'HSET',
    control_key,
    'workflow_id', workflow_id,
    'workflow_name', workflow_name,
    'workflow_task_queue', workflow_task_queue,
    'status', 'running',
    'run_no', tostring(new_run_no),
    'current_run_id', run_id,
    'result_payload', '',
    'error_payload', '',
    'updated_at_ms', tostring(now_ms),
    'created_at_ms', tostring(now_ms)
)

redis.call(
    'HSET',
    run_state_key,
    'workflow_id', workflow_id,
    'workflow_name', workflow_name,
    'workflow_task_queue', workflow_task_queue,
    'run_id', run_id,
    'run_no', tostring(new_run_no),
    'status', 'running',
    'input_payload', input_payload,
    'next_history_event_id', '2',
    'next_workflow_task_sequence_no', '2',
    'next_activity_sequence_no', '1',
    'open_workflow_task_id', workflow_task_id,
    'open_workflow_task_attempt_no', tostring(workflow_task_attempt_no),
    'waiting_activity_execution_id', '',
    'sticky_task_queue', '',
    'sticky_owner_id', '',
    'sticky_expires_at_ms', '0',
    'updated_at_ms', tostring(now_ms),
    'created_at_ms', tostring(now_ms)
)

redis.call('RPUSH', history_key, started_event)
redis.call('XADD', workflow_queue_key, '*', 'payload', workflow_task)
redis.call('ZADD', timers_key, workflow_deadline_ms, workflow_timer_member)

return {'started', run_id, 'running', tostring(new_run_no), '', ''}
"""


APPLY_WORKFLOW_TASK_COMPLETION = r"""
local control_key = KEYS[1]
local run_state_key = KEYS[2]
local history_key = KEYS[3]
local activity_key_prefix = KEYS[4]
local timers_key = KEYS[5]
local workflow_result_channel_key = KEYS[6]

local now_ms = tonumber(ARGV[1])
local workflow_task_id = ARGV[2]
local attempt_no = tonumber(ARGV[3])
local timer_member = ARGV[4]
local sticky_task_queue = ARGV[5]
local sticky_owner_id = ARGV[6]
local sticky_expires_at_ms = tonumber(ARGV[7] or '0')
local command_count = tonumber(ARGV[8] or '0')

local function pack(value)
    return cmsgpack.pack(value)
end

local status = redis.call('HGET', run_state_key, 'status')
local open_workflow_task_id = redis.call('HGET', run_state_key, 'open_workflow_task_id')
local open_workflow_task_attempt_no = tonumber(redis.call('HGET', run_state_key, 'open_workflow_task_attempt_no') or '0')

if status ~= 'running' then
    return {'conflict', '', ''}
end

if open_workflow_task_id ~= workflow_task_id or open_workflow_task_attempt_no ~= attempt_no then
    return {'stale', '', ''}
end

local run_id = redis.call('HGET', run_state_key, 'run_id')
local workflow_id = redis.call('HGET', run_state_key, 'workflow_id')
local workflow_name = redis.call('HGET', run_state_key, 'workflow_name')
local next_event_id = tonumber(redis.call('HGET', run_state_key, 'next_history_event_id') or '1')
local activity_sequence = tonumber(redis.call('HGET', run_state_key, 'next_activity_sequence_no') or '1')
local sticky_timer_member = 'workflow-sticky-timeout:' .. run_id .. ':' .. workflow_task_id .. ':' .. attempt_no
local first_activity_execution_id = ''
local activity_execution_ids = {}

redis.call('ZREM', timers_key, timer_member)
redis.call('ZREM', timers_key, sticky_timer_member)

local function set_sticky_fields()
    if sticky_task_queue ~= nil and sticky_task_queue ~= '' then
        redis.call(
            'HSET',
            run_state_key,
            'sticky_task_queue', sticky_task_queue,
            'sticky_owner_id', sticky_owner_id,
            'sticky_expires_at_ms', tostring(sticky_expires_at_ms)
        )
    else
        redis.call(
            'HSET',
            run_state_key,
            'sticky_task_queue', '',
            'sticky_owner_id', '',
            'sticky_expires_at_ms', '0'
        )
    end
end

local offset = 9
for _ = 1, command_count do
    local kind = ARGV[offset]
    local activity_name = ARGV[offset + 1]
    local activity_task_queue = ARGV[offset + 2]
    local activity_queue_key = ARGV[offset + 3]
    local input_payload = ARGV[offset + 4]
    local schedule_to_close_timeout_ms = tonumber(ARGV[offset + 5] or '0')
    local max_attempts = tonumber(ARGV[offset + 6] or '1')
    local initial_interval_ms = tonumber(ARGV[offset + 7] or '0')
    local backoff_coefficient = tonumber(ARGV[offset + 8] or '0')
    local max_interval_ms = tonumber(ARGV[offset + 9] or '0')
    local result_payload = ARGV[offset + 10]
    local error_payload = ARGV[offset + 11]
    offset = offset + 12

    if kind == 'schedule_activity' then
        local activity_execution_id = run_id .. ':act:' .. tostring(activity_sequence)
        local activity_key = activity_key_prefix .. activity_execution_id
        local activity_event = pack({
            event_id = next_event_id,
            event_type = 'ActivityScheduled',
            timestamp_ms = now_ms,
            run_id = run_id,
            workflow_id = workflow_id,
            activity_execution_id = activity_execution_id,
            activity_name = activity_name,
            task_queue = activity_task_queue,
            input_payload = input_payload,
            schedule_to_close_timeout_ms = schedule_to_close_timeout_ms,
            max_attempts = max_attempts,
            initial_interval_ms = initial_interval_ms,
            backoff_coefficient = backoff_coefficient,
            max_interval_ms = max_interval_ms,
        })
        local activity_task = pack({
            kind = 'activity_task',
            run_id = run_id,
            workflow_id = workflow_id,
            activity_execution_id = activity_execution_id,
            activity_name = activity_name,
            task_queue = activity_task_queue,
            attempt_no = 1,
            input_payload = input_payload,
            schedule_to_close_timeout_ms = schedule_to_close_timeout_ms,
        })

        if first_activity_execution_id == '' then
            first_activity_execution_id = activity_execution_id
        end
        table.insert(activity_execution_ids, activity_execution_id)

        redis.call(
            'HSET',
            activity_key,
            'activity_execution_id', activity_execution_id,
            'run_id', run_id,
            'workflow_id', workflow_id,
            'workflow_name', workflow_name,
            'activity_name', activity_name,
            'task_queue', activity_task_queue,
            'input_payload', input_payload,
            'status', 'scheduled',
            'current_attempt_no', '1',
            'max_attempts', tostring(max_attempts),
            'initial_interval_ms', tostring(initial_interval_ms),
            'backoff_coefficient', tostring(backoff_coefficient),
            'max_interval_ms', tostring(max_interval_ms),
            'schedule_to_close_timeout_ms', tostring(schedule_to_close_timeout_ms),
            'updated_at_ms', tostring(now_ms),
            'created_at_ms', tostring(now_ms)
        )

        if schedule_to_close_timeout_ms > 0 then
            redis.call('HSET', activity_key, 'deadline_at_ms', tostring(now_ms + schedule_to_close_timeout_ms))
            redis.call(
                'ZADD',
                timers_key,
                now_ms + schedule_to_close_timeout_ms,
                'activity-timeout:' .. activity_execution_id .. ':1'
            )
        end

        redis.call('RPUSH', history_key, activity_event)
        redis.call('XADD', activity_queue_key, '*', 'payload', activity_task)

        next_event_id = next_event_id + 1
        activity_sequence = activity_sequence + 1
    elseif kind == 'complete_workflow' then
        local completed_event = pack({
            event_id = next_event_id,
            event_type = 'WorkflowCompleted',
            timestamp_ms = now_ms,
            run_id = run_id,
            workflow_id = workflow_id,
            result_payload = result_payload,
        })

        redis.call('RPUSH', history_key, completed_event)
        redis.call(
            'HSET',
            run_state_key,
            'status', 'completed',
            'result_payload', result_payload,
            'next_history_event_id', tostring(next_event_id + 1),
            'next_activity_sequence_no', tostring(activity_sequence),
            'open_workflow_task_id', '',
            'open_workflow_task_attempt_no', '0',
            'waiting_activity_execution_id', '',
            'sticky_task_queue', '',
            'sticky_owner_id', '',
            'sticky_expires_at_ms', '0',
            'updated_at_ms', tostring(now_ms)
        )
        redis.call(
            'HSET',
            control_key,
            'status', 'completed',
            'current_run_id', run_id,
            'result_payload', result_payload,
            'error_payload', '',
            'updated_at_ms', tostring(now_ms)
        )
        redis.call('PUBLISH', workflow_result_channel_key, 'completed')
        return {'completed', run_id, ''}
    elseif kind == 'fail_workflow' then
        local failed_event = pack({
            event_id = next_event_id,
            event_type = 'WorkflowFailed',
            timestamp_ms = now_ms,
            run_id = run_id,
            workflow_id = workflow_id,
            error_payload = error_payload,
        })

        redis.call('RPUSH', history_key, failed_event)
        redis.call(
            'HSET',
            run_state_key,
            'status', 'failed',
            'error_payload', error_payload,
            'next_history_event_id', tostring(next_event_id + 1),
            'next_activity_sequence_no', tostring(activity_sequence),
            'open_workflow_task_id', '',
            'open_workflow_task_attempt_no', '0',
            'waiting_activity_execution_id', '',
            'sticky_task_queue', '',
            'sticky_owner_id', '',
            'sticky_expires_at_ms', '0',
            'updated_at_ms', tostring(now_ms)
        )
        redis.call(
            'HSET',
            control_key,
            'status', 'failed',
            'current_run_id', run_id,
            'error_payload', error_payload,
            'result_payload', '',
            'updated_at_ms', tostring(now_ms)
        )
        redis.call('PUBLISH', workflow_result_channel_key, 'failed')
        return {'failed', run_id, ''}
    end
end

redis.call(
    'HSET',
    run_state_key,
    'next_history_event_id', tostring(next_event_id),
    'next_activity_sequence_no', tostring(activity_sequence),
    'open_workflow_task_id', '',
    'open_workflow_task_attempt_no', '0',
    'waiting_activity_execution_id', '',
    'updated_at_ms', tostring(now_ms)
)
set_sticky_fields()

if first_activity_execution_id ~= '' then
    return {'scheduled_activity', run_id, first_activity_execution_id, pack(activity_execution_ids)}
end

return {'waiting', run_id, '', pack(activity_execution_ids)}
"""
APPLY_ACTIVITY_COMPLETION = r"""
local control_key = KEYS[1]
local run_state_key = KEYS[2]
local history_key = KEYS[3]
local activity_key = KEYS[4]
local timers_key = KEYS[5]

local now_ms = tonumber(ARGV[1])
local completion_status = ARGV[2]
local attempt_no = tonumber(ARGV[3])
local payload = ARGV[4]
local workflow_task_timeout_ms = tonumber(ARGV[5])
local activity_timeout_timer_member = ARGV[6]
local workflow_queue_prefix = ARGV[7]
local sticky_schedule_to_start_timeout_ms = tonumber(ARGV[8])

local function pack(value)
    return cmsgpack.pack(value)
end

local function clear_sticky()
    redis.call(
        'HSET',
        run_state_key,
        'sticky_task_queue', '',
        'sticky_owner_id', '',
        'sticky_expires_at_ms', '0'
    )
end

local function choose_next_workflow_queue()
    local sticky_queue = redis.call('HGET', run_state_key, 'sticky_task_queue')
    local sticky_expires = tonumber(redis.call('HGET', run_state_key, 'sticky_expires_at_ms') or '0')
    if sticky_queue and sticky_queue ~= '' and sticky_expires > now_ms then
        return sticky_queue, true
    end
    return redis.call('HGET', run_state_key, 'workflow_task_queue'), false
end

local function maybe_schedule_next_workflow_task(run_id, workflow_id)
    local open_id = redis.call('HGET', run_state_key, 'open_workflow_task_id')
    if open_id and open_id ~= '' then
        return ''
    end

    local workflow_name = redis.call('HGET', run_state_key, 'workflow_name')
    local next_sequence = tonumber(redis.call('HGET', run_state_key, 'next_workflow_task_sequence_no') or '1')
    local next_workflow_task_id = 'wft-' .. tostring(next_sequence)
    local target_queue, use_sticky = choose_next_workflow_queue()
    local workflow_task = pack({
        kind = 'workflow_task',
        workflow_id = workflow_id,
        workflow_name = workflow_name,
        run_id = run_id,
        task_queue = target_queue,
        workflow_task_id = next_workflow_task_id,
        attempt_no = 1,
    })

    redis.call('XADD', workflow_queue_prefix .. target_queue, '*', 'payload', workflow_task)
    redis.call(
        'ZADD',
        timers_key,
        now_ms + workflow_task_timeout_ms,
        'workflow-timeout:' .. run_id .. ':' .. next_workflow_task_id .. ':1'
    )
    if use_sticky then
        redis.call(
            'ZADD',
            timers_key,
            now_ms + sticky_schedule_to_start_timeout_ms,
            'workflow-sticky-timeout:' .. run_id .. ':' .. next_workflow_task_id .. ':1'
        )
    else
        clear_sticky()
    end
    redis.call(
        'HSET',
        run_state_key,
        'next_workflow_task_sequence_no', tostring(next_sequence + 1),
        'open_workflow_task_id', next_workflow_task_id,
        'open_workflow_task_attempt_no', '1'
    )
    return next_workflow_task_id
end

if redis.call('EXISTS', activity_key) == 0 then
    return {'missing', '', ''}
end

local current_attempt_no = tonumber(redis.call('HGET', activity_key, 'current_attempt_no') or '0')
local activity_status = redis.call('HGET', activity_key, 'status')

if current_attempt_no ~= attempt_no then
    return {'stale', '', ''}
end

if activity_status ~= 'scheduled' and activity_status ~= 'running' then
    return {'stale', '', ''}
end

redis.call('ZREM', timers_key, activity_timeout_timer_member)

local activity_execution_id = redis.call('HGET', activity_key, 'activity_execution_id')
local run_id = redis.call('HGET', activity_key, 'run_id')
local workflow_id = redis.call('HGET', activity_key, 'workflow_id')
local activity_name = redis.call('HGET', activity_key, 'activity_name')
local next_event_id = tonumber(redis.call('HGET', run_state_key, 'next_history_event_id') or '1')

if completion_status == 'completed' then
    local completed_event = pack({
        event_id = next_event_id,
        event_type = 'ActivityCompleted',
        timestamp_ms = now_ms,
        run_id = run_id,
        workflow_id = workflow_id,
        activity_execution_id = activity_execution_id,
        activity_name = activity_name,
        result_payload = payload,
    })
    redis.call('RPUSH', history_key, completed_event)
    redis.call(
        'HSET',
        activity_key,
        'status', 'completed',
        'accepted_status', 'completed',
        'accepted_result_payload', payload,
        'updated_at_ms', tostring(now_ms)
    )
    redis.call(
        'HSET',
        run_state_key,
        'next_history_event_id', tostring(next_event_id + 1),
        'updated_at_ms', tostring(now_ms)
    )
    local next_workflow_task_id = maybe_schedule_next_workflow_task(run_id, workflow_id)
    return {'accepted', run_id, next_workflow_task_id}
end

local max_attempts = tonumber(redis.call('HGET', activity_key, 'max_attempts') or '0')
local initial_interval_ms = tonumber(redis.call('HGET', activity_key, 'initial_interval_ms') or '0')
local backoff_coefficient = tonumber(redis.call('HGET', activity_key, 'backoff_coefficient') or '0')
local max_interval_ms = tonumber(redis.call('HGET', activity_key, 'max_interval_ms') or '0')

if max_attempts == 0 then
    max_attempts = 1
end

if attempt_no < max_attempts then
    local next_attempt_no = attempt_no + 1
    local delay_ms = initial_interval_ms
    if delay_ms <= 0 then
        delay_ms = 1
    end
    if next_attempt_no > 2 and backoff_coefficient > 1 then
        delay_ms = math.floor(delay_ms * (backoff_coefficient ^ (attempt_no - 1)))
    end
    if max_interval_ms > 0 and delay_ms > max_interval_ms then
        delay_ms = max_interval_ms
    end

    local failed_event = pack({
        event_id = next_event_id,
        event_type = 'ActivityAttemptFailed',
        timestamp_ms = now_ms,
        run_id = run_id,
        workflow_id = workflow_id,
        activity_execution_id = activity_execution_id,
        activity_name = activity_name,
        attempt_no = attempt_no,
        error_payload = payload,
    })
    local retry_event = pack({
        event_id = next_event_id + 1,
        event_type = 'ActivityRetryScheduled',
        timestamp_ms = now_ms,
        run_id = run_id,
        workflow_id = workflow_id,
        activity_execution_id = activity_execution_id,
        activity_name = activity_name,
        attempt_no = next_attempt_no,
        delay_ms = delay_ms,
    })

    redis.call('RPUSH', history_key, failed_event)
    redis.call('RPUSH', history_key, retry_event)
    redis.call(
        'HSET',
        activity_key,
        'status', 'retry_pending',
        'current_attempt_no', tostring(next_attempt_no),
        'last_error_payload', payload,
        'updated_at_ms', tostring(now_ms)
    )
    redis.call(
        'HSET',
        run_state_key,
        'next_history_event_id', tostring(next_event_id + 2),
        'updated_at_ms', tostring(now_ms)
    )
    redis.call(
        'ZADD',
        timers_key,
        now_ms + delay_ms,
        'activity-retry:' .. activity_execution_id .. ':' .. next_attempt_no
    )
    return {'retry_scheduled', run_id, ''}
end

local failed_event = pack({
    event_id = next_event_id,
    event_type = 'ActivityFailed',
    timestamp_ms = now_ms,
    run_id = run_id,
    workflow_id = workflow_id,
    activity_execution_id = activity_execution_id,
    activity_name = activity_name,
    error_payload = payload,
})
redis.call('RPUSH', history_key, failed_event)
redis.call(
    'HSET',
    activity_key,
    'status', 'failed',
    'accepted_status', 'failed',
    'accepted_error_payload', payload,
    'updated_at_ms', tostring(now_ms)
)
redis.call(
    'HSET',
    run_state_key,
    'next_history_event_id', tostring(next_event_id + 1),
    'updated_at_ms', tostring(now_ms)
)
local next_workflow_task_id = maybe_schedule_next_workflow_task(run_id, workflow_id)
return {'accepted', run_id, next_workflow_task_id}
"""
APPLY_TIMER = r"""
local run_state_key = KEYS[1]
local history_key = KEYS[2]
local activity_key = KEYS[3]
local timers_key = KEYS[4]

local now_ms = tonumber(ARGV[1])
local timer_kind = ARGV[2]
local logical_id = ARGV[3]
local attempt_no = tonumber(ARGV[4])
local workflow_task_timeout_ms = tonumber(ARGV[5])
local timeout_error_payload = ARGV[6]
local workflow_queue_prefix = ARGV[7]
local activity_queue_prefix = ARGV[8]
local sticky_schedule_to_start_timeout_ms = tonumber(ARGV[9])

local function pack(value)
    return cmsgpack.pack(value)
end

local function clear_sticky()
    redis.call(
        'HSET',
        run_state_key,
        'sticky_task_queue', '',
        'sticky_owner_id', '',
        'sticky_expires_at_ms', '0'
    )
end

local function choose_next_workflow_queue()
    local sticky_queue = redis.call('HGET', run_state_key, 'sticky_task_queue')
    local sticky_expires = tonumber(redis.call('HGET', run_state_key, 'sticky_expires_at_ms') or '0')
    if sticky_queue and sticky_queue ~= '' and sticky_expires > now_ms then
        return sticky_queue, true
    end
    return redis.call('HGET', run_state_key, 'workflow_task_queue'), false
end

local function maybe_schedule_next_workflow_task(run_id, workflow_id)
    local open_id = redis.call('HGET', run_state_key, 'open_workflow_task_id')
    if open_id and open_id ~= '' then
        return ''
    end

    local workflow_name = redis.call('HGET', run_state_key, 'workflow_name')
    local next_sequence = tonumber(redis.call('HGET', run_state_key, 'next_workflow_task_sequence_no') or '1')
    local next_workflow_task_id = 'wft-' .. tostring(next_sequence)
    local target_queue, use_sticky = choose_next_workflow_queue()
    local workflow_task = pack({
        kind = 'workflow_task',
        workflow_id = workflow_id,
        workflow_name = workflow_name,
        run_id = run_id,
        task_queue = target_queue,
        workflow_task_id = next_workflow_task_id,
        attempt_no = 1,
    })

    redis.call('XADD', workflow_queue_prefix .. target_queue, '*', 'payload', workflow_task)
    redis.call(
        'ZADD',
        timers_key,
        now_ms + workflow_task_timeout_ms,
        'workflow-timeout:' .. run_id .. ':' .. next_workflow_task_id .. ':1'
    )
    if use_sticky then
        redis.call(
            'ZADD',
            timers_key,
            now_ms + sticky_schedule_to_start_timeout_ms,
            'workflow-sticky-timeout:' .. run_id .. ':' .. next_workflow_task_id .. ':1'
        )
    else
        clear_sticky()
    end
    redis.call(
        'HSET',
        run_state_key,
        'next_workflow_task_sequence_no', tostring(next_sequence + 1),
        'open_workflow_task_id', next_workflow_task_id,
        'open_workflow_task_attempt_no', '1'
    )
    return next_workflow_task_id
end

if timer_kind == 'workflow-timeout' or timer_kind == 'workflow-sticky-timeout' then
    if redis.call('EXISTS', run_state_key) == 0 then
        return {'missing', '', ''}
    end

    local current_id = redis.call('HGET', run_state_key, 'open_workflow_task_id')
    local current_attempt_no = tonumber(redis.call('HGET', run_state_key, 'open_workflow_task_attempt_no') or '0')
    local run_status = redis.call('HGET', run_state_key, 'status')
    if run_status ~= 'running' then
        return {'stale', '', ''}
    end
    if current_id ~= logical_id or current_attempt_no ~= attempt_no then
        return {'stale', '', ''}
    end

    local run_id = redis.call('HGET', run_state_key, 'run_id')
    local workflow_id = redis.call('HGET', run_state_key, 'workflow_id')
    local workflow_name = redis.call('HGET', run_state_key, 'workflow_name')
    local normal_workflow_queue = redis.call('HGET', run_state_key, 'workflow_task_queue')
    local next_event_id = tonumber(redis.call('HGET', run_state_key, 'next_history_event_id') or '1')
    local retried_attempt_no = attempt_no + 1
    local workflow_task = pack({
        kind = 'workflow_task',
        workflow_id = workflow_id,
        workflow_name = workflow_name,
        run_id = run_id,
        task_queue = normal_workflow_queue,
        workflow_task_id = logical_id,
        attempt_no = retried_attempt_no,
    })

    if timer_kind == 'workflow-timeout' then
        local timeout_event = pack({
            event_id = next_event_id,
            event_type = 'WorkflowTaskTimedOut',
            timestamp_ms = now_ms,
            run_id = run_id,
            workflow_id = workflow_id,
            workflow_task_id = logical_id,
            attempt_no = attempt_no,
        })
        redis.call('RPUSH', history_key, timeout_event)
        redis.call(
            'HSET',
            run_state_key,
            'next_history_event_id', tostring(next_event_id + 1),
            'updated_at_ms', tostring(now_ms)
        )
    else
        redis.call('HSET', run_state_key, 'updated_at_ms', tostring(now_ms))
    end

    clear_sticky()
    redis.call(
        'HSET',
        run_state_key,
        'open_workflow_task_attempt_no', tostring(retried_attempt_no)
    )
    redis.call('XADD', workflow_queue_prefix .. normal_workflow_queue, '*', 'payload', workflow_task)
    redis.call(
        'ZADD',
        timers_key,
        now_ms + workflow_task_timeout_ms,
        'workflow-timeout:' .. run_id .. ':' .. logical_id .. ':' .. retried_attempt_no
    )
    return {'retried', run_id, logical_id}
end

if redis.call('EXISTS', activity_key) == 0 then
    return {'missing', '', ''}
end

local current_attempt_no = tonumber(redis.call('HGET', activity_key, 'current_attempt_no') or '0')
local activity_status = redis.call('HGET', activity_key, 'status')
if current_attempt_no ~= attempt_no then
    return {'stale', '', ''}
end

local activity_execution_id = redis.call('HGET', activity_key, 'activity_execution_id')
local run_id = redis.call('HGET', activity_key, 'run_id')
local workflow_id = redis.call('HGET', activity_key, 'workflow_id')
local activity_name = redis.call('HGET', activity_key, 'activity_name')

if timer_kind == 'activity-retry' then
    if activity_status ~= 'retry_pending' then
        return {'stale', run_id, ''}
    end

    local activity_task_queue = redis.call('HGET', activity_key, 'task_queue')
    local input_payload = redis.call('HGET', activity_key, 'input_payload')
    local schedule_to_close_timeout_ms = tonumber(redis.call('HGET', activity_key, 'schedule_to_close_timeout_ms') or '0')
    local activity_task = pack({
        kind = 'activity_task',
        run_id = run_id,
        workflow_id = workflow_id,
        activity_execution_id = activity_execution_id,
        activity_name = activity_name,
        task_queue = activity_task_queue,
        attempt_no = attempt_no,
        input_payload = input_payload,
        schedule_to_close_timeout_ms = schedule_to_close_timeout_ms,
    })

    redis.call(
        'HSET',
        activity_key,
        'status', 'scheduled',
        'updated_at_ms', tostring(now_ms)
    )
    if schedule_to_close_timeout_ms > 0 then
        redis.call(
            'HSET',
            activity_key,
            'deadline_at_ms', tostring(now_ms + schedule_to_close_timeout_ms)
        )
        redis.call(
            'ZADD',
            timers_key,
            now_ms + schedule_to_close_timeout_ms,
            'activity-timeout:' .. activity_execution_id .. ':' .. attempt_no
        )
    end
    redis.call('XADD', activity_queue_prefix .. activity_task_queue, '*', 'payload', activity_task)
    return {'retried', run_id, activity_execution_id}
end

if activity_status ~= 'scheduled' and activity_status ~= 'running' then
    return {'stale', run_id, ''}
end

local max_attempts = tonumber(redis.call('HGET', activity_key, 'max_attempts') or '0')
local initial_interval_ms = tonumber(redis.call('HGET', activity_key, 'initial_interval_ms') or '0')
local backoff_coefficient = tonumber(redis.call('HGET', activity_key, 'backoff_coefficient') or '0')
local max_interval_ms = tonumber(redis.call('HGET', activity_key, 'max_interval_ms') or '0')
local next_event_id = tonumber(redis.call('HGET', run_state_key, 'next_history_event_id') or '1')

if max_attempts == 0 then
    max_attempts = 1
end

if attempt_no < max_attempts then
    local next_attempt_no = attempt_no + 1
    local delay_ms = initial_interval_ms
    if delay_ms <= 0 then
        delay_ms = 1
    end
    if next_attempt_no > 2 and backoff_coefficient > 1 then
        delay_ms = math.floor(delay_ms * (backoff_coefficient ^ (attempt_no - 1)))
    end
    if max_interval_ms > 0 and delay_ms > max_interval_ms then
        delay_ms = max_interval_ms
    end

    local timed_out_event = pack({
        event_id = next_event_id,
        event_type = 'ActivityAttemptTimedOut',
        timestamp_ms = now_ms,
        run_id = run_id,
        workflow_id = workflow_id,
        activity_execution_id = activity_execution_id,
        activity_name = activity_name,
        attempt_no = attempt_no,
    })
    local retry_event = pack({
        event_id = next_event_id + 1,
        event_type = 'ActivityRetryScheduled',
        timestamp_ms = now_ms,
        run_id = run_id,
        workflow_id = workflow_id,
        activity_execution_id = activity_execution_id,
        activity_name = activity_name,
        attempt_no = next_attempt_no,
        delay_ms = delay_ms,
    })

    redis.call('RPUSH', history_key, timed_out_event)
    redis.call('RPUSH', history_key, retry_event)
    redis.call(
        'HSET',
        activity_key,
        'status', 'retry_pending',
        'current_attempt_no', tostring(next_attempt_no),
        'last_error_payload', timeout_error_payload,
        'updated_at_ms', tostring(now_ms)
    )
    redis.call(
        'HSET',
        run_state_key,
        'next_history_event_id', tostring(next_event_id + 2),
        'updated_at_ms', tostring(now_ms)
    )
    redis.call(
        'ZADD',
        timers_key,
        now_ms + delay_ms,
        'activity-retry:' .. activity_execution_id .. ':' .. next_attempt_no
    )
    return {'retry_scheduled', run_id, activity_execution_id}
end

local failed_event = pack({
    event_id = next_event_id,
    event_type = 'ActivityFailed',
    timestamp_ms = now_ms,
    run_id = run_id,
    workflow_id = workflow_id,
    activity_execution_id = activity_execution_id,
    activity_name = activity_name,
    error_payload = timeout_error_payload,
})
redis.call('RPUSH', history_key, failed_event)
redis.call(
    'HSET',
    activity_key,
    'status', 'failed',
    'accepted_status', 'failed',
    'accepted_error_payload', timeout_error_payload,
    'updated_at_ms', tostring(now_ms)
)
redis.call(
    'HSET',
    run_state_key,
    'next_history_event_id', tostring(next_event_id + 1),
    'updated_at_ms', tostring(now_ms)
)
local next_workflow_task_id = maybe_schedule_next_workflow_task(run_id, workflow_id)
return {'accepted', run_id, next_workflow_task_id}
"""
