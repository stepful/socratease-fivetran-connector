from fivetran_connector_sdk import Connector, Logging as log, Operations as op
from datetime import datetime
import socratease_api as api


##############################
# Destination Schema
##############################
def schema(config):
    return [
        {"table": "units", "primary_key": ["unit_id"]},
        # Composite primary key to allow storing multiple versions of a question
        # this is to handle a question that changes AFTER a question-attempt on the previous
        # version of the question.
        # TODO: if updated_at is not available, I think i read that fivetran can do hashing?
        {"table": "unit_questions", "primary_key": ["question_id", "updated_at"]},
        {
            "table": "unit_attempts",
            # TODO: is there a real primary key we can use here?
            "primary_key": ["user_id", "unit_id", "attempt_num"],
        },
        {"table": "unit_question_attempts", "primary_key": ["user_response_id"]},
    ]


##############################
# Sync: Quiz Definitions (Units)
##############################
def sync_units(config, state):
    last_sync = state.get("units_last_ts") or "1970-01-01T00:00:00Z"
    offset = 0
    batch_size = config.get("limit", 100)
    all_units = []
    while True:
        data = api.get_units(
            config=config,
            limit=batch_size,
            offset=offset,
            updated_after=last_sync,
        )
        units = data.get("data", {}).get("quiz", [])
        if not units:
            break
        all_units.extend(units)
        if len(units) < batch_size:
            break
        offset += batch_size
    log.info(f"Fetched {len(all_units)} units.")
    # Determine new checkpoint based on max created_at timestamp
    new_last_ts = last_sync
    for unit in all_units:
        ts = unit.get("created_at")
        if ts and ts > new_last_ts:
            new_last_ts = ts
    if all_units:
        yield op.upsert(table="units", data=all_units)
    return new_last_ts


##############################
# Sync: Unit Questions (Question Definitions)
##############################
# Note: A unit's question definitions are not returned by /v1/units/; you must call
# GET /v1/units/:unit_id/ to fetch the complete details, including questions.
# Since questions can change over time (even after an attempt is made), we store each
# version by adding a version identifier based on the question's updated_at timestamp.
# The composite primary key (question_id, version) ensures both versions are stored.
def sync_unit_questions(config, state, unit):
    unit_id = unit.get("unit_id")

    # Get unit details including questions
    data = api.get_unit_details(config=config, unit_id=unit_id)

    # Assume the response includes a "questions" key in data
    questions = data.get("data", {}).get("questions", [])
    if questions:
        for question in questions:
            # Also include unit_id for reference
            question["unit_id"] = unit_id

        # TODO: we should probably clean up the insert data for SQL
        yield op.upsert(table="unit_questions", data=questions)


##############################
# Sync: Unit Attempts (Quiz Attempts)
##############################
def sync_unit_attempts(config, state, unit):
    unit_id = unit.get("unit_id")
    last_sync = state.get("attempts_last_ts", {}).get(unit_id) or "1970-01-01T00:00:00Z"
    batch_size = config.get("limit", 100)

    # Get all attempts for this unit with pagination handled by the API function
    attempts = api.get_unit_attempts(
        config=config, unit_id=unit_id, limit=batch_size, updated_after=last_sync
    )

    log.info(f"Fetched {len(attempts)} attempts for unit {unit_id}")

    # Track the latest timestamp for this unit's attempts
    new_last_ts = last_sync

    # Update the checkpoint timestamp based on the latest finished_at or updated_at
    for attempt in attempts:
        # Use finished_at or updated_at as the timestamp for incremental syncing
        ts = attempt.get("finished_at") or attempt.get("updated_at")
        if ts and ts > new_last_ts:
            new_last_ts = ts

    # Yield the attempts for database insertion
    if attempts:
        # TODO: we should probably clean up the insert data for SQL
        yield op.upsert(table="unit_attempts", data=attempts)

    # Return both the attempts and the new timestamp for checkpointing
    return attempts, new_last_ts


##############################
# Sync: Unit Question Attempts (Quiz Question Attempts)
##############################
def sync_unit_question_attempts(config, state, unit, attempt):
    soc_user_id = attempt.get("user_id")
    attempt_num = attempt.get("attempt_num")
    unit_id = unit.get("unit_id")

    if not (soc_user_id and attempt_num and unit_id):
        log.warning("Skipping quiz question attempts due to missing params.")
        return

    # Get question attempts
    data = api.get_question_attempts(
        config=config, user_id=soc_user_id, unit_id=unit_id, attempt_num=attempt_num
    )

    responses_obj = data.get("data", {}).get("responses", {})
    question_attempts = []
    for key, responses in responses_obj.items():
        if responses and isinstance(responses, list):
            # Use the last response from the list.
            question_attempt = responses[-1]
            question_attempts.append(question_attempt)
    if question_attempts:
        # TODO: we should probably clean up the insert data for SQL
        yield op.upsert(table="unit_question_attempts", data=question_attempts)


##############################
# Main Update Function
##############################
def update(config, state):
    new_state = {
        "units_last_ts": state.get("units_last_ts", "1970-01-01T00:00:00Z"),
        "attempts_last_ts": state.get("attempts_last_ts", {}),
    }

    # 1. Sync quiz definitions (units)
    new_units_last_ts = yield from sync_units(config, state)
    new_state["units_last_ts"] = new_units_last_ts

    # 2. For each unit, fetch full unit details to sync questions,
    #    then sync attempts and question attempts.
    # Get all units
    data = api.get_units(config=config)
    units = data.get("data", {}).get("quiz", [])

    for unit in units:
        unit_id = unit.get("unit_id")

        # Sync unit questions by calling GET /v1/units/:unit_id/
        yield from sync_unit_questions(config, state, unit)

        # Sync unit attempts (quiz attempts) with pagination
        attempts_result = sync_unit_attempts(config, state, unit)

        # The sync_unit_attempts function now returns a tuple of (attempts, new_timestamp)
        for result in attempts_result:
            if isinstance(result, list):  # This is the list of attempts
                for attempt in result:
                    # For each attempt, sync unit question attempts (quiz question attempts)
                    yield from sync_unit_question_attempts(config, state, unit, attempt)
            else:  # This is the new timestamp
                # Update the checkpoint for this unit's attempts
                new_state["attempts_last_ts"][unit_id] = result

    # Final checkpoint with new state
    yield op.checkpoint(state=new_state)


##############################
# Connector Entry Point
##############################
connector = Connector(update=update, schema=schema)
