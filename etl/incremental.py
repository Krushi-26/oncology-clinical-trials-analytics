from datetime import datetime
import mysql.connector
from config.db_config import DB_CONFIG

PIPELINE_NAME = "clinical_trials_etl"


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def get_last_successful_watermark():
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT MAX(max_last_updated)
        FROM etl_runs
        WHERE pipeline_name = %s
        AND status = 'SUCCESS'
    """

    cursor.execute(query, (PIPELINE_NAME,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return result[0] if result and result[0] else None


def start_run():
    conn = get_connection()
    cursor = conn.cursor()

    start_time = datetime.now()

    insert_query = """
        INSERT INTO etl_runs
        (pipeline_name, start_time, status)
        VALUES (%s, %s, %s)
    """

    cursor.execute(insert_query, (PIPELINE_NAME, start_time, "STARTED"))
    conn.commit()

    run_id = cursor.lastrowid

    cursor.close()
    conn.close()

    return run_id


def mark_success(run_id, records_processed, max_last_updated):
    conn = get_connection()
    cursor = conn.cursor()

    end_time = datetime.now()

    update_query = """
        UPDATE etl_runs
        SET status = %s,
            end_time = %s,
            records_processed = %s,
            max_last_updated = %s
        WHERE run_id = %s
    """

    cursor.execute(update_query, (
        "SUCCESS",
        end_time,
        records_processed,
        max_last_updated,
        run_id
    ))

    conn.commit()
    cursor.close()
    conn.close()


def mark_failure(run_id, error_message):
    conn = get_connection()
    cursor = conn.cursor()

    end_time = datetime.now()

    update_query = """
        UPDATE etl_runs
        SET status = %s,
            end_time = %s,
            error_message = %s
        WHERE run_id = %s
    """

    cursor.execute(update_query, (
        "FAILED",
        end_time,
        error_message,
        run_id
    ))

    conn.commit()
    cursor.close()
    conn.close()