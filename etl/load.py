import mysql.connector
from config.db_config import DB_CONFIG


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def upsert_trials(records):

    if not records:
        print("No records to upsert.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO clinical_trials
        (nct_id, cancer_type, phase, status, start_date, enrollment, sponsor_type, state, last_update_posted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            cancer_type = VALUES(cancer_type),
            phase = VALUES(phase),
            status = VALUES(status),
            start_date = VALUES(start_date),
            enrollment = VALUES(enrollment),
            sponsor_type = VALUES(sponsor_type),
            state = VALUES(state),
            last_update_posted = VALUES(last_update_posted)
    """

    values = [
        (
            record["nct_id"],
            record["cancer_type"],
            record["phase"],
            record["status"],
            record["start_date"],
            record["enrollment"],
            record["sponsor_type"],
            record["state"],
            record["last_update_posted"]
        )
        for record in records
    ]

    cursor.executemany(insert_query, values)
    conn.commit()

    cursor.close()
    conn.close()

    print(f"💾 Upserted {len(records)} records.")