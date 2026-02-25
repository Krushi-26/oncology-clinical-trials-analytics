import time
from datetime import datetime
import mysql.connector

from ingestion.api_client import ClinicalTrialsAPI
from etl.transform import ClinicalTrialTransformer
from etl.load import upsert_trials
from config.db_config import DB_CONFIG


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def get_watermark():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(last_update_posted) FROM clinical_trials")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result


def log_etl_run(run_data):
    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO etl_run_logs
        (run_timestamp, records_fetched, records_transformed,
         records_loaded, status, error_message, duration_seconds)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    cursor.execute(insert_query, (
        run_data["run_timestamp"],
        run_data["records_fetched"],
        run_data["records_transformed"],
        run_data["records_loaded"],
        run_data["status"],
        run_data["error_message"],
        run_data["duration_seconds"]
    ))

    conn.commit()
    cursor.close()
    conn.close()


def run_pipeline():

    start_time = time.time()
    run_timestamp = datetime.now()

    run_data = {
        "run_timestamp": run_timestamp,
        "records_fetched": 0,
        "records_transformed": 0,
        "records_loaded": 0,
        "status": "FAILED",
        "error_message": None,
        "duration_seconds": 0
    }

    print("🚀 Starting ETL pipeline...")

    try:
        watermark = get_watermark()
        print(f"📌 Current watermark: {watermark}")

        api = ClinicalTrialsAPI()
        raw_response = api.fetch_oncology_trials(page_size=100)

        if not raw_response:
            raise Exception("No response from API")

        raw_data = raw_response.get("studies", [])
        run_data["records_fetched"] = len(raw_data)
        print(f"📥 Records fetched: {len(raw_data)}")

        transformed_data = ClinicalTrialTransformer.transform(raw_response)
        run_data["records_transformed"] = len(transformed_data)
        print(f"🔄 Records transformed: {len(transformed_data)}")

        # Incremental logic
        if watermark:
            incremental_data = [
                record for record in transformed_data
                if record.get("last_update_posted")
                and record["last_update_posted"] > watermark
            ]
            print(f"🆕 Incremental records: {len(incremental_data)}")
        else:
            incremental_data = transformed_data
            print("🆕 First run — loading all records")

        run_data["records_loaded"] = len(incremental_data)

        if incremental_data:
            upsert_trials(incremental_data)

        run_data["status"] = "SUCCESS"

    except Exception as e:
        run_data["error_message"] = str(e)
        print("❌ ETL failed:", str(e))

    finally:
        end_time = time.time()
        run_data["duration_seconds"] = round(end_time - start_time, 2)

        log_etl_run(run_data)
        print("📝 ETL run logged.")
        print("✅ ETL pipeline completed.")


if __name__ == "__main__":
    run_pipeline()