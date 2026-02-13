from flask import Flask, request, jsonify, render_template
import psycopg2
import threading
import time
from datetime import datetime, timezone
import requests
import logging

# ==================================================
# CONFIG
# ==================================================
TRAIN_API_URL = "http://localhost:5000/train"
INCREMENT_DELAY = 0.5

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sack_count_db",
    "user": "postgres",
    "password": "postgres"
}

# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("wagon_loader")

app = Flask(__name__)

# ==================================================
# LOADING LOGIC (BY SIDING)
# ==================================================
def load_wagons(filter_field, filter_value, max_bags):
    logger.info(
        f"Loading started | {filter_field}={filter_value} | MAX_BAGS={max_bags}"
    )

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # --------------------------------------------------
    # FETCH ONLY PENDING WAGONS
    # --------------------------------------------------
    cur.execute(f"""
        SELECT id, wagon_number, tower_number
        FROM wagon_records
        WHERE {filter_field} = %s
        AND loading_status = false
        ORDER BY tower_number
    """, (filter_value,))
    wagons = cur.fetchall()

    if not wagons:
        logger.warning("No pending wagons found")
        cur.close()
        conn.close()
        return

    # --------------------------------------------------
    # AUTO ASSIGN WAGON NUMBERS (WGN-001 format)
    # --------------------------------------------------
    for idx, (row_id, wagon_number, tower_number) in enumerate(wagons, start=1):
        if not wagon_number:
            new_wgn = f"WGN-{idx:03d}"
            cur.execute("""
                UPDATE wagon_records
                SET wagon_number = %s
                WHERE id = %s
            """, (new_wgn, row_id))

    conn.commit()

    # --------------------------------------------------
    # RELOAD AFTER NUMBER ASSIGNMENT
    # --------------------------------------------------
    cur.execute(f"""
        SELECT wagon_number, tower_number
        FROM wagon_records
        WHERE {filter_field} = %s
        AND loading_status = false
        ORDER BY tower_number
    """, (filter_value,))
    wagons = cur.fetchall()

    # --------------------------------------------------
    # MAIN LOADING LOOP
    # --------------------------------------------------
    for wagon_number, tower_number in wagons:
        logger.info(f"Loading wagon {wagon_number}")

        while True:
            cur.execute(f"""
                UPDATE wagon_records
                SET loaded_bag_count = loaded_bag_count + 1
                WHERE {filter_field} = %s
                AND wagon_number = %s
                AND loading_status = false
                AND loaded_bag_count < %s
                RETURNING loaded_bag_count
            """, (filter_value, wagon_number, max_bags))

            row = cur.fetchone()
            conn.commit()

            if not row:
                break

            count = row[0]

            # First bag → start time
            if count == 1:
                start_time = datetime.now(timezone.utc).replace(microsecond=0)
                cur.execute(f"""
                    UPDATE wagon_records
                    SET loading_start_time = %s
                    WHERE {filter_field} = %s
                    AND wagon_number = %s
                """, (start_time, filter_value, wagon_number))
                conn.commit()

            # Last bag → complete
            if count == max_bags:
                end_time = datetime.now(timezone.utc).replace(microsecond=0)
                cur.execute(f"""
                    UPDATE wagon_records
                    SET loading_end_time = %s,
                        loading_status = true
                    WHERE {filter_field} = %s
                    AND wagon_number = %s
                """, (end_time, filter_value, wagon_number))
                conn.commit()

            logger.info(
                f"{wagon_number} → {count}/{max_bags}"
            )
            time.sleep(INCREMENT_DELAY)

    cur.close()
    conn.close()
    logger.info("All pending wagons finished")


# ==================================================
# ADD TRAIN → AUTO START BY SIDING
# ==================================================
@app.route("/add-train", methods=["POST"])
def add_train():
    data = request.get_json()

    if not data:
        return jsonify({"error": "JSON required"}), 400

    train_id = data.get("train_id")
    wagon_count = data.get("wagon_count")
    siding = data.get("siding")
    max_bags = data.get("max_bags")

    if not siding:
        return jsonify({"error": "siding is required"}), 400

    if not isinstance(max_bags, int) or max_bags <= 0:
        return jsonify({"error": "max_bags must be positive"}), 400

    try:
        # Register train
        res = requests.post(
            TRAIN_API_URL,
            json=data,
            headers={"Content-Type": "application/json"}
        )

        if res.status_code not in [200, 201]:
            return jsonify({
                "error": "Train registration failed",
                "details": res.text
            }), res.status_code

        # Auto start loading for this siding
        threading.Thread(
            target=load_wagons,
            args=("siding", siding, max_bags),
            daemon=True
        ).start()

        logger.info(f"Auto loading started for siding {siding}")

        return jsonify({
            "status": "train_added_and_loading_started",
            "siding": siding
        })

    except Exception as e:
        logger.error(e)
        return jsonify({"error": str(e)}), 500

# ==================================================
# RESET
# ==================================================
@app.route("/reset-system", methods=["POST"])
def reset_system():
    data = request.get_json() or {}

    if data.get("confirm") != "YES":
        return jsonify({"error": "Send confirm YES"}), 400

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        TRUNCATE TABLE
        wagon_records,
        dashboard_records,
        train_session,
        dispatch_records
        RESTART IDENTITY CASCADE;
    """)

    conn.commit()
    cur.close()
    conn.close()

    logger.warning("SYSTEM RESET")
    return jsonify({"status": "system_reset_successful"})

# ==================================================
# HOME
# ==================================================
@app.route("/")
def home():
    return render_template("index.html")

# ==================================================
# RUN
# ==================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
