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
    "host": "192.168.0.222",
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
    
    # Fetch only pending wagons
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
    
    # Auto assign wagon numbers (WGN-001 format) if missing
    for idx, (row_id, wagon_number, tower_number) in enumerate(wagons, start=1):
        if not wagon_number:
            new_wgn = f"WGN-{idx:03d}"
            cur.execute("""
                UPDATE wagon_records
                SET wagon_number = %s
                WHERE id = %s
            """, (new_wgn, row_id))
    
    conn.commit()
    
    # Reload after number assignment
    cur.execute(f"""
        SELECT wagon_number, tower_number
        FROM wagon_records
        WHERE {filter_field} = %s
        AND loading_status = false
        ORDER BY tower_number
    """, (filter_value,))
    wagons = cur.fetchall()
    
    # Main loading loop
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
                    SET loading_end_time = %s
                    WHERE {filter_field} = %s
                    AND wagon_number = %s
                """, (end_time, filter_value, wagon_number))
                conn.commit()
            
            logger.info(f"{wagon_number} → {count}/{max_bags}")
            time.sleep(INCREMENT_DELAY)
    
    cur.close()
    conn.close()
    logger.info("All pending wagons finished")

# ==================================================
# CAMERA DASHBOARD API ROUTES
# ==================================================
@app.route("/cameras")
def get_cameras():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, camera_name, siding, status, blur, shaking
        FROM camera_records
        ORDER BY
          CASE
            WHEN siding = 'SPUR-8' THEN 1
            WHEN siding = 'SPUR-9' THEN 2
            ELSE 3
          END,
          camera_name
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return jsonify([
        {
            "id": r[0],
            "camera_name": r[1],
            "siding": r[2],
            "status": r[3],
            "blur": r[4],
            "shaking": r[5]
        } for r in rows
    ])


@app.route("/add-camera", methods=["POST"])
def add_camera():
    data = request.json
    if not data or "camera_name" not in data or "siding" not in data:
        return jsonify({"error": "camera_name and siding required"}), 400
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO camera_records (camera_name, siding, status, blur, shaking)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (data["camera_name"], data["siding"], data.get("status", True), False, False))
    
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "camera added"})


@app.route("/toggle", methods=["POST"])
def toggle_camera():
    data = request.json
    if not data or "id" not in data or "status" not in data:
        return jsonify({"error": "id and status required"}), 400
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("UPDATE camera_records SET status = %s WHERE id = %s",
                (data["status"], data["id"]))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "status updated"})


@app.route("/toggle-blur", methods=["POST"])
def toggle_blur():
    data = request.json
    if not data or "id" not in data or "blur" not in data:
        return jsonify({"error": "id and blur required"}), 400
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("UPDATE camera_records SET blur = %s WHERE id = %s",
                (data["blur"], data["id"]))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "blur updated"})


@app.route("/toggle-shaking", methods=["POST"])
def toggle_shaking():
    data = request.json
    if not data or "id" not in data or "shaking" not in data:
        return jsonify({"error": "id and shaking required"}), 400
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("UPDATE camera_records SET shaking = %s WHERE id = %s",
                (data["shaking"], data["id"]))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "shaking updated"})


# ==================================================
# EXISTING WAGON/TRAIN ROUTES
# ==================================================
@app.route("/add-train", methods=["POST"])
def add_train():
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON required"}), 400
    
    train_id   = data.get("train_id")
    wagon_count = data.get("wagon_count")
    siding     = data.get("siding")
    max_bags   = data.get("max_bags")
    
    if not siding:
        return jsonify({"error": "siding is required"}), 400
    if not isinstance(max_bags, int) or max_bags <= 0:
        return jsonify({"error": "max_bags must be positive"}), 400
    
    try:
        # Register train (forward to train service)
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
        
        # Auto-start loading for this siding
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


@app.route("/update-rake-haulout", methods=["POST"])
def update_rake_haulout():
    data = request.get_json()
    rake_serial_number = data.get("rake_serial_number")
    haul_out_datetime  = data.get("rake_haul_out_datetime")
    
    if not rake_serial_number or not haul_out_datetime:
        return jsonify({
            "error": "rake_serial_number and rake_haul_out_datetime required"
        }), 400
    
    try:
        haul_out_dt = datetime.fromisoformat(haul_out_datetime)
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            UPDATE dispatch_records
            SET rake_haul_out_datetime = %s
            WHERE rake_serial_number = %s
        """, (haul_out_dt, rake_serial_number))
        
        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"error": "Rake not found"}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "rake_haul_out_updated"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reset-system", methods=["POST"])
def reset_system():
    data = request.get_json() or {}
    if data.get("confirm") != "YES":
        return jsonify({"error": "Send confirm=YES"}), 400
    
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
# HOME (now expects templates/index.html)
# ==================================================
@app.route("/")
def home():
    return render_template("index.html")


# ==================================================
# RUN
# ==================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)