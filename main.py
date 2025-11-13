import os
import time
import json
import re
import logging
import pymysql
import redis

# configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "channel:history:s-*:r-*")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", os.getenv("MYSQL_PASSWORD", "rancher-ai"))
MYSQL_DB = os.getenv("MYSQL_DATABASE", os.getenv("MYSQL_DB", "rancher-ai"))

def wait_for_db():
    while True:
        try:
            conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, connect_timeout=5)
            conn.close()
            return
        except Exception as e:
            logger.warning("Waiting for DB: %s", e)
            time.sleep(1)

def tables_init():
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                session_id VARCHAR(255),
                user_id VARCHAR(255),
                active BOOLEAN DEFAULT TRUE,
                created_at NUMERIC(20),
                UNIQUE KEY ux_session_user (session_id, user_id),
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                session_id VARCHAR(255),
                request_id VARCHAR(255),
                role VARCHAR(32),
                message MEDIUMTEXT,
                created_at NUMERIC(20),
                PRIMARY KEY (session_id, request_id, role)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
    conn.close()

# def insert_session(session_id, user_id):
#     conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
#     with conn.cursor() as cur:
#         cur.execute("UPDATE sessions SET active=FALSE WHERE user_id=%s", user_id)
#         cur.execute("INSERT INTO sessions (session_id, user_id, active) VALUES (%s, %s, TRUE)", (session_id, user_id))
#         conn.commit()
#     conn.close()

def insert_or_update_message(session_id, request_id, role, text, timestamp=None):
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    with conn.cursor() as cur:
        # Insert or update message by appending text
        cur.execute(
            "INSERT INTO messages (session_id, request_id, role, message, created_at) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE role=%s, message=CONCAT(COALESCE(message,''), %s), created_at=%s",
            (session_id, request_id, role, text, timestamp, role, text, timestamp)
        )

        conn.commit()
    conn.close()

# def update_session_active(session_id, user_id, active):
#     conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
#     with conn.cursor() as cur:
#         cur.execute("UPDATE sessions SET active=FALSE WHERE user_id=%s", user_id)
#         cur.execute("UPDATE sessions SET active=%s WHERE session_id=%s", (active, session_id))
#         conn.commit()
#     conn.close()

def run():
    logger.info("Initializing DB supervisor...")

    wait_for_db()
    tables_init()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pub = r.pubsub(ignore_subscribe_messages=True)

    # Use pattern subscribe to match parametric channels like history:s-<session_id>:r-<request_id>
    pub.psubscribe(REDIS_CHANNEL)
    logger.info("PSubscribed to pattern: %s", REDIS_CHANNEL)

    key_pattern = re.compile(r"^channel:history:s-(?P<session>[^:]+):r-(?P<request>.+)$")
    for item in pub.listen():
        logger.debug("Message received from channel pattern %s: %s", REDIS_CHANNEL, item)
        match = key_pattern.match(item.get("channel", "") or "")
        if match:
            session_id = match.group("session")
            request_id = match.group("request")
            logger.debug("Extracted session_id: %s request_id: %s", session_id, request_id)

            try:
                payload = json.loads(item.get("data", "{}"))
            except Exception:
                payload = {}
            if payload:
                try:
                    insert_or_update_message(session_id, request_id, payload.get("role"), payload.get("text"), payload.get("ts"))
                    logger.debug("Updated message into DB: %s %s %s", session_id, request_id, payload)
                except Exception as e:
                    logger.error("DB insert failed: %s", e)

if __name__ == "__main__":
    run()