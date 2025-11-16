import os
import time
import json
import re
import logging
import pymysql
import redis
import websockets
import asyncio

# configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_MESSAGE_CHANNEL = os.getenv("REDIS_MESSAGE_CHANNEL", "channel:history:s-*:r-*")
REDIS_SESSION_CHANNEL = os.getenv("REDIS_SESSION_CHANNEL", "channel:sessions:u-*")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", os.getenv("MYSQL_PASSWORD", "rancher-ai"))
MYSQL_DB = os.getenv("MYSQL_DATABASE", os.getenv("MYSQL_DB", "rancher-ai"))

AGENT_WS_SUMMARY_URL = os.getenv("AGENT_WS_SUMMARY_URL", "ws://localhost:8000/agent/ws/summary")
AGENT_WS_TIMEOUT = float(os.getenv("AGENT_WS_TIMEOUT", "5"))

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
                name VARCHAR(255) DEFAULT "",
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

def insert_or_update_session(session_id, user_id, active=True, name="", timestamp=None):
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sessions (session_id, user_id, active, name, created_at) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE active=%s, name=%s",
                (session_id, user_id, active, name, timestamp, active, name)
            )

            conn.commit()
    finally:
        conn.close()

def insert_or_update_message(session_id, request_id, role, text, timestamp=None):
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    try:
        with conn.cursor() as cur:
            # Insert or update message by appending text
            cur.execute(
                "INSERT INTO messages (session_id, request_id, role, message, created_at) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE role=%s, message=CONCAT(COALESCE(message,''), %s), created_at=%s",
                (session_id, request_id, role, text, timestamp, role, text, timestamp)
            )

        conn.commit()
    finally:
        conn.close()

def _get_previous_session_for_user(r, user_id):
    key = f"sessions:u-{user_id}"
    try:
        vals = r.lrange(key, 0, -1)

        if len(vals) >= 2:
            return json.loads(vals[-2])
    except Exception as e:
        logger.debug("Error reading previous session for user %s: %s", user_id, e)

    return None

async def get_session_messages(session_id):
    messages = []
    if session_id:
        # Fetch from DB
        conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT message FROM messages WHERE session_id=%s ORDER BY created_at ASC",
                    (session_id,)
                )
                rows = cur.fetchall()
                
                messages = [row[0] for row in rows]
        finally:
            conn.close()
    return messages

async def get_chat_summary(messages):
    try:
        async with websockets.connect(AGENT_WS_SUMMARY_URL) as ws:
            text = "\n" + "\n".join(f"- {str(m)}" for m in messages) + "\n"

            logger.info("Sending request to summary service %s", text)

            await ws.send(text)

            buffer = ""
            end_time = asyncio.get_event_loop().time() + AGENT_WS_TIMEOUT
            while True:
                timeout = max(0.1, end_time - asyncio.get_event_loop().time())
                try:
                    chunk = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.debug("Timeout waiting for more chunks from summary service")
                    break
                except websockets.ConnectionClosedOK:
                    logger.debug("WebSocket closed by server")
                    break
                except Exception as e:
                    logger.debug("WebSocket recv error: %s", e)
                    return None

                if isinstance(chunk, (bytes, bytearray)):
                    try:
                        chunk = chunk.decode("utf-8", errors="ignore")
                    except Exception:
                        chunk = str(chunk)

                buffer += chunk
                logger.debug("< Chunk received (len=%d)", len(chunk))

                if "</message>" in buffer:
                    logger.debug("Terminator '</message>' found in buffer")
                    break

                if asyncio.get_event_loop().time() >= end_time:
                    logger.debug("Overall deadline reached while waiting for terminator")
                    break

            # Try extract between <message>...</message>
            m = re.search(r"<message>(.*?)</message>", buffer, re.DOTALL)
            if m:
                result = m.group(1).strip()
            else:
                # Fallback to full buffer
                result = buffer.strip() if buffer else None

            logger.debug("Final summary-service response=%s", result)

            return result

    except Exception as e:
        logger.debug("WebSocket connect/send failed to %s: %s", AGENT_WS_SUMMARY_URL, e)
        return None

async def run():
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
    logging.getLogger().setLevel(LOG_LEVEL)

    logger.info("Initializing DB supervisor...")

    wait_for_db()
    tables_init()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pub = r.pubsub(ignore_subscribe_messages=True)

    # Subscribe patterns on the same PubSub
    pub.psubscribe(REDIS_MESSAGE_CHANNEL)
    logger.info("PSubscribed to pattern: %s", REDIS_MESSAGE_CHANNEL)
    pub.psubscribe(REDIS_SESSION_CHANNEL)
    logger.info("PSubscribed to pattern: %s", REDIS_SESSION_CHANNEL)

    key_pattern_msg = re.compile(r"^channel:history:s-(?P<session>[^:]+):r-(?P<request>.+)$")
    key_pattern_session = re.compile(r"^channel:sessions:u-(?P<user>.+)$")

    for item in pub.listen():
        logger.debug("Received pubsub item: %s", item)
        try:
            itype = item.get("type")
            if itype not in ("pmessage", "message"):
                continue

            channel = item.get("channel", "") or ""

            # Message channel
            m = key_pattern_msg.match(channel)
            if m:
                session_id = m.group("session")
                request_id = m.group("request")
                logger.debug("Message channel, extracted session_id: %s request_id: %s", session_id, request_id)

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
                continue

            m2 = key_pattern_session.match(channel)
            if m2:
                user_id = m2.group("user")
                logger.debug("Session channel, extracted user_id: %s", user_id)

                try:
                    payload = json.loads(item.get("data", "{}"))
                except Exception:
                    payload = {}

                # Insert the new session from payload
                session_id = payload.get("session_id")
                if session_id:
                    try:
                        insert_or_update_session(session_id, user_id, active=payload.get("active", True), name=payload.get("name", ""), timestamp=payload.get("created_at"))
                        logger.info("Inserted new session %s for user %s", session_id, user_id)
                    
                        # Check previous session for missing name
                        prev_session = _get_previous_session_for_user(r, user_id)

                        # Try to get the chat summary from websocket summary service
                        name_for_previous = None
                        if prev_session:
                            try:
                                messages = await get_session_messages(prev_session["session_id"])
                                if messages:
                                    name_for_previous = await get_chat_summary(messages)
                            except Exception as e:
                                logger.debug("Failed to obtain name from websocket summary service: %s", e)

                        # TODO prev_session.get("name", None) should be checked in the DB too
                        if prev_session and not prev_session.get("name", None) and name_for_previous:
                            try:
                                insert_or_update_session(prev_session["session_id"], user_id, active=True, name=name_for_previous, timestamp=prev_session["created_at"])
                                logger.info("Updated previous session %s for user %s with name=%s", prev_session["session_id"], user_id, name_for_previous)
                            except Exception as e:
                                logger.error("Failed to update previous session in DB: %s", e)

                    except Exception as e:
                        logger.error("Failed to insert new session in DB: %s", e)
                else:
                    logger.debug("No session_id in payload; skipping new-session insert (payload=%s)", payload)

                continue

            # Otherwise ignore / debug
            logger.debug("Ignored pubsub item on channel %s: %s", channel, item)

        except Exception as e:
            logger.exception("Error handling pubsub item: %s", e)

if __name__ == "__main__":
   asyncio.run(run())