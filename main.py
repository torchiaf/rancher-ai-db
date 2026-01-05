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
REDIS_MESSAGE_CHANNEL = os.getenv("REDIS_MESSAGE_CHANNEL", "channel:history:c-*:r-*")
REDIS_CHAT_CHANNEL = os.getenv("REDIS_CHAT_CHANNEL", "channel:chats:u-*")

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
            CREATE TABLE IF NOT EXISTS chats (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                chat_id VARCHAR(255),
                user_id VARCHAR(255),
                active BOOLEAN DEFAULT TRUE,
                name VARCHAR(255) DEFAULT "",
                created_at NUMERIC(20),
                UNIQUE KEY ux_chat_user (chat_id, user_id),
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                chat_id VARCHAR(255),
                request_id VARCHAR(255),
                role VARCHAR(32),
                message LONGTEXT,
                context LONGTEXT,
                tags JSON NOT NULL DEFAULT ('[]'),
                created_at NUMERIC(20),
                PRIMARY KEY (chat_id, request_id, role)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
    conn.close()

def insert_or_update_chat(chat_id, user_id, active=1, name="", timestamp=None):
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chats (chat_id, user_id, active, name, created_at) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE active=%s, name=COALESCE(NULLIF(%s, ''), name), created_at=COALESCE(%s, created_at)",
                (chat_id, user_id, active, name, timestamp, active, name, timestamp)
            )

            conn.commit()
    finally:
        conn.close()

def insert_or_update_message(chat_id, request_id, role, text, context=None, tags=None, timestamp=None):
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    try:
        with conn.cursor() as cur:
            # Insert or update message by appending text
            cur.execute(
                "INSERT INTO messages (chat_id, request_id, role, message, context, tags, created_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE role=%s, message=CONCAT(COALESCE(message,''), %s), context=%s, tags=%s, created_at=%s",
                (chat_id, request_id, role, text, context, tags, timestamp, role, text, context, tags, timestamp)
            )

        conn.commit()
    finally:
        conn.close()

async def get_chats():
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    chats = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT chat_id, user_id, active, name, created_at FROM chats",
            )
            res = cur.fetchall()
            for row in res:
                chats.append({
                    "chat_id": row[0],
                    "user_id": row[1],
                    "active": row[2],
                    "name": row[3],
                    "created_at": row[4],
                })
    finally:
        conn.close()
    return chats

async def get_chat_messages(chat_id):
    messages = []
    if chat_id:
        # Fetch from DB
        conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT message FROM messages WHERE chat_id=%s ORDER BY created_at ASC",
                    (chat_id,)
                )
                rows = cur.fetchall()
                
                messages = [row[0] for row in rows]
        finally:
            conn.close()
    return messages

async def assign_name_to_previous_chats(user_id: str, active_chat_id: str, max_messages: int = 5):
    """
    Assign a name to a chat.
    """
    chats = await get_chats()
    
    for chat in chats:
        if chat["user_id"] == user_id and chat["chat_id"] != active_chat_id and not chat["name"]:
            logger.debug("Assigning name to chat %s for user %s", chat["chat_id"], user_id)
            await assign_name_to_chat(user_id, chat_id=chat["chat_id"], chat=chat, max_messages=max_messages)
    
async def assign_name_to_chat(user_id: str, chat_id: str, chat, max_messages: int = 5):
    name = None

    try:
        messages = await get_chat_messages(chat_id)
        if messages:
            name = await get_chat_summary(messages[:max_messages])
    except Exception as e:
        logger.debug("Failed to obtain name from websocket summary service: %s", e)

    if name:
        try:
            insert_or_update_chat(chat_id=chat_id, user_id=user_id, active=chat["active"], name=name, timestamp=chat["created_at"])
            logger.info("Updated chat %s for user %s with name='%s'", chat_id, user_id, name)
        except Exception as e:
            logger.error("Failed to update chat in DB: %s", e)

async def get_chat_summary(messages):
    try:
        async with websockets.connect(AGENT_WS_SUMMARY_URL) as ws:
            text = "\n" + "\n".join(f"- {str(m)}" for m in messages) + "\n"

            logger.info("Sending summary request with text: '%s'", text)

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
                logger.debug("Message Chunk received (len=%d)", len(chunk))

                if "</message>" in buffer:
                    logger.debug("Terminator '</message>' found in buffer")
                    break

                if asyncio.get_event_loop().time() >= end_time:
                    logger.debug("Deadline reached while waiting for terminator")
                    break

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
    pub.psubscribe(REDIS_CHAT_CHANNEL)
    logger.info("PSubscribed to pattern: %s", REDIS_CHAT_CHANNEL)

    key_pattern_msg = re.compile(r"^channel:history:c-(?P<chat>[^:]+):r-(?P<request>.+)$")
    key_pattern_chat = re.compile(r"^channel:chats:u-(?P<user>.+)$")

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
                chat_id = m.group("chat")
                request_id = m.group("request")
                logger.debug("Message channel, extracted chat_id: %s request_id: %s", chat_id, request_id)

                try:
                    payload = json.loads(item.get("data", "{}"))
                except Exception:
                    payload = {}
                if payload:
                    try:
                        insert_or_update_message(
                            chat_id,
                            request_id,
                            payload.get("role"),
                            payload.get("text"),
                            payload.get("context"),
                            payload.get("tags"),
                            payload.get("ts")
                        )
                        logger.debug("Updated message into DB: %s %s %s", chat_id, request_id, payload)
                    except Exception as e:
                        logger.error("DB insert failed: %s", e)
                continue

            m2 = key_pattern_chat.match(channel)
            if m2:
                user_id = m2.group("user")
                logger.debug("Chat channel, extracted user_id: %s", user_id)

                try:
                    payload = json.loads(item.get("data", "{}"))
                except Exception:
                    payload = {}

                # Insert the new chat from payload
                chat_id = payload.get("chat_id")
                if chat_id:
                    try:
                        is_active = payload.get("active", 1)

                        insert_or_update_chat(
                            chat_id,
                            user_id,
                            active=is_active,
                            name=payload.get("name", ""),
                            timestamp=payload.get("created_at")
                        )
                        logger.info("Inserted/Updated chat into DB: %s for user %s with payload %s", chat_id, user_id, payload)
                        
                        if is_active:
                            await assign_name_to_previous_chats(user_id, chat_id)

                    except Exception as e:
                        logger.error("Failed to insert new chat in DB: %s", e)
                else:
                    logger.debug("No chat_id in payload; skipping new-chat insert (payload=%s)", payload)

                continue

            # Otherwise ignore / debug
            logger.debug("Ignored pubsub item on channel %s: %s", channel, item)

        except Exception as e:
            logger.exception("Error handling pubsub item: %s", e)

if __name__ == "__main__":
   asyncio.run(run())