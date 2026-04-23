import os
import logging
from datetime import datetime

from procrastinate import App, PsycopgConnector
from langchain_core.messages import AIMessageChunk, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.ai.agent import GraphBuilder
from src.ai.config import get_llm
from src.cache.redis import get_redis

logger = logging.getLogger(__name__)

STREAM_TTL_SECONDS = int(os.getenv("STREAM_TTL_SECONDS", "900"))

app = App(
    connector=PsycopgConnector(
        conninfo=os.getenv("DATABASE_URI", ""),
    )
)


@app.task(queue="chat", name="generate_response")
async def generate_response(thread_id: str, stream_id: str, message: str) -> None:
    r = get_redis()
    llm = get_llm("chat")
    config = {
        "configurable": {
            "thread_id": thread_id,
            "last_activity_time": datetime.now().isoformat(),
        }
    }
    try:
        async with AsyncPostgresSaver.from_conn_string(
            conn_string=os.getenv("DATABASE_URI", ""),
        ) as checkpointer:
            await checkpointer.setup()
            graph = GraphBuilder(
                llm=llm, checkpointer=checkpointer, store=None
            ).get_graph()

            events = graph.astream(
                {"messages": HumanMessage(content=message)},
                config,
                stream_mode="messages",
            )

            async for chunk, metadata in events:
                if (
                    isinstance(chunk, AIMessageChunk)
                    and chunk.content
                    and metadata.get("langgraph_node", "") == "agent"
                ):
                    await r.xadd(stream_id, {"event": "chunk", "data": chunk.content})

                if isinstance(chunk, AIMessageChunk) and chunk.tool_calls:
                    for tool_call in chunk.tool_calls:
                        tool_name = tool_call["name"].strip()
                        if tool_name:
                            await r.xadd(
                                stream_id, {"event": "tool_call", "data": tool_name}
                            )

                if chunk.response_metadata and chunk.response_metadata.get("finish_reason"):
                    msg_id = await r.xadd(
                        stream_id, {"event": "system", "data": "message_ended"}
                    )
                    await r.set(
                        f"{stream_id}:message_ended", msg_id, ex=STREAM_TTL_SECONDS
                    )

                await r.expire(thread_id, STREAM_TTL_SECONDS)
                await r.expire(stream_id, STREAM_TTL_SECONDS)
                await r.expire(f"{stream_id}:message_ended", STREAM_TTL_SECONDS)
                await r.expire(f"{stream_id}:status", STREAM_TTL_SECONDS)
    except Exception as e:
        logger.error(f"Error generating response: {e}")
        await r.xadd(stream_id, {"event": "system", "data": "error"})
    finally:
        await r.xadd(stream_id, {"event": "system", "data": "end"})
        await r.set(f"{stream_id}:status", "completed", ex=STREAM_TTL_SECONDS)
