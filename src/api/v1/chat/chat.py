from uuid import uuid4, UUID

from fastapi import APIRouter, Response, status
from sse_starlette.sse import EventSourceResponse

from src.cache.redis import get_redis
from src.schema.chat import ChatRequest
from src.tasks.queue import STREAM_TTL_SECONDS, generate_response

router = APIRouter()


@router.post("/message", status_code=status.HTTP_200_OK)
async def chat_message(request: ChatRequest) -> None:
    r = get_redis()

    stream_id = str(uuid4())
    thread_id = str(request.thread_id)
    await r.set(f"{stream_id}:status", "running", ex=STREAM_TTL_SECONDS)
    await r.set(thread_id, stream_id, ex=STREAM_TTL_SECONDS)

    await generate_response.defer_async(
        thread_id=thread_id,
        stream_id=stream_id,
        message=request.message,
    )

    return None


@router.get("/stream")
async def stream_tokens(thread_id: UUID):
    r = get_redis()
    STREAM_ID: str | None = await r.get(str(thread_id))
    if not STREAM_ID:
        return Response(status_code=204)

    stream_status: str | None = await r.get(f"{STREAM_ID}:status")
    if not stream_status or stream_status == "completed":
        return Response(status_code=204)

    message_ended_id = await r.get(f"{STREAM_ID}:message_ended")

    async def event_generator():
        last_id = message_ended_id or "0"
        while True:
            messages = await r.xread(streams={STREAM_ID: last_id}, block=3000)
            if not messages:
                continue

            for _, msgs in messages:
                for msg_id, data in msgs:
                    yield {
                        "id": msg_id,
                        "event": data["event"],
                        "data": data["data"],
                    }
                    last_id = msg_id

                    if data["event"] == "system" and data["data"] == "end":
                        return

    return EventSourceResponse(event_generator())
