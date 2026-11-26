# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project


from collections.abc import AsyncGenerator

from agent_traffic_v3 import emit as _v3_emit, event_meta as _v3_event_meta, enabled as _v3_enabled
from http import HTTPStatus

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from vllm.entrypoints.openai.engine.protocol import ErrorResponse
from vllm.entrypoints.openai.responses.protocol import (
    ResponsesRequest,
    ResponsesResponse,
    StreamingResponsesResponse,
)
from vllm.entrypoints.openai.responses.serving import OpenAIServingResponses
from vllm.entrypoints.serve.utils.api_utils import (
    load_aware_call,
    validate_json_request,
    with_cancellation,
)
from vllm.logger import init_logger

logger = init_logger(__name__)

router = APIRouter()


def responses(request: Request) -> OpenAIServingResponses | None:
    return request.app.state.openai_serving_responses


async def _convert_stream_to_sse_events(
    generator: AsyncGenerator[StreamingResponsesResponse, None],
) -> AsyncGenerator[str, None]:
    """Convert the generator to a stream of events in SSE format"""
    _v3_response_id = None
    async for event in generator:
        event_type = getattr(event, "type", "unknown")
        if _v3_enabled():
            _v3_metadata = _v3_event_meta(event)
            if _v3_metadata.get('response_id'):
                _v3_response_id = _v3_metadata['response_id']
            else:
                _v3_metadata['response_id'] = _v3_response_id
        # https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
        if _v3_enabled():
            _v3_emit('sse_encoder','sse_encode_start', **_v3_metadata)
        event_data = (
            f"event: {event_type}\ndata: "
            f"{event.model_dump_json(indent=None, by_alias=True)}\n\n"
        )
        if _v3_enabled():
            _v3_emit('sse_encoder','sse_encode_done', encoded_bytes=len(event_data.encode('utf-8')),
                     **_v3_metadata)
            _v3_emit('sse_encoder','stream_yield', **_v3_metadata)
        yield event_data


@router.post(
    "/v1/responses",
    dependencies=[Depends(validate_json_request)],
    responses={
        HTTPStatus.OK.value: {"content": {"text/event-stream": {}}},
        HTTPStatus.BAD_REQUEST.value: {"model": ErrorResponse},
        HTTPStatus.NOT_FOUND.value: {"model": ErrorResponse},
        HTTPStatus.INTERNAL_SERVER_ERROR.value: {"model": ErrorResponse},
    },
)
@with_cancellation
@load_aware_call
async def create_responses(request: ResponsesRequest, raw_request: Request):
    if _v3_enabled():
        _v3_emit('api_server','request_validation_done',request_id=request.request_id,
                 path='/v1/responses',timestamp_semantics='FastAPI route start after JSON validation')
    handler = responses(raw_request)
    if handler is None:
        raise NotImplementedError("The model does not support Responses API")

    if _v3_enabled():
        _v3_emit('api_server','responses_handler_start',request_id=request.request_id)
    generator = await handler.create_responses(request, raw_request)

    if isinstance(generator, ErrorResponse):
        return JSONResponse(
            content=generator.model_dump(mode="json", by_alias=True),
            status_code=generator.error.code,
        )
    elif isinstance(generator, ResponsesResponse):
        return JSONResponse(content=generator.model_dump(mode="json", by_alias=True))

    return StreamingResponse(
        content=_convert_stream_to_sse_events(generator), media_type="text/event-stream"
    )


@router.get("/v1/responses/{response_id}")
@load_aware_call
async def retrieve_responses(
    response_id: str,
    raw_request: Request,
    starting_after: int | None = None,
    stream: bool | None = False,
):
    handler = responses(raw_request)
    if handler is None:
        raise NotImplementedError("The model does not support Responses API")

    response = await handler.retrieve_responses(
        response_id,
        starting_after=starting_after,
        stream=stream,
    )

    if isinstance(response, ErrorResponse):
        return JSONResponse(
            content=response.model_dump(mode="json", by_alias=True),
            status_code=response.error.code,
        )
    elif isinstance(response, ResponsesResponse):
        return JSONResponse(content=response.model_dump(mode="json", by_alias=True))
    return StreamingResponse(
        content=_convert_stream_to_sse_events(response), media_type="text/event-stream"
    )


@router.post("/v1/responses/{response_id}/cancel")
@load_aware_call
async def cancel_responses(response_id: str, raw_request: Request):
    handler = responses(raw_request)
    if handler is None:
        raise NotImplementedError("The model does not support Responses API")

    response = await handler.cancel_responses(response_id)

    if isinstance(response, ErrorResponse):
        return JSONResponse(
            content=response.model_dump(mode="json", by_alias=True),
            status_code=response.error.code,
        )
    return JSONResponse(content=response.model_dump(mode="json", by_alias=True))


def attach_router(app: FastAPI):
    app.include_router(router)
