"""StreamCo Command Center backend.

Local:    cd app && .venv/bin/uvicorn backend.main:app --port 8010      (FORCE_FIXTURE=1 for recorded only)
Deployed: app.yaml runs uvicorn with the app's service principal.
"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException

from .config import FRONTEND_DIST
from .routers import ask, save, screens

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("backend")

app = FastAPI(title="StreamCo Command Center", docs_url="/api/docs", openapi_url="/api/openapi.json", redoc_url=None)
for r in (screens.router, ask.router, save.router):
    app.include_router(r)


def _error(status: int, message: str) -> JSONResponse:
    return JSONResponse({"error": {"message": message}}, status_code=status)


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    if exc.status_code == 404 and exc.detail == "Not Found":
        return _error(404, "That item isn't available in this demo.")
    if exc.status_code == 405:
        return _error(405, "That request isn't supported.")
    return _error(exc.status_code, str(exc.detail))


@app.exception_handler(RequestValidationError)
async def bad_request(request: Request, exc: RequestValidationError):
    return _error(400, "The request is missing a value or has an invalid one.")


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    log.exception("Unhandled error on %s %s", request.method, request.url.path)
    return _error(500, "Something went wrong on our side. Try again, or press F for the recorded copy.")


@app.api_route("/api/{rest:path}", methods=["GET", "POST"], include_in_schema=False)
async def api_not_found(rest: str):
    return _error(404, "That item isn't available in this demo.")


if (FRONTEND_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets")


@app.get("/{path:path}", include_in_schema=False)
async def spa(path: str):
    index = FRONTEND_DIST / "index.html"
    if index.is_file():
        return FileResponse(index, headers={"Cache-Control": "no-cache"})
    return JSONResponse({"ok": True, "message": "Backend is running. Build the frontend: cd app/frontend && npm run build"})
