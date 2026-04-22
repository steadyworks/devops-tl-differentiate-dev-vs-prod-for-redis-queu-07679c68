from typing import TYPE_CHECKING

from fastapi import APIRouter

if TYPE_CHECKING:
    from backend.app import TimelensApp  # Avoids actual circular import


class RouteHandler:
    def __init__(self, app: "TimelensApp") -> None:
        self.app = app
        self.router = APIRouter()
        self.register_routes()

    def register_routes(self) -> None:
        pass

    def get_router(self) -> APIRouter:
        return self.router
