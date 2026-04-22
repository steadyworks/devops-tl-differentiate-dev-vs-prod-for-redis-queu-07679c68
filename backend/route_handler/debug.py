from fastapi.responses import JSONResponse

from backend.lib.supabase.types.users import UserCreate
from backend.route_handler.base import RouteHandler


class DebugHandler(RouteHandler):
    def register_routes(self) -> None:
        self.router.add_api_route("/api/debug", self.debug, methods=["GET"])
        self.router.add_api_route(
            "/api/debug/sentry-debug",
            self.sentry_debug,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/api/debug/test-create-user",
            self.test_create_user,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/api/debug/test-enqueue-jobs",
            self.test_enqueue_jobs,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/api/debug/test-get-job-status",
            self.test_get_job_status,
            methods=["GET"],
        )

    async def debug(self) -> JSONResponse:
        return JSONResponse({"hello": "world"})

    async def sentry_debug(self) -> JSONResponse:
        _division_by_zero = 1 / 0
        return JSONResponse("")

    async def test_create_user(self) -> JSONResponse:
        _user_out = self.app.supabase_manager.create_user(
            UserCreate(email="test@gmail.com", name="test")
        )
        return JSONResponse({"success": True})

    async def test_enqueue_jobs(self) -> JSONResponse:
        await self.app.job_manager.enqueue("test123", ["a.png", "b.png"])
        status = await self.app.job_manager.get_status("test123")
        return JSONResponse(status)

    async def test_get_job_status(self) -> JSONResponse:
        status = await self.app.job_manager.get_status("test123")
        return JSONResponse(status)
