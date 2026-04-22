import asyncio
import logging
import uuid
from pathlib import Path
from typing import Optional

from fastapi import File, UploadFile
from fastapi.responses import JSONResponse

from backend.lib.utils import TempDirManager
from backend.route_handler.base import RouteHandler


class TimelensAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.router.add_api_route(
            "/api/new_photobook", self.new_photobook, methods=["POST"]
        )

    @staticmethod
    def is_accepted_mime(mime: Optional[str]) -> bool:
        return mime is not None and (
            mime.startswith("image/")
            # or mime.startswith("video/") # only images allowed for now
        )

    async def new_photobook(self, files: list[UploadFile] = File(...)) -> JSONResponse:
        job_id = f"job_{uuid.uuid4().hex}"

        # Upload files
        valid_files = [
            file
            for file in files
            if TimelensAPIHandler.is_accepted_mime(file.content_type)
        ]
        file_names = [file.filename for file in valid_files]
        skipped = [file.filename for file in files if file not in valid_files]
        logging.info({"accepted_files": file_names, "skipped_non_media": skipped})

        async with TempDirManager(valid_files) as file_map:
            # New structure: file_id is a unique per-file ID
            success: list[dict[str, str]] = []
            failed_uploads: list[dict[str, str]] = []
            failed_signing: list[dict[str, str]] = []

            async def safe_upload(
                _file_id: int, original_name: str, path: Path, mime: str
            ) -> None:
                key = f"uploads/{job_id}/{path.name}"
                try:
                    await self.app.asset_manager.upload_file(
                        file_path=str(path),
                        key=key,
                        content_type=mime,
                    )
                    success.append({"filename": original_name, "storage_key": key})
                except Exception as e:
                    msg = f"Failed to upload {original_name} → {key}: {e}"
                    logging.warning(msg)
                    failed_uploads.append({"filename": original_name, "error": str(e)})

            await asyncio.gather(
                *[
                    safe_upload(file_id, original_name, path, mime)
                    for file_id, (original_name, (path, mime)) in enumerate(
                        file_map.items()
                    )
                ]
            )

        # Generate signed URLs
        async def safe_sign(entry: dict[str, str]) -> None:
            try:
                signed_url = await self.app.asset_manager.generate_signed_url(
                    key=entry["storage_key"], expires_in=3600
                )
                entry["signed_url"] = signed_url
            except Exception as e:
                logging.warning(f"Failed to sign URL for {entry['filename']}: {e}")
                failed_signing.append(
                    {
                        "filename": entry["filename"],
                        "error": str(e),
                    }
                )

        await asyncio.gather(*[safe_sign(entry) for entry in success])

        return JSONResponse(
            {
                "job_id": job_id,
                "uploaded_files": [f for f in success if "signed_url" in f],
                "failed_uploads": failed_uploads,
                "failed_signing": failed_signing,
                "skipped_non_media": skipped,
            }
        )
