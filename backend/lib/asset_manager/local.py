import asyncio
import shutil
from pathlib import Path
from typing import Optional

from backend.path_manager import PathManager

from .base import AssetManager


class LocalAssetManager(AssetManager):
    def __init__(self, root_dir: Path = PathManager().get_assets_root()):
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)

    async def upload_file(
        self,
        file_path: str,
        key: str,
        public: bool = False,
        content_type: Optional[str] = None,
    ) -> None:
        dest_path = self.root_dir / key
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copy, file_path, dest_path)

    async def generate_signed_url(self, key: str, expires_in: int = 3600) -> str:
        # No real signing; just return a URL FastAPI can serve locally
        return f"http://localhost:8000/assets/{key}"
