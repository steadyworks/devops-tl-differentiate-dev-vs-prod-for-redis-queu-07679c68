import shutil
import uuid
from pathlib import Path
from types import TracebackType
from typing import Dict, List, Optional, TypeVar

from fastapi import UploadFile

T = TypeVar("T")


def none_throws(value: Optional[T], message: str = "Value cannot be None") -> T:
    if value is None:
        raise Exception(message)
    return value


class TempDirManager:
    def __init__(self, upload_files: List[UploadFile], tmp_root: Path = Path("/tmp")):
        self.upload_files = upload_files
        self.tmp_root = tmp_root
        self.temp_dir: Path = tmp_root / f"job_{uuid.uuid4().hex}"
        self.saved_paths: Dict[str, tuple[Path, str]] = {}

    async def __aenter__(self) -> Dict[str, tuple[Path, str]]:
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        for upload_file in self.upload_files:
            # Fallbacks for missing filename or content_type
            original_name = upload_file.filename or f"unnamed_{uuid.uuid4().hex}.bin"
            ext = Path(original_name).suffix or ".bin"
            safe_name = f"{uuid.uuid4().hex}{ext}"
            temp_path = self.temp_dir / safe_name
            content_type = upload_file.content_type or "application/octet-stream"

            contents = await upload_file.read()
            with open(temp_path, "wb") as f:
                f.write(contents)

            self.saved_paths[original_name] = (temp_path, content_type)

        return self.saved_paths

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        return None
