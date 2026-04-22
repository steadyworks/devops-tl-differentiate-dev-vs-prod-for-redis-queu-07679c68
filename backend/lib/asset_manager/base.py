from abc import ABC, abstractmethod
from typing import Optional


class AssetManager(ABC):
    @abstractmethod
    async def upload_file(
        self,
        file_path: str,
        key: str,
        public: bool = False,
        content_type: Optional[str] = None,
    ) -> None: ...

    @abstractmethod
    async def generate_signed_url(self, key: str, expires_in: int = 3600) -> str: ...
