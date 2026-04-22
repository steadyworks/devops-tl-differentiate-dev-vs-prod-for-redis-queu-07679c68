import os

from supabase import Client, create_client

from backend.lib.supabase.types.users import UserCreate, UserOut
from backend.lib.utils import none_throws


class SupabaseManager:
    def __init__(self) -> None:
        self.client: Client = create_client(
            none_throws(os.getenv("SUPABASE_URL")),
            none_throws(os.getenv("SUPABASE_SERVICE_ROLE_KEY")),
        )

    def create_user(self, user: UserCreate) -> UserOut:
        res = self.client.table("users").insert(user.model_dump()).execute()

        if not res.data:
            raise ValueError(
                f"Failed to insert user: {res.status_code}, {res.response_text}"
            )

        return UserOut(**res.data[0])
