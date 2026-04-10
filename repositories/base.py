from typing import Any, Dict, Optional


class BaseRepository:
    def __init__(self, db: Any) -> None:
        self.db = db

    @property
    def conn(self) -> Any:
        return self.db.conn

    @property
    def lock(self) -> Any:
        return self.db.lock

    def row_to_dict(self, row: Optional[Any]) -> Optional[Dict[str, Any]]:
        return self.db.row_to_dict(row)
