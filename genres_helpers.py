from typing import Any, Dict, List


def item_genre_names(db: Any, item: Dict[str, Any]) -> List[str]:
    all_genres = db.get_all_genres_merged()
    return [all_genres.get(int(gid), str(gid)) for gid in item.get("genre_ids", []) if int(gid) in all_genres]


def sub_genre_names(db: Any, sub: Dict[str, Any]) -> List[str]:
    all_genres = db.get_all_genres_merged()
    return [all_genres.get(int(gid), str(gid)) for gid in sub.get("genre_ids", []) if int(gid) in all_genres]
