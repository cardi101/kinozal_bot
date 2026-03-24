from typing import Any, Dict, List


def item_genre_names(db: Any, item: Dict[str, Any]) -> List[str]:
    all_genres = db.get_all_genres_merged()
    result = []
    for gid in item.get("genre_ids", []):
        try:
            gid_int = int(gid)
        except (TypeError, ValueError):
            continue
        if gid_int in all_genres:
            result.append(all_genres[gid_int])
    return result


def sub_genre_names(db: Any, sub: Dict[str, Any]) -> List[str]:
    all_genres = db.get_all_genres_merged()
    result = []
    for gid in sub.get("genre_ids", []):
        try:
            gid_int = int(gid)
        except (TypeError, ValueError):
            continue
        if gid_int in all_genres:
            result.append(all_genres[gid_int])
    return result
