import os
import requests

def get_booking_id_for_clip(recorded_at: str) -> str | None:
    url = os.getenv("PICKLE_PLANNER_MATCH_URL")
    api_key = os.getenv("PICKLE_PLANNER_API_KEY")
    api_key_header = os.getenv("PICKLE_PLANNER_API_KEY_HEADER", "x-api-key")

    if not url:
        raise RuntimeError("Missing PICKLE_PLANNER_MATCH_URL")

    if not api_key:
        raise RuntimeError("Missing PICKLE_PLANNER_API_KEY")

    response = requests.post(
        url,
        headers={
            "Content-Type": "application/json",
            api_key_header: api_key,
        },
        json={
            "recorded_at": recorded_at,
        },
        timeout=15,
    )

    if not response.ok:
        raise RuntimeError(
            f"Pickle Planner match failed: {response.status_code} {response.text}"
        )

    data = response.json()
    booking_id = data.get("booking_id")

    if booking_id and isinstance(booking_id, str):
        return booking_id.strip()

    return None