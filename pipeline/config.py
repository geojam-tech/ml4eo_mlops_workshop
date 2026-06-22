import os
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

load_dotenv()

# A blank token means "run token-free" (the default window is fully seeded). The
# old example placeholder is treated as blank too, so a Codespace that copied the
# previous .env.example doesn't send an invalid token and 401.
_gfw_token = os.getenv("GFW_TOKEN", "").strip()
GFW_TOKEN = "" if _gfw_token == "your_token_here" else _gfw_token

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = DATA_DIR / "metrics.db"

PORT_BBOX = [28.55, 60.28, 28.85, 60.38]

PORT_GEOJSON = {
    "type": "Polygon",
    "coordinates": [[
        [PORT_BBOX[0], PORT_BBOX[1]],
        [PORT_BBOX[2], PORT_BBOX[1]],
        [PORT_BBOX[2], PORT_BBOX[3]],
        [PORT_BBOX[0], PORT_BBOX[3]],
        [PORT_BBOX[0], PORT_BBOX[1]],
    ]],
}

# Default monitoring window, used only as the flow's default parameters. The
# flow processes whatever window it is given.
DATE_START = "2021-02-01"
DATE_END = "2024-02-28"

# Keep a scene if its footprint covers at least this fraction of PORT_BBOX.
COVERAGE_THRESHOLD = float(os.getenv("COVERAGE_THRESHOLD", "0.9"))

# Flow parameter choices, centralised here so the Prefect forms render dropdowns.
SITE = "primorsk"
Site = Literal["primorsk"]
Source = Literal["live", "cache"]
DEFAULT_MODEL = "default"   # the MLflow alias setup registers (points at strict)

EVENTS = [
    ("2021-12-23", "S1B failure"),
    ("2022-02-24", "Invasion"),
    ("2022-12-05", "EU crude embargo"),
    ("2023-01-01", "GFW AIS source switch"),
]
