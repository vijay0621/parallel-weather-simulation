import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests
from mpi4py import MPI

from tn_districts import DISTRICTS

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "weather.json")


def ensure_data_dir_exists() -> None:
	if not os.path.isdir(DATA_DIR):
		os.makedirs(DATA_DIR, exist_ok=True)


def partition_indices(total: int, parts: int, index: int) -> Tuple[int, int]:
	base = total // parts
	remainder = total % parts
	# First 'remainder' parts get one extra
	if index < remainder:
		start = index * (base + 1)
		end = start + base + 1
	else:
		start = remainder * (base + 1) + (index - remainder) * base
		end = start + base
	return start, min(end, total)


def create_session_with_retries() -> requests.Session:
	session = requests.Session()
	adapter = requests.adapters.HTTPAdapter(max_retries=3)
	session.mount("http://", adapter)
	session.mount("https://", adapter)
	session.headers.update({"Accept": "application/json"})
	return session


def fetch_weather_for_query(session: requests.Session, api_key: str, query: str) -> Dict[str, Any]:
	params = {
		"q": f"{query}, Tamil Nadu, IN",
		"appid": api_key,
		"units": "metric",
	}
	response = session.get(OPENWEATHER_URL, params=params, timeout=15)
	response.raise_for_status()
	return response.json()


def extract_metrics(payload: Dict[str, Any]) -> Dict[str, float]:
	main = payload.get("main", {})
	wind = payload.get("wind", {})
	rain = payload.get("rain", {})

	rainfall = 0.0
	if isinstance(rain, dict):
		# Prefer last 1 hour if available, else 3 hour
		rainfall = float(rain.get("1h", rain.get("3h", 0.0)) or 0.0)

	return {
		"temperature_c": float(main.get("temp")) if main.get("temp") is not None else None,
		"humidity_pct": float(main.get("humidity")) if main.get("humidity") is not None else None,
		"wind_speed_ms": float(wind.get("speed")) if wind.get("speed") is not None else None,
		"rainfall_mm": rainfall,
	}


def main() -> None:
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	size = comm.Get_size()

	api_key = os.environ.get("OPENWEATHER_API_KEY")
	if not api_key:
		if rank == 0:
			print("ERROR: OPENWEATHER_API_KEY environment variable is not set.", file=sys.stderr)
		sys.exit(2)

	# Broadcast the districts list to all ranks (root provides)
	if rank == 0:
		districts = DISTRICTS
	else:
		districts = None
	districts = comm.bcast(districts, root=0)

	total = len(districts)
	start, end = partition_indices(total, size, rank)
	local_slice = districts[start:end]

	session = create_session_with_retries()
	local_results: List[Dict[str, Any]] = []

	for item in local_slice:
		district_name = item["district"]
		query = item["query"]
		try:
			payload = fetch_weather_for_query(session, api_key, query)
			metrics = extract_metrics(payload)
			local_results.append({
				"district": district_name,
				"query": query,
				"processor_rank": rank,
				**metrics,
			})
		except Exception as exc:  # noqa: BLE001
			local_results.append({
				"district": district_name,
				"query": query,
				"processor_rank": rank,
				"temperature_c": None,
				"humidity_pct": None,
				"wind_speed_ms": None,
				"rainfall_mm": None,
				"error": str(exc),
			})

	gathered: List[List[Dict[str, Any]]] = comm.gather(local_results, root=0)

	if rank == 0:
		ensure_data_dir_exists()
		flat: List[Dict[str, Any]] = []
		for part in gathered:
			flat.extend(part)

		# Sort deterministically by district name
		flat.sort(key=lambda r: r["district"])

		# Compute averages ignoring None
		def avg(values: List[float]) -> float:
			valid = [v for v in values if isinstance(v, (int, float))]
			return round(sum(valid) / len(valid), 2) if valid else None

		averages = {
			"temperature_c": avg([r.get("temperature_c") for r in flat]),
			"humidity_pct": avg([r.get("humidity_pct") for r in flat]),
			"wind_speed_ms": avg([r.get("wind_speed_ms") for r in flat]),
			"rainfall_mm": avg([r.get("rainfall_mm") for r in flat]),
		}

		out_obj = {
			"last_updated": datetime.now(timezone.utc).isoformat(),
			"districts": flat,
			"averages": averages,
			"meta": {
				"total_districts": total,
				"mpi_processes": size,
			},
		}

		with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
			json.dump(out_obj, f, ensure_ascii=False, indent=2)

		print(f"Wrote {len(flat)} records to {OUTPUT_PATH}")


if __name__ == "__main__":
	main()