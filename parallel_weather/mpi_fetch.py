import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from mpi4py import MPI

from tn_districts import DISTRICTS

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
ONECALL_FORECAST_URL = "https://api.openweathermap.org/data/3.0/onecall"
ONECALL_TIMEMACHINE_URL = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
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


def extract_current_metrics_and_coord(payload: Dict[str, Any]) -> Tuple[Dict[str, Optional[float]], Optional[Dict[str, float]]]:
	main = payload.get("main", {})
	wind = payload.get("wind", {})
	rain = payload.get("rain", {})
	coord = payload.get("coord") or None

	rainfall = 0.0
	if isinstance(rain, dict):
		# Prefer last 1 hour if available, else 3 hour
		rainfall = float(rain.get("1h", rain.get("3h", 0.0)) or 0.0)

	metrics = {
		"temperature_c": float(main.get("temp")) if main.get("temp") is not None else None,
		"humidity_pct": float(main.get("humidity")) if main.get("humidity") is not None else None,
		"wind_speed_ms": float(wind.get("speed")) if wind.get("speed") is not None else None,
		"rainfall_mm": rainfall,
	}
	if isinstance(coord, dict) and coord.get("lat") is not None and coord.get("lon") is not None:
		return metrics, {"lat": float(coord["lat"]), "lon": float(coord["lon"])}
	return metrics, None


def list_past_days(num_days: int) -> List[datetime]:
	# Exclude today, return last num_days dates (UTC midnight)
	today = datetime.now(timezone.utc).date()
	return [datetime.combine(today - timedelta(days=i), datetime.min.time(), tzinfo=timezone.utc) for i in range(1, num_days + 1)][::-1]


def list_next_days(num_days: int) -> List[datetime]:
	# Include the next num_days starting tomorrow
	today = datetime.now(timezone.utc).date()
	return [datetime.combine(today + timedelta(days=i), datetime.min.time(), tzinfo=timezone.utc) for i in range(1, num_days + 1)]


def aggregate_hourly_to_daily(hourly_items: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
	if not hourly_items:
		return {"temperature_c": None, "humidity_pct": None, "wind_speed_ms": None, "rainfall_mm": None}

	temps: List[float] = []
	humids: List[float] = []
	winds: List[float] = []
	rain_sum = 0.0
	for h in hourly_items:
		t = h.get("temp")
		if t is not None:
			try:
				temps.append(float(t))
			except Exception:
				pass
		hh = h.get("humidity")
		if hh is not None:
			try:
				humids.append(float(hh))
			except Exception:
				pass
		ws = h.get("wind_speed") or (h.get("wind", {}) or {}).get("speed")
		if ws is not None:
			try:
				winds.append(float(ws))
			except Exception:
				pass
		rain = h.get("rain")
		if isinstance(rain, dict):
			val = rain.get("1h") or rain.get("3h")
			if val is not None:
				try:
					rain_sum += float(val)
				except Exception:
					pass

	def avg(lst: List[float]) -> Optional[float]:
		return round(sum(lst) / len(lst), 2) if lst else None

	return {
		"temperature_c": avg(temps),
		"humidity_pct": avg(humids),
		"wind_speed_ms": avg(winds),
		"rainfall_mm": round(train_sum, 2),
	}


def fetch_onecall_timemachine(session: requests.Session, api_key: str, lat: float, lon: float, dt_unix: int) -> Dict[str, Optional[float]]:
	params = {
		"lat": lat,
		"lon": lon,
		"dt": dt_unix,
		"appid": api_key,
		"units": "metric",
	}
	resp = session.get(ONECALL_TIMEMACHINE_URL, params=params, timeout=20)
	resp.raise_for_status()
	data = resp.json()
	hourly = data.get("hourly") or data.get("data") or []
	return aggregate_hourly_to_daily(hourly)


def fetch_onecall_forecast_daily(session: requests.Session, api_key: str, lat: float, lon: float) -> List[Dict[str, Optional[float]]]:
	params = {
		"lat": lat,
		"lon": lon,
		"exclude": "current,minutely,hourly,alerts",
		"appid": api_key,
		"units": "metric",
	}
	resp = session.get(ONECALL_FORECAST_URL, params=params, timeout=20)
	resp.raise_for_status()
	payload = resp.json()
	daily = payload.get("daily", [])
	results: List[Dict[str, Optional[float]]] = []
	for d in daily:
		temp = d.get("temp") or {}
		metrics = {
			"temperature_c": float(temp.get("day")) if temp.get("day") is not None else None,
			"humidity_pct": float(d.get("humidity")) if d.get("humidity") is not None else None,
			"wind_speed_ms": float(d.get("wind_speed")) if d.get("wind_speed") is not None else None,
			"rainfall_mm": float(d.get("rain")) if d.get("rain") is not None else 0.0,
		}
		results.append(metrics)
	return results


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

	# PHASE 1: Spatial parallelism across districts for current weather and coordinates
	total = len(districts)
	start, end = partition_indices(total, size, rank)
	local_slice = districts[start:end]

	session = create_session_with_retries()
	local_current: List[Dict[str, Any]] = []

	for item in local_slice:
		district_name = item["district"]
		query = item["query"]
		try:
			payload = fetch_weather_for_query(session, api_key, query)
			metrics, coord = extract_current_metrics_and_coord(payload)
			local_current.append({
				"district": district_name,
				"query": query,
				"processor_rank": rank,
				"coord": coord,
				"current": {**metrics, "processor_rank": rank},
			})
		except Exception as exc:  # noqa: BLE001
			local_current.append({
				"district": district_name,
				"query": query,
				"processor_rank": rank,
				"coord": None,
				"current": {
					"temperature_c": None,
					"humidity_pct": None,
					"wind_speed_ms": None,
					"rainfall_mm": None,
					"error": str(exc),
					"processor_rank": rank,
				},
			})

	gathered_current: List[List[Dict[str, Any]]] = comm.gather(local_current, root=0)

	# Root prepares temporal tasks and distributes
	if rank == 0:
		# Build district map and coords
		flat_current: List[Dict[str, Any]] = []
		for part in gathered_current:
			flat_current.extend(part)
		flat_current.sort(key=lambda r: r["district"])
		district_to_coord: Dict[str, Optional[Dict[str, float]]] = {r["district"]: r.get("coord") for r in flat_current}

		# Build history and forecast task lists
		hist_dates = list_past_days(7)
		fc_dates = list_next_days(7)
		history_tasks: List[Dict[str, Any]] = []
		forecast_tasks: List[Dict[str, Any]] = []
		for r in flat_current:
			d = r["district"]
			coord = district_to_coord.get(d)
			if coord and coord.get("lat") is not None and coord.get("lon") is not None:
				for dt in hist_dates:
					history_tasks.append({
						"district": d,
						"query": r["query"],
						"lat": coord["lat"],
						"lon": coord["lon"],
						"dt": int(dt.timestamp()),
						"date_iso": dt.date().isoformat(),
					})
				forecast_tasks.append({
					"district": d,
					"query": r["query"],
					"lat": coord["lat"],
					"lon": coord["lon"],
					"date_isos": [dd.date().isoformat() for dd in fc_dates],
				})
			else:
				# No coord -> still create placeholder tasks to propagate errors/missing
				for dt in hist_dates:
					history_tasks.append({
						"district": d,
						"query": r["query"],
						"lat": None,
						"lon": None,
						"dt": int(dt.timestamp()),
						"date_iso": dt.date().isoformat(),
					})
				forecast_tasks.append({
					"district": d,
					"query": r["query"],
					"lat": None,
					"lon": None,
					"date_isos": [dd.date().isoformat() for dd in fc_dates],
				})

		# Split tasks among ranks
		def split_tasks(tasks: List[Dict[str, Any]], workers: int) -> List[List[Dict[str, Any]]]:
			chunks: List[List[Dict[str, Any]]] = [[] for _ in range(workers)]
			for i, t in enumerate(tasks):
				chunks[i % workers].append(t)
			return chunks

		chunks_hist = split_tasks(history_tasks, size)
		chunks_fc = split_tasks(forecast_tasks, size)
	else:
		flat_current = None
		district_to_coord = None
		chunks_hist = None
		chunks_fc = None

	# Scatter tasks
	local_hist_tasks: List[Dict[str, Any]] = comm.scatter(chunks_hist, root=0)
	local_fc_tasks: List[Dict[str, Any]] = comm.scatter(chunks_fc, root=0)

	# PHASE 2a: Temporal parallelism for historical (past 7 days)
	local_hist_results: List[Dict[str, Any]] = []
	for t in local_hist_tasks:
		if t["lat"] is None or t["lon"] is None:
			local_hist_results.append({
				"district": t["district"],
				"date_iso": t["date_iso"],
				"processor_rank": rank,
				"temperature_c": None,
				"humidity_pct": None,
				"wind_speed_ms": None,
				"rainfall_mm": None,
				"error": "Missing coordinates from current weather phase",
			})
			continue
		try:
			metrics = fetch_onecall_timemachine(session, api_key, float(t["lat"]), float(t["lon"]), int(t["dt"]))
			local_hist_results.append({
				"district": t["district"],
				"date_iso": t["date_iso"],
				"processor_rank": rank,
				**metrics,
			})
		except Exception as exc:  # noqa: BLE001
			local_hist_results.append({
				"district": t["district"],
				"date_iso": t["date_iso"],
				"processor_rank": rank,
				"temperature_c": None,
				"humidity_pct": None,
				"wind_speed_ms": None,
				"rainfall_mm": None,
				"error": str(exc),
			})

	# PHASE 2b: Forecast (next 7 days). We fetch once per district and expand to daily entries
	local_fc_results: List[Dict[str, Any]] = []
	for t in local_fc_tasks:
		if t["lat"] is None or t["lon"] is None:
			for date_iso in t["date_isos"]:
				local_fc_results.append({
					"district": t["district"],
					"date_iso": date_iso,
					"processor_rank": rank,
					"temperature_c": None,
					"humidity_pct": None,
					"wind_speed_ms": None,
					"rainfall_mm": None,
					"error": "Missing coordinates from current weather phase",
				})
			continue
		try:
			daily_list = fetch_onecall_forecast_daily(session, api_key, float(t["lat"]), float(t["lon"]))
			# Align by available days up to requested 7
			for i, date_iso in enumerate(t["date_isos"]):
				metrics = daily_list[i] if i < len(daily_list) else {"temperature_c": None, "humidity_pct": None, "wind_speed_ms": None, "rainfall_mm": None}
				local_fc_results.append({
					"district": t["district"],
					"date_iso": date_iso,
					"processor_rank": rank,
					**metrics,
				})
		except Exception as exc:  # noqa: BLE001
			for date_iso in t["date_isos"]:
				local_fc_results.append({
					"district": t["district"],
					"date_iso": date_iso,
					"processor_rank": rank,
					"temperature_c": None,
					"humidity_pct": None,
					"wind_speed_ms": None,
					"rainfall_mm": None,
					"error": str(exc),
				})

	gathered_hist: List[List[Dict[str, Any]]] = comm.gather(local_hist_results, root=0)
	gathered_fc: List[List[Dict[str, Any]]] = comm.gather(local_fc_results, root=0)

	if rank == 0:
		ensure_data_dir_exists()
		# Flatten all
		flat_current = flat_current or []
		flat_hist: List[Dict[str, Any]] = []
		for part in gathered_hist:
			flat_hist.extend(part)
		flat_fc: List[Dict[str, Any]] = []
		for part in gathered_fc:
			flat_fc.extend(part)

		# Assemble per-district structure
		district_map: Dict[str, Dict[str, Any]] = {}
		for r in flat_current:
			district_map[r["district"]] = {
				"district": r["district"],
				"query": r["query"],
				"coord": r.get("coord"),
				"current": r["current"],
				"history": [],
				"forecast": [],
			}

		for h in flat_hist:
			entry = district_map.get(h["district"]) or district_map.setdefault(h["district"], {
				"district": h["district"],
				"query": None,
				"coord": None,
				"current": {
					"temperature_c": None,
					"humidity_pct": None,
					"wind_speed_ms": None,
					"rainfall_mm": None,
					"processor_rank": None,
				},
				"history": [],
				"forecast": [],
			})
			entry["history"].append({
				"date": h["date_iso"],
				"temperature_c": h.get("temperature_c"),
				"humidity_pct": h.get("humidity_pct"),
				"wind_speed_ms": h.get("wind_speed_ms"),
				"rainfall_mm": h.get("rainfall_mm"),
				"processor_rank": h.get("processor_rank"),
				"error": h.get("error"),
			})

		for f in flat_fc:
			entry = district_map.get(f["district"]) or district_map.setdefault(f["district"], {
				"district": f["district"],
				"query": None,
				"coord": None,
				"current": {
					"temperature_c": None,
					"humidity_pct": None,
					"wind_speed_ms": None,
					"rainfall_mm": None,
					"processor_rank": None,
				},
				"history": [],
				"forecast": [],
			})
			entry["forecast"].append({
				"date": f["date_iso"],
				"temperature_c": f.get("temperature_c"),
				"humidity_pct": f.get("humidity_pct"),
				"wind_speed_ms": f.get("wind_speed_ms"),
				"rainfall_mm": f.get("rainfall_mm"),
				"processor_rank": f.get("processor_rank"),
				"error": f.get("error"),
			})

		# Sort history/forecast by date
		for v in district_map.values():
			v["history"].sort(key=lambda x: x["date"])
			v["forecast"].sort(key=lambda x: x["date"])

		# Averages helpers
		def avg(values: List[Optional[float]]) -> Optional[float]:
			valid = [v for v in values if isinstance(v, (int, float))]
			return round(sum(valid) / len(valid), 2) if valid else None

		current_list = [v["current"] for v in district_map.values()]
		history_list = [d for v in district_map.values() for d in v["history"]]
		forecast_list = [d for v in district_map.values() for d in v["forecast"]]

		averages = {
			"current": {
				"temperature_c": avg([r.get("temperature_c") for r in current_list]),
				"humidity_pct": avg([r.get("humidity_pct") for r in current_list]),
				"wind_speed_ms": avg([r.get("wind_speed_ms") for r in current_list]),
				"rainfall_mm": avg([r.get("rainfall_mm") for r in current_list]),
			},
			"history": {
				"temperature_c": avg([r.get("temperature_c") for r in history_list]),
				"humidity_pct": avg([r.get("humidity_pct") for r in history_list]),
				"wind_speed_ms": avg([r.get("wind_speed_ms") for r in history_list]),
				"rainfall_mm": avg([r.get("rainfall_mm") for r in history_list]),
			},
			"forecast": {
				"temperature_c": avg([r.get("temperature_c") for r in forecast_list]),
				"humidity_pct": avg([r.get("humidity_pct") for r in forecast_list]),
				"wind_speed_ms": avg([r.get("wind_speed_ms") for r in forecast_list]),
				"rainfall_mm": avg([r.get("rainfall_mm") for r in forecast_list]),
			},
		}

		# Workload (counts per rank) for visualization
		def count_by_rank(items: List[Dict[str, Any]]) -> Dict[str, int]:
			counts: Dict[str, int] = {}
			for it in items:
				r = it.get("processor_rank")
				key = str(r)
				counts[key] = counts.get(key, 0) + 1
			return counts

		workload = {
			"current": count_by_rank([{**v["current"]} for v in district_map.values()]),
			"history": count_by_rank(history_list),
			"forecast": count_by_rank(forecast_list),
		}

		# Dates for convenience in frontend
		meta_history_dates = sorted({h["date"] for h in history_list})
		meta_forecast_dates = sorted({f["date"] for f in forecast_list})

		out_obj = {
			"last_updated": datetime.now(timezone.utc).isoformat(),
			"districts": list(district_map.values()),
			"averages": averages,
			"meta": {
				"total_districts": total,
				"mpi_processes": size,
				"history_dates": meta_history_dates,
				"forecast_dates": meta_forecast_dates,
				"workload": workload,
			},
		}

		with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
			json.dump(out_obj, f, ensure_ascii=False, indent=2)

		print(f"Wrote extended data to {OUTPUT_PATH} (districts: {len(district_map)})")


if __name__ == "__main__":
	main()