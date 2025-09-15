import json
import os
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_PATH = DATA_DIR / "weather.json"
STATIC_DIR = BASE_DIR / "static"

REFRESH_INTERVAL_MINUTES = 10
MPI_PROCESSES = 5

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="/")

_refresh_lock = threading.Lock()


def is_data_stale() -> bool:
	if not OUTPUT_PATH.exists():
		return True
	try:
		with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
			data = json.load(f)
		last = data.get("last_updated")
		if not last:
			return True
		last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
		return datetime.now(timezone.utc) - last_dt > timedelta(minutes=REFRESH_INTERVAL_MINUTES)
	except Exception:
		return True


def run_mpi_fetch() -> subprocess.CompletedProcess:
	env = os.environ.copy()
	if not env.get("OPENWEATHER_API_KEY"):
		raise RuntimeError("OPENWEATHER_API_KEY is not set in environment")

	cmd = [
		"mpiexec",
		"-n",
		str(MPI_PROCESSES),
		sys.executable,
		str(BASE_DIR / "mpi_fetch.py"),
	]
	return subprocess.run(cmd, cwd=str(BASE_DIR), check=True, capture_output=True, text=True)


@app.route("/")
def index():
	return send_from_directory(str(STATIC_DIR), "index.html")


@app.route("/api/data", methods=["GET"])
def api_data():
	# Optionally trigger refresh if stale (non-blocking)
	if is_data_stale():
		def _bg_refresh():
			with _refresh_lock:
				try:
					run_mpi_fetch()
				except Exception:
					pass
		threading.Thread(target=_bg_refresh, daemon=True).start()

	if not OUTPUT_PATH.exists():
		return jsonify({"error": "Data not available yet. Please click Refresh after setting OPENWEATHER_API_KEY."}), 503

	with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
		data = json.load(f)
	return jsonify(data)


@app.route("/api/refresh", methods=["POST"])

def api_refresh():
	with _refresh_lock:
		try:
			result = run_mpi_fetch()
			# After refresh, return new data
			with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
				data = json.load(f)
			return jsonify({"ok": True, "data": data, "stdout": result.stdout})
		except subprocess.CalledProcessError as cpe:
			return jsonify({"ok": False, "error": cpe.stderr or str(cpe)}), 500
		except Exception as exc:  # noqa: BLE001
			return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
	port = int(os.environ.get("PORT", 5000))
	app.run(host="0.0.0.0", port=port, debug=True)