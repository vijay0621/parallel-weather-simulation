### Parallel Weather Simulation for Tamil Nadu (MPI + Flask)

This project fetches weather for all 38 districts of Tamil Nadu using the OpenWeather API, in parallel with MPI (default 5 processes). It now demonstrates both spatial and temporal parallelism:

- Spatial: districts are split across MPI ranks
- Temporal: for each district, past 7 days (historical) and next 7 days (forecast) are fetched in parallel

The Flask frontend shows:
- Per-district current metrics (with the MPI rank that processed it)
- History and forecast trends with a timeframe selector
- State-wide averages for current/history/forecast
- MPI workload distribution chart by rank (current, history, forecast)

### Prerequisites
- Linux with system MPI (MPICH or OpenMPI)
- Python 3.9+
- OpenWeather API key

### Setup
1. Install system MPI first (example with MPICH):
```bash
sudo apt-get update && sudo apt-get install -y mpich
```
2. Install Python dependencies:
```bash
pip install -r requirements.txt
```
3. Provide your OpenWeather API key via environment variable:
```bash
export OPENWEATHER_API_KEY="YOUR_KEY_HERE"
```

Notes on API access:
- Current weather uses `api.openweathermap.org/data/2.5/weather`.
- Historical and forecast use One Call 3.0: `api.openweathermap.org/data/3.0/onecall` and `.../timemachine`.
- Depending on your OpenWeather plan, historical coverage and quotas may vary. If some days are unavailable, they will appear as missing with an error message in the JSON and UI. You can reduce the history/forecast window in `mpi_fetch.py` if needed.

### Development
- Run an initial parallel fetch (5 processes):
```bash
cd parallel_weather
mpiexec -n 5 python mpi_fetch.py
```
- Start the Flask server:
```bash
export FLASK_APP=app.py
python app.py
```
The app will be available on http://localhost:5000

- Or with Gunicorn:
```bash
gunicorn -w 2 -b 0.0.0.0:5000 app:app
```

### Notes
- Districts are assigned to ranks (0-4) in near-equal chunks (7 or 8 per rank).
- Temporal tasks (history + forecast) are distributed round-robin across all ranks for balanced workload.
- Rainfall uses OpenWeather's fields: for current weather 1h/3h; for forecast daily `rain` field; for history, hourly rainfall is summed per day.
- Data is written to `data/weather.json` with a timestamp and workload breakdown.
- The frontend auto-refreshes every 10 minutes and also provides a manual Refresh button.

### Output Schema
`data/weather.json` has the following top-level fields:
- `last_updated`: ISO timestamp (UTC)
- `districts`: array of district objects
  - `district`, `query`, `coord`
  - `current`: { temperature_c, humidity_pct, wind_speed_ms, rainfall_mm, processor_rank, error? }
  - `history`: [ { date, temperature_c, humidity_pct, wind_speed_ms, rainfall_mm, processor_rank, error? }, ... ]
  - `forecast`: [ same shape as history ]
- `averages`: { current: {...}, history: {...}, forecast: {...} }
- `meta`: { total_districts, mpi_processes, history_dates[], forecast_dates[], workload: { current, history, forecast } }

All averages ignore missing values.

### MPI Emphasis
Two-dimensional parallelism is showcased:
- Spatial: ranks handle disjoint subsets of districts for current data (and to get coordinates)
- Temporal: the root composes day-level tasks which are then scattered among all ranks for history/forecast, and gathered for aggregation

This reduces total runtime compared to sequentially fetching 38×(1 current + 7 history + 7 forecast) requests.