### Parallel Weather Simulation for Tamil Nadu (MPI + Flask)

This project fetches real-time weather data for all 38 districts of Tamil Nadu using the OpenWeather API, in parallel with MPI (5 processes). A Flask frontend shows a grid of district weather, which MPI rank processed each district, interactive charts (temperature, humidity, rainfall, windspeed), and the overall averages. Data refresh runs every 10 minutes and can be triggered manually.

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
- Rainfall uses OpenWeather current "rain" field if present (1h or 3h), otherwise 0.
- Data is written to `data/weather.json` with a timestamp and per-district `processor_rank`.
- The frontend auto-refreshes every 10 minutes and also provides a manual Refresh button.

### MPI Emphasis
The heavy lifting (API calls + parsing) is executed in parallel by 5 MPI ranks using `mpi4py`. The root process aggregates results and computes state-wide averages before persisting.