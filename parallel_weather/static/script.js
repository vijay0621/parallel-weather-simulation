let chartInstance = null;

async function fetchData() {
	const res = await fetch('/api/data');
	if (!res.ok) {
		throw new Error('Failed to load data');
	}
	return res.json();
}

function renderAverages(averages) {
	document.getElementById('avgTemp').textContent = averages.temperature_c ?? '—';
	document.getElementById('avgHumidity').textContent = averages.humidity_pct ?? '—';
	document.getElementById('avgRain').textContent = averages.rainfall_mm ?? '—';
	document.getElementById('avgWind').textContent = averages.wind_speed_ms ?? '—';
}

function renderGrid(districts) {
	const grid = document.getElementById('grid');
	grid.innerHTML = '';
	districts.forEach(d => {
		const card = document.createElement('div');
		card.className = 'card';
		card.innerHTML = `
			<h3>${d.district}</h3>
			<div class="meta">Processed by MPI Rank ${d.processor_rank}</div>
			<div class="row"><span>Temp</span><strong>${d.temperature_c ?? '—'} °C</strong></div>
			<div class="row"><span>Humidity</span><strong>${d.humidity_pct ?? '—'} %</strong></div>
			<div class="row"><span>Rainfall</span><strong>${d.rainfall_mm ?? '—'} mm</strong></div>
			<div class="row"><span>Wind</span><strong>${d.wind_speed_ms ?? '—'} m/s</strong></div>
		`;
		grid.appendChild(card);
	});
}

function updateLastUpdated(ts) {
	const el = document.getElementById('lastUpdated');
	el.textContent = `Last updated: ${new Date(ts).toLocaleString()}`;
}

function renderChart(districts, kind) {
	const labels = districts.map(d => d.district);
	let data = [];
	let label = '';
	switch (kind) {
		case 'temp':
			label = 'Temperature (°C)';
			data = districts.map(d => d.temperature_c ?? null);
			break;
		case 'humidity':
			label = 'Humidity (%)';
			data = districts.map(d => d.humidity_pct ?? null);
			break;
		case 'rain':
			label = 'Rainfall (mm)';
			data = districts.map(d => d.rainfall_mm ?? null);
			break;
		case 'wind':
			label = 'Wind Speed (m/s)';
			data = districts.map(d => d.wind_speed_ms ?? null);
			break;
		default:
			return;
	}

	const ctx = document.getElementById('chartCanvas').getContext('2d');
	if (chartInstance) {
		chartInstance.destroy();
	}
	chartInstance = new Chart(ctx, {
		type: 'bar',
		data: {
			labels,
			datasets: [{
				label,
				data,
				backgroundColor: 'rgba(30, 136, 229, 0.6)'
			}]
		},
		options: {
			responsive: true,
			maintainAspectRatio: false,
			scales: {
				y: { beginAtZero: true }
			}
		}
	});
}

async function loadAndRender() {
	try {
		const data = await fetchData();
		renderAverages(data.averages || {});
		renderGrid(data.districts || []);
		updateLastUpdated(data.last_updated || Date.now());
		// default chart
		renderChart(data.districts || [], 'temp');
	} catch (e) {
		console.error(e);
	}
}

async function refreshNow() {
	const btn = document.getElementById('refreshBtn');
	btn.disabled = true;
	try {
		const res = await fetch('/api/refresh', { method: 'POST' });
		const json = await res.json();
		if (json && json.data) {
			renderAverages(json.data.averages || {});
			renderGrid(json.data.districts || []);
			updateLastUpdated(json.data.last_updated || Date.now());
		}
	} catch (e) {
		console.error(e);
	} finally {
		btn.disabled = false;
	}
}

function setupEvents() {
	document.getElementById('refreshBtn').addEventListener('click', refreshNow);
	document.querySelectorAll('.chart-buttons button').forEach(btn => {
		btn.addEventListener('click', () => {
			const kind = btn.getAttribute('data-chart');
			fetchData().then(data => renderChart(data.districts || [], kind));
		});
	});
	// Auto-refresh every 10 minutes
	setInterval(() => {
		refreshNow();
	}, 10 * 60 * 1000);
}

window.addEventListener('DOMContentLoaded', () => {
	setupEvents();
	loadAndRender();
});