let chartInstance = null;
let workloadChart = null;
let cachedData = null;

async function fetchData() {
	const res = await fetch('/api/data');
	if (!res.ok) {
		throw new Error('Failed to load data');
	}
	return res.json();
}

function renderAverages(averages) {
    const tf = document.getElementById('timeframe').value;
    const src = (averages && averages[tf]) ? averages[tf] : {};
    document.getElementById('avgTemp').textContent = src.temperature_c ?? '—';
    document.getElementById('avgHumidity').textContent = src.humidity_pct ?? '—';
    document.getElementById('avgRain').textContent = src.rainfall_mm ?? '—';
    document.getElementById('avgWind').textContent = src.wind_speed_ms ?? '—';
}

function renderGrid(districts) {
	const grid = document.getElementById('grid');
	grid.innerHTML = '';
	const tf = document.getElementById('timeframe').value;
	const isCurrent = tf === 'current';
	const dateLookup = (dataArr) => {
		const dates = (cachedData?.meta?.[tf + '_dates']) || [];
		if (!dates.length) return null;
		return dataArr?.find(x => x.date === dates[0]) || null; // first day as example
	};

	districts.forEach(d => {
		const card = document.createElement('div');
		card.className = 'card';
		let metrics = null;
		let proc = null;
		if (isCurrent) {
			metrics = d.current || {};
			proc = d.current?.processor_rank ?? d.processor_rank;
		} else if (tf === 'history') {
			const h = dateLookup(d.history);
			metrics = h || {};
			proc = h?.processor_rank ?? d.current?.processor_rank;
		} else {
			const f = dateLookup(d.forecast);
			metrics = f || {};
			proc = f?.processor_rank ?? d.current?.processor_rank;
		}
		card.innerHTML = `
			<h3>${d.district}</h3>
			<div class="meta">Processed by MPI Rank ${proc ?? '—'}</div>
			<div class="row"><span>Temp</span><strong>${metrics.temperature_c ?? '—'} °C</strong></div>
			<div class="row"><span>Humidity</span><strong>${metrics.humidity_pct ?? '—'} %</strong></div>
			<div class="row"><span>Rainfall</span><strong>${metrics.rainfall_mm ?? '—'} mm</strong></div>
			<div class="row"><span>Wind</span><strong>${metrics.wind_speed_ms ?? '—'} m/s</strong></div>
		`;
		grid.appendChild(card);
	});
}

function updateLastUpdated(ts) {
	const el = document.getElementById('lastUpdated');
	el.textContent = `Last updated: ${new Date(ts).toLocaleString()}`;
}

function renderChart(districts, kind) {
    const tf = document.getElementById('timeframe').value;
    const isCurrent = tf === 'current';
    const timeAxis = (tf === 'current') ? null : (cachedData?.meta?.[tf + '_dates'] || []);

    let labels = [];
    let dsLabel = '';
    let datasets = [];

    if (isCurrent) {
        labels = districts.map(d => d.district);
        const values = districts.map(d => {
            const m = d.current || {};
            switch (kind) {
                case 'temp': return m.temperature_c ?? null;
                case 'humidity': return m.humidity_pct ?? null;
                case 'rain': return m.rainfall_mm ?? null;
                case 'wind': return m.wind_speed_ms ?? null;
                default: return null;
            }
        });
        dsLabel = kind === 'temp' ? 'Temperature (°C)'
            : kind === 'humidity' ? 'Humidity (%)'
            : kind === 'rain' ? 'Rainfall (mm)'
            : 'Wind Speed (m/s)';
        datasets = [{ label: dsLabel, data: values, backgroundColor: 'rgba(30, 136, 229, 0.6)' }];
    } else {
        // temporal chart: average per date across districts
        labels = timeAxis;
        const values = timeAxis.map(dateIso => {
            const vals = districts.map(d => {
                const arr = d[tf] || [];
                const rec = arr.find(x => x.date === dateIso) || {};
                switch (kind) {
                    case 'temp': return rec.temperature_c ?? null;
                    case 'humidity': return rec.humidity_pct ?? null;
                    case 'rain': return rec.rainfall_mm ?? null;
                    case 'wind': return rec.wind_speed_ms ?? null;
                    default: return null;
                }
            }).filter(v => v !== null && v !== undefined);
            if (!vals.length) return null;
            const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
            return Math.round(avg * 100) / 100;
        });
        dsLabel = (tf === 'history' ? 'Past 7 Days' : 'Next 7 Days') + ' ' + (
            kind === 'temp' ? 'Temperature (°C)'
            : kind === 'humidity' ? 'Humidity (%)'
            : kind === 'rain' ? 'Rainfall (mm)'
            : 'Wind Speed (m/s)'
        );
        datasets = [{ label: dsLabel, data: values, backgroundColor: 'rgba(30, 136, 229, 0.6)' }];
    }

    const ctx = document.getElementById('chartCanvas').getContext('2d');
    if (chartInstance) chartInstance.destroy();
    chartInstance = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } } }
    });
}

function renderWorkload(meta) {
    const wl = meta?.workload || {};
    const ranks = Array.from(new Set([...(Object.keys(wl.current || {})), ...(Object.keys(wl.history || {})), ...(Object.keys(wl.forecast || {}))])).sort((a,b)=>Number(a)-Number(b));
    const datasets = [
        { label: 'Current', data: ranks.map(r => Number(wl.current?.[r] || 0)), backgroundColor: 'rgba(67, 160, 71, 0.7)' },
        { label: 'History', data: ranks.map(r => Number(wl.history?.[r] || 0)), backgroundColor: 'rgba(30, 136, 229, 0.7)' },
        { label: 'Forecast', data: ranks.map(r => Number(wl.forecast?.[r] || 0)), backgroundColor: 'rgba(239, 108, 0, 0.7)' },
    ];
    const ctx = document.getElementById('workloadCanvas').getContext('2d');
    if (workloadChart) workloadChart.destroy();
    workloadChart = new Chart(ctx, { type: 'bar', data: { labels: ranks.map(r => 'Rank ' + r), datasets }, options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } } } });
}

async function loadAndRender() {
	try {
		const data = await fetchData();
		cachedData = data;
		renderAverages(data.averages || {});
		renderGrid(data.districts || []);
		updateLastUpdated(data.last_updated || Date.now());
		renderWorkload(data.meta || {});
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
			cachedData = json.data;
			renderAverages(json.data.averages || {});
			renderGrid(json.data.districts || []);
			updateLastUpdated(json.data.last_updated || Date.now());
			renderWorkload(json.data.meta || {});
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
			if (cachedData) {
				renderChart(cachedData.districts || [], kind);
			} else {
				fetchData().then(data => { cachedData = data; renderChart(data.districts || [], kind); });
			}
		});
	});
	document.getElementById('timeframe').addEventListener('change', () => {
		if (!cachedData) return;
		renderAverages(cachedData.averages || {});
		renderGrid(cachedData.districts || []);
		renderChart(cachedData.districts || [], 'temp');
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