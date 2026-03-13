require('dotenv').config();
const express = require('express');
const Database = require('better-sqlite3');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const https = require('https');
const http = require('http');
const sharp = require('sharp');
const crypto = require('crypto');

const app = express();
app.use(express.json({ limit: '15mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Serve uploaded images
const uploadsDir = path.join(__dirname, 'data', 'uploads');
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });
app.use('/uploads', express.static(uploadsDir));

const PORT = process.env.PORT || 3000;
const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY || '';
const ADMIN_PIN = process.env.ADMIN_PIN || '2024';
const DAILY_AVG_NM = parseFloat(process.env.DAILY_AVG_NM) || 100;
const MMSI = '261006055';

// Route waypoints
const ROUTE_WAYPOINTS = [
  { name: 'Ismailia', lat: 30.60, lon: 32.27 },
  { name: 'Port Said', lat: 31.27, lon: 32.30 },
  { name: 'Open Sea', lat: 33.0, lon: 30.0 },
  { name: 'Open Sea', lat: 34.2, lon: 27.5 },
  { name: 'Crete', lat: 35.34, lon: 25.14 },
  { name: 'Open Sea', lat: 35.2, lon: 23.0 },
  { name: 'Open Sea', lat: 36.0, lon: 20.0 },
  { name: 'Malta', lat: 35.90, lon: 14.51 },
  { name: 'Open Sea', lat: 36.8, lon: 15.1 },
  { name: 'Sicily', lat: 37.08, lon: 15.29 },
  { name: 'Open Sea', lat: 37.8, lon: 12.5 },
  { name: 'Sardinia', lat: 39.22, lon: 9.12 },
  { name: 'Open Sea', lat: 39.0, lon: 6.0 },
  { name: 'Formentera', lat: 38.71, lon: 1.44 },
  { name: 'Open Sea', lat: 37.8, lon: -0.5 },
  { name: 'Almería', lat: 36.84, lon: -2.46 },
  { name: 'Benalmádena', lat: 36.60, lon: -4.52 }
];

const TOTAL_DISTANCE_NM = 1925;

// --- SQLite Setup ---
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);

const db = new Database(path.join(dataDir, 'tracker.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    speed REAL,
    course REAL,
    heading REAL,
    timestamp TEXT NOT NULL,
    wind_speed REAL,
    wind_dir REAL,
    wind_gust REAL,
    wave_height REAL,
    wave_period REAL,
    swell_height REAL,
    sea_temp REAL
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS updates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crew_name TEXT NOT NULL DEFAULT 'Crew',
    message TEXT,
    photo_url TEXT,
    timestamp TEXT NOT NULL
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS forecast_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    waypoint_name TEXT NOT NULL,
    waypoint_lat REAL NOT NULL,
    waypoint_lon REAL NOT NULL,
    forecast_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL
  )
`);

const insertUpdate = db.prepare('INSERT INTO updates (crew_name, message, photo_url, timestamp) VALUES (?, ?, ?, ?)');
const getUpdates = db.prepare('SELECT * FROM updates ORDER BY id DESC LIMIT 50');

const insertPosition = db.prepare(`
  INSERT INTO positions (lat, lon, speed, course, heading, timestamp,
    wind_speed, wind_dir, wind_gust, wave_height, wave_period, swell_height, sea_temp)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const getLatest = db.prepare('SELECT * FROM positions ORDER BY id DESC LIMIT 1');
const getHistory = db.prepare('SELECT * FROM positions ORDER BY id ASC');

// --- Weather Fetching ---
function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    mod.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

async function fetchWeather(lat, lon) {
  const weather = { wind_speed: null, wind_dir: null, wind_gust: null, wave_height: null, wave_period: null, swell_height: null, sea_temp: null };
  try {
    const [marine, forecast] = await Promise.all([
      fetchJSON(`https://marine-api.open-meteo.com/v1/marine?latitude=${lat}&longitude=${lon}&current=wave_height,wave_direction,wave_period,swell_wave_height`),
      fetchJSON(`https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,wind_speed_10m,wind_direction_10m,wind_gusts_10m`)
    ]);
    if (marine && marine.current) {
      weather.wave_height = marine.current.wave_height ?? null;
      weather.wave_period = marine.current.wave_period ?? null;
      weather.swell_height = marine.current.swell_wave_height ?? null;
    }
    if (forecast && forecast.current) {
      weather.wind_speed = forecast.current.wind_speed_10m ?? null;
      weather.wind_dir = forecast.current.wind_direction_10m ?? null;
      weather.wind_gust = forecast.current.wind_gusts_10m ?? null;
      weather.sea_temp = forecast.current.temperature_2m ?? null;
    }
  } catch (err) {
    console.error('Weather fetch error:', err.message);
  }
  return weather;
}

// --- Haversine Distance (nm) ---
function haversineNm(lat1, lon1, lat2, lon2) {
  const R = 3440.065;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// --- Get Next Waypoints Along Route ---
function getNextWaypoints(lat, lon) {
  let minDist = Infinity;
  let bestSegIdx = 0;
  for (let i = 0; i < ROUTE_WAYPOINTS.length - 1; i++) {
    const wp1 = ROUTE_WAYPOINTS[i], wp2 = ROUTE_WAYPOINTS[i + 1];
    const segLen = haversineNm(wp1.lat, wp1.lon, wp2.lat, wp2.lon);
    const dFrom = haversineNm(lat, lon, wp1.lat, wp1.lon);
    const dTo = haversineNm(lat, lon, wp2.lat, wp2.lon);
    const proj = (dFrom ** 2 + segLen ** 2 - dTo ** 2) / (2 * segLen);
    const onSeg = proj >= -50 && proj <= segLen + 50;
    const perpDist = Math.sqrt(Math.max(0, dFrom ** 2 - proj ** 2));
    if (onSeg && perpDist < minDist) {
      minDist = perpDist;
      bestSegIdx = i;
    }
  }
  // Return next 3 named waypoints after the current segment
  const result = [];
  for (let i = bestSegIdx + 1; i < ROUTE_WAYPOINTS.length && result.length < 3; i++) {
    result.push(ROUTE_WAYPOINTS[i]);
  }
  // If we have fewer than 3, include the end of current segment
  if (result.length === 0) {
    result.push(ROUTE_WAYPOINTS[ROUTE_WAYPOINTS.length - 1]);
  }
  return result;
}

// --- Fetch 48h Forecast ---
async function fetchForecastData(lat, lon) {
  const forecast = { time: [], wind_speed_10m: [], wind_direction_10m: [], wind_gusts_10m: [], temperature_2m: [], wave_height: [], swell_wave_height: [] };
  try {
    const [marine, atmo] = await Promise.all([
      fetchJSON(`https://marine-api.open-meteo.com/v1/marine?latitude=${lat}&longitude=${lon}&hourly=wave_height,swell_wave_height&forecast_days=2`),
      fetchJSON(`https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,wind_gusts_10m&forecast_days=2`)
    ]);
    if (atmo && atmo.hourly) {
      forecast.time = atmo.hourly.time || [];
      forecast.wind_speed_10m = atmo.hourly.wind_speed_10m || [];
      forecast.wind_direction_10m = atmo.hourly.wind_direction_10m || [];
      forecast.wind_gusts_10m = atmo.hourly.wind_gusts_10m || [];
      forecast.temperature_2m = atmo.hourly.temperature_2m || [];
    }
    if (marine && marine.hourly) {
      forecast.wave_height = marine.hourly.wave_height || [];
      forecast.swell_wave_height = marine.hourly.swell_wave_height || [];
      // Use marine time if atmo time is empty
      if (forecast.time.length === 0) forecast.time = marine.hourly.time || [];
    }
  } catch (err) {
    console.error('Forecast fetch error:', err.message);
  }
  return forecast;
}

let lastForecastFetch = 0;

async function updateForecastCache(lat, lon) {
  // Only refetch every 30 minutes to avoid rate limits
  const now = Date.now();
  if (now - lastForecastFetch < 30 * 60 * 1000) return;
  lastForecastFetch = now;

  try {
    const nextWPs = getNextWaypoints(lat, lon);
    const forecasts = await Promise.all(
      nextWPs.map(async (wp) => ({
        name: wp.name,
        lat: wp.lat,
        lon: wp.lon,
        forecast: await fetchForecastData(wp.lat, wp.lon)
      }))
    );
    db.prepare('DELETE FROM forecast_cache').run();
    const ins = db.prepare('INSERT INTO forecast_cache (waypoint_name, waypoint_lat, waypoint_lon, forecast_json, fetched_at) VALUES (?, ?, ?, ?, ?)');
    const ts = new Date().toISOString();
    for (const f of forecasts) {
      ins.run(f.name, f.lat, f.lon, JSON.stringify(f.forecast), ts);
    }
    console.log(`Forecast cache updated for ${forecasts.map(f => f.name).join(', ')}`);
  } catch (err) {
    console.error('Forecast cache update error:', err.message);
  }
}

// --- Save Position ---
async function savePosition(lat, lon, speed, course, heading, timestamp) {
  const weather = await fetchWeather(lat, lon);
  insertPosition.run(
    lat, lon, speed, course, heading, timestamp,
    weather.wind_speed, weather.wind_dir, weather.wind_gust,
    weather.wave_height, weather.wave_period, weather.swell_height, weather.sea_temp
  );
  console.log(`Position saved: ${lat.toFixed(4)}, ${lon.toFixed(4)} @ ${speed} kn | ${timestamp}`);
  // Update forecast cache in background (non-blocking)
  updateForecastCache(lat, lon).catch(err => console.error('Forecast bg error:', err.message));
}

// --- AIS Stream ---
let aisConnected = false;
let reconnectTimeout = null;

function connectAIS() {
  if (!AISSTREAM_API_KEY) {
    console.warn('No AISSTREAM_API_KEY set — AIS stream disabled. Use POST /api/position for manual entry.');
    return;
  }

  console.log('AIS: Connecting to aisstream.io...');
  const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');

  ws.on('open', () => {
    aisConnected = true;
    console.log('AIS: Connected');
    ws.send(JSON.stringify({
      APIKey: AISSTREAM_API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: [MMSI],
      FilterMessageTypes: ['PositionReport']
    }));
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.MessageType === 'PositionReport') {
        const pos = msg.Message.PositionReport;
        const meta = msg.MetaData;
        const lat = pos.Latitude;
        const lon = pos.Longitude;
        const speed = pos.Sog;
        const course = pos.Cog;
        const heading = pos.TrueHeading;
        const timestamp = meta.time_utc || new Date().toISOString();
        await savePosition(lat, lon, speed, course, heading, timestamp);
      }
    } catch (err) {
      console.error('AIS message parse error:', err.message);
    }
  });

  ws.on('close', () => {
    aisConnected = false;
    console.log('AIS: Disconnected. Reconnecting in 10s...');
    reconnectTimeout = setTimeout(connectAIS, 10000);
  });

  ws.on('error', (err) => {
    console.error('AIS: WebSocket error:', err.message);
    ws.close();
  });
}

// --- API Routes ---
app.get('/api/latest', (req, res) => {
  const row = getLatest.get();
  res.json(row || null);
});

app.get('/api/history', (req, res) => {
  const rows = getHistory.all();
  res.json(rows);
});

app.get('/api/config', (req, res) => {
  res.json({
    mmsi: MMSI,
    vesselName: 'BLUE MARINE',
    departure: { name: 'Ismailia, Egypt', lat: 30.60, lon: 32.27 },
    destination: { name: 'Benalmádena, Spain', lat: 36.60, lon: -4.52 },
    totalDistanceNm: TOTAL_DISTANCE_NM,
    dailyAvgNm: DAILY_AVG_NM,
    waypoints: ROUTE_WAYPOINTS
  });
});

app.post('/api/verify-pin', (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.body.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  res.json({ success: true });
});

app.post('/api/position', async (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.body.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const { lat, lon, speed, course, heading } = req.body;
  if (lat == null || lon == null) {
    return res.status(400).json({ error: 'lat and lon are required' });
  }
  const timestamp = req.body.timestamp || new Date().toISOString();
  try {
    await savePosition(lat, lon, speed || 0, course || 0, heading || 0, timestamp);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/position/:id', (req, res) => {
  const pin = req.headers['x-admin-pin'];
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid id' });
  const result = db.prepare('DELETE FROM positions WHERE id = ?').run(id);
  if (result.changes === 0) return res.status(404).json({ error: 'Position not found' });
  console.log(`Position ${id} deleted by admin`);
  res.json({ success: true, deleted: id });
});

// --- Updates API ---
app.get('/api/updates', (req, res) => {
  const rows = getUpdates.all();
  res.json(rows);
});

app.post('/api/updates', async (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.body.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const { crew_name, message, photo } = req.body;
  if (!message && !photo) {
    return res.status(400).json({ error: 'Message or photo required' });
  }

  let photoUrl = null;
  if (photo) {
    try {
      // photo is a base64 data URI
      const matches = photo.match(/^data:image\/\w+;base64,(.+)$/);
      if (!matches) {
        return res.status(400).json({ error: 'Invalid photo format' });
      }
      const buf = Buffer.from(matches[1], 'base64');
      const filename = crypto.randomBytes(8).toString('hex') + '.jpg';
      const outPath = path.join(uploadsDir, filename);
      await sharp(buf)
        .resize(800, 800, { fit: 'inside', withoutEnlargement: true })
        .jpeg({ quality: 80 })
        .toFile(outPath);
      photoUrl = '/uploads/' + filename;
    } catch (err) {
      console.error('Photo processing error:', err.message);
      return res.status(500).json({ error: 'Failed to process photo' });
    }
  }

  const timestamp = new Date().toISOString();
  const name = (crew_name || 'Crew').trim().slice(0, 30);
  insertUpdate.run(name, message || null, photoUrl, timestamp);
  console.log(`Update posted by ${name}: ${message || '(photo)'}`);
  res.json({ success: true });
});

// --- Forecast API ---
app.get('/api/forecast', (req, res) => {
  const rows = db.prepare('SELECT * FROM forecast_cache ORDER BY id ASC').all();
  if (rows.length === 0) return res.json({ summary: [], fetched_at: null });

  const waypoints = rows.map(r => ({
    name: r.waypoint_name,
    lat: r.waypoint_lat,
    lon: r.waypoint_lon,
    forecast: JSON.parse(r.forecast_json)
  }));

  const now = new Date();
  const blocks = [
    { label: 'Next 12h', start: 0, end: 12 },
    { label: '12–24h', start: 12, end: 24 },
    { label: '24–48h', start: 24, end: 48 }
  ];

  const summary = blocks.map(block => {
    let maxWind = 0, maxGust = 0, maxWave = 0;
    let tempSum = 0, tempCount = 0;
    const windDirSin = [], windDirCos = [];

    for (const wp of waypoints) {
      const h = wp.forecast;
      if (!h || !h.time) continue;
      for (let i = 0; i < h.time.length; i++) {
        const hoursAhead = (new Date(h.time[i]) - now) / 3600000;
        if (hoursAhead >= block.start && hoursAhead < block.end) {
          if (h.wind_speed_10m[i] != null && h.wind_speed_10m[i] > maxWind) maxWind = h.wind_speed_10m[i];
          if (h.wind_gusts_10m[i] != null && h.wind_gusts_10m[i] > maxGust) maxGust = h.wind_gusts_10m[i];
          if (h.wave_height[i] != null && h.wave_height[i] > maxWave) maxWave = h.wave_height[i];
          if (h.temperature_2m[i] != null) { tempSum += h.temperature_2m[i]; tempCount++; }
          if (h.wind_direction_10m[i] != null) {
            const rad = h.wind_direction_10m[i] * Math.PI / 180;
            windDirSin.push(Math.sin(rad));
            windDirCos.push(Math.cos(rad));
          }
        }
      }
    }

    // Circular mean for wind direction
    let avgWindDir = null;
    if (windDirSin.length > 0) {
      const meanSin = windDirSin.reduce((a, b) => a + b, 0) / windDirSin.length;
      const meanCos = windDirCos.reduce((a, b) => a + b, 0) / windDirCos.length;
      avgWindDir = Math.round(((Math.atan2(meanSin, meanCos) * 180 / Math.PI) + 360) % 360);
    }

    // Severity: green / orange / red
    let severity = 'calm';
    if (maxWind > 40 || maxWave > 3) severity = 'rough';
    else if (maxWind > 20 || maxWave > 1.5) severity = 'moderate';

    return {
      label: block.label,
      max_wind: Math.round(maxWind),
      max_gust: Math.round(maxGust),
      max_wave: Math.round(maxWave * 10) / 10,
      avg_temp: tempCount > 0 ? Math.round(tempSum / tempCount * 10) / 10 : null,
      wind_dir: avgWindDir,
      severity
    };
  });

  res.json({ summary, fetched_at: rows[0].fetched_at });
});

// --- Start ---
app.listen(PORT, () => {
  console.log(`Blue Marine Tracker running on port ${PORT}`);
  connectAIS();
});
