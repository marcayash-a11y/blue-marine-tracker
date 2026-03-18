require('dotenv').config();
const express = require('express');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const Database = require('better-sqlite3');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const https = require('https');
const http = require('http');
const sharp = require('sharp');
const crypto = require('crypto');
const multer = require('multer');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegStatic = require('ffmpeg-static');
const webPush = require('web-push');

ffmpeg.setFfmpegPath(ffmpegStatic);

const app = express();
app.set('trust proxy', 1);
app.use(compression());
app.use(express.json({ limit: '15mb' }));
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'public, max-age=300');
    }
  }
}));

// --- Rate Limiting ---
const readLimiter = rateLimit({ windowMs: 60000, max: 60, standardHeaders: true, legacyHeaders: false });
const adminLimiter = rateLimit({ windowMs: 60000, max: 10, standardHeaders: true, legacyHeaders: false, message: { error: 'Too many requests, try again later' } });
const reactLimiter = rateLimit({ windowMs: 60000, max: 30, standardHeaders: true, legacyHeaders: false, message: { error: 'Too many reactions, slow down' } });
const commentLimiter = rateLimit({ windowMs: 60000, max: 10, standardHeaders: true, legacyHeaders: false, message: { error: 'Too many comments, slow down' } });

// Data directory: use DATA_DIR env for persistent volumes (e.g. Railway), otherwise local ./data
const dataDir = process.env.DATA_DIR || process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

// Serve uploaded images and videos
const uploadsDir = path.join(dataDir, 'uploads');
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });

// Serve uploads with Accept-Ranges for video streaming
app.use('/uploads', (req, res, next) => {
  const ext = path.extname(req.path).toLowerCase();
  const mimeTypes = { '.mp4': 'video/mp4', '.mov': 'video/quicktime', '.webm': 'video/webm' };
  if (mimeTypes[ext]) {
    const filePath = path.join(uploadsDir, req.path);
    // Prevent path traversal
    if (!filePath.startsWith(uploadsDir)) return res.status(403).end();
    if (!fs.existsSync(filePath)) return res.status(404).end();
    const stat = fs.statSync(filePath);
    const fileSize = stat.size;
    const range = req.headers.range;
    if (range) {
      const parts = range.replace(/bytes=/, '').split('-');
      const start = parseInt(parts[0], 10);
      const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
      const chunkSize = end - start + 1;
      res.writeHead(206, {
        'Content-Range': `bytes ${start}-${end}/${fileSize}`,
        'Accept-Ranges': 'bytes',
        'Content-Length': chunkSize,
        'Content-Type': mimeTypes[ext],
        'Cache-Control': 'public, max-age=86400',
      });
      fs.createReadStream(filePath, { start, end }).pipe(res);
    } else {
      res.writeHead(200, {
        'Content-Length': fileSize,
        'Content-Type': mimeTypes[ext],
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'public, max-age=86400',
      });
      fs.createReadStream(filePath).pipe(res);
    }
  } else {
    next();
  }
});
app.use('/uploads', express.static(uploadsDir, {
  setHeaders: (res, filePath) => {
    res.setHeader('Cache-Control', 'public, max-age=86400');
  }
}));

// Multer config for file uploads (50MB per file)
const uploadsTmpDir = path.join(uploadsDir, 'tmp');
if (!fs.existsSync(uploadsTmpDir)) fs.mkdirSync(uploadsTmpDir, { recursive: true });
const upload = multer({
  dest: uploadsTmpDir,
  limits: { fileSize: 50 * 1024 * 1024, files: 5 },
  fileFilter: (req, file, cb) => {
    const allowed = /^(image\/(jpeg|png|gif|webp|heic|heif)|video\/(mp4|quicktime|webm|x-m4v))$/;
    cb(null, allowed.test(file.mimetype));
  }
});

const PORT = process.env.PORT || 3000;
const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY || '';
const ADMIN_PIN = process.env.ADMIN_PIN || '2024';
const DAILY_AVG_NM = parseFloat(process.env.DAILY_AVG_NM) || 100;
const MMSI = '261006055';

// Route waypoints
const ROUTE_WAYPOINTS = [
  { name: 'Ismailia', lat: 30.60, lon: 32.27 },
  { name: 'Port Said', lat: 31.27, lon: 32.30 },
  { name: 'Open Sea', lat: 32.5, lon: 32.5 },
  { name: 'Limassol', lat: 34.67, lon: 33.04 },
  { name: 'Open Sea', lat: 34.5, lon: 30.0 },
  { name: 'Open Sea', lat: 34.8, lon: 27.5 },
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

// --- Voyage Phases (for server-side auto-detection) ---
const VOYAGE_PHASES = [
  { name: 'Phase 3: Port Said → Limassol',      nm: 230, from: {lat:31.27, lon:32.30}, to: {lat:34.67, lon:33.04} },
  { name: 'Phase 4: Limassol → Crete',           nm: 480, from: {lat:34.67, lon:33.04}, to: {lat:35.34, lon:25.14} },
  { name: 'Phase 5: Crete → Malta',              nm: 495, from: {lat:35.34, lon:25.14}, to: {lat:35.90, lon:14.51} },
  { name: 'Phase 6: Malta → Sicily',             nm: 130, from: {lat:35.90, lon:14.51}, to: {lat:37.08, lon:15.29} },
  { name: 'Phase 7: Sicily → Sardinia',          nm: 190, from: {lat:37.08, lon:15.29}, to: {lat:39.22, lon: 9.12} },
  { name: 'Phase 8: Sardinia → Formentera',      nm: 345, from: {lat:39.22, lon: 9.12}, to: {lat:38.71, lon: 1.44} },
  { name: 'Phase 9: Formentera → Almería',       nm: 215, from: {lat:38.71, lon: 1.44}, to: {lat:36.84, lon:-2.46} },
  { name: 'Phase 10: Almería → Benalmádena',     nm: 110, from: {lat:36.84, lon:-2.46}, to: {lat:36.60, lon:-4.52} },
];

// Calculate current voyage phase from boat position
function calculateCurrentPhase(lat, lon) {
  // Check if boat is within 20nm of any waypoint — if so, use the phase departing from that waypoint
  for (let i = 0; i < VOYAGE_PHASES.length; i++) {
    const p = VOYAGE_PHASES[i];
    const distToEnd = haversineNm(lat, lon, p.to.lat, p.to.lon);
    if (distToEnd <= 20 && i < VOYAGE_PHASES.length - 1) {
      // Near the end of this leg = at the start of the next leg
      return { index: i + 1, phase: VOYAGE_PHASES[i + 1] };
    }
  }
  // Check if near the start of the first phase
  const distToFirstStart = haversineNm(lat, lon, VOYAGE_PHASES[0].from.lat, VOYAGE_PHASES[0].from.lon);
  if (distToFirstStart <= 20) {
    return { index: 0, phase: VOYAGE_PHASES[0] };
  }

  // For each leg, find the one where distance_to_start + distance_to_end is closest to the actual leg distance
  let bestIdx = 0;
  let bestDiff = Infinity;
  for (let i = 0; i < VOYAGE_PHASES.length; i++) {
    const p = VOYAGE_PHASES[i];
    const dFrom = haversineNm(lat, lon, p.from.lat, p.from.lon);
    const dTo = haversineNm(lat, lon, p.to.lat, p.to.lon);
    const legDist = haversineNm(p.from.lat, p.from.lon, p.to.lat, p.to.lon);
    const diff = Math.abs((dFrom + dTo) - legDist);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestIdx = i;
    }
  }
  return { index: bestIdx, phase: VOYAGE_PHASES[bestIdx] };
}

// Store current phase in config table
function updateStoredPhase(lat, lon) {
  const result = calculateCurrentPhase(lat, lon);
  const phaseData = JSON.stringify({ index: result.index, name: result.phase.name, nm: result.phase.nm });
  db.prepare("INSERT OR REPLACE INTO config (key, value) VALUES ('current_phase', ?)").run(phaseData);
  return result;
}

// --- SQLite Setup ---

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
    timestamp TEXT NOT NULL,
    estimated_lat REAL,
    estimated_lon REAL
  )
`);

// Add columns if they don't exist (migration for existing databases)
try { db.exec('ALTER TABLE updates ADD COLUMN estimated_lat REAL'); } catch (e) { /* already exists */ }
try { db.exec('ALTER TABLE updates ADD COLUMN estimated_lon REAL'); } catch (e) { /* already exists */ }
try { db.exec('ALTER TABLE updates ADD COLUMN wind_speed REAL'); } catch (e) { /* already exists */ }
try { db.exec('ALTER TABLE updates ADD COLUMN wave_height REAL'); } catch (e) { /* already exists */ }
try { db.exec('ALTER TABLE updates ADD COLUMN sea_temp REAL'); } catch (e) { /* already exists */ }
try { db.exec('ALTER TABLE updates ADD COLUMN pinned INTEGER DEFAULT 0'); } catch (e) { /* already exists */ }
try { db.exec('ALTER TABLE updates ADD COLUMN media_url TEXT'); } catch (e) { /* already exists */ }
try { db.exec('ALTER TABLE positions ADD COLUMN calculated_speed REAL'); } catch (e) { /* already exists */ }

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

db.exec(`
  CREATE TABLE IF NOT EXISTS milestones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    timestamp TEXT NOT NULL
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS reactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    update_id INTEGER NOT NULL,
    emoji TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(update_id, emoji)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    update_id INTEGER NOT NULL,
    name TEXT NOT NULL DEFAULT 'Anonymous',
    message TEXT NOT NULL,
    timestamp TEXT NOT NULL
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS digests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    day_number INTEGER NOT NULL,
    date TEXT NOT NULL UNIQUE,
    distance_nm REAL,
    avg_speed REAL,
    max_speed REAL,
    positions_count INTEGER,
    updates_count INTEGER,
    weather_summary TEXT,
    timestamp TEXT NOT NULL
  )
`);

// --- Push Notification Tables ---
db.exec(`
  CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT NOT NULL UNIQUE,
    keys_p256dh TEXT NOT NULL,
    keys_auth TEXT NOT NULL,
    timestamp TEXT NOT NULL
  )
`);

// --- VAPID Key Setup ---
function getOrCreateVapidKeys() {
  const pubRow = db.prepare("SELECT value FROM config WHERE key = 'vapid_public_key'").get();
  const privRow = db.prepare("SELECT value FROM config WHERE key = 'vapid_private_key'").get();

  if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
    return { publicKey: process.env.VAPID_PUBLIC_KEY, privateKey: process.env.VAPID_PRIVATE_KEY };
  }

  if (pubRow && privRow) {
    return { publicKey: pubRow.value, privateKey: privRow.value };
  }

  // Generate new keys
  const vapidKeys = webPush.generateVAPIDKeys();
  db.prepare("INSERT OR REPLACE INTO config (key, value) VALUES ('vapid_public_key', ?)").run(vapidKeys.publicKey);
  db.prepare("INSERT OR REPLACE INTO config (key, value) VALUES ('vapid_private_key', ?)").run(vapidKeys.privateKey);
  console.log('=== VAPID KEYS GENERATED ===');
  console.log('VAPID_PUBLIC_KEY=' + vapidKeys.publicKey);
  console.log('VAPID_PRIVATE_KEY=' + vapidKeys.privateKey);
  console.log('Save these as environment variables for persistence across deploys.');
  console.log('============================');
  return vapidKeys;
}

const vapidKeys = getOrCreateVapidKeys();
webPush.setVapidDetails('mailto:blue-marine@tracker.local', vapidKeys.publicKey, vapidKeys.privateKey);

// --- Push Notification Helpers ---
let lastNotificationTime = 0;
const NOTIFICATION_COOLDOWN_MS = 60 * 60 * 1000; // 1 hour

function canSendNotification() {
  const now = Date.now();
  if (now - lastNotificationTime < NOTIFICATION_COOLDOWN_MS) return false;
  lastNotificationTime = now;
  return true;
}

async function sendPushToAll(payload) {
  const subs = db.prepare('SELECT * FROM subscriptions').all();
  if (subs.length === 0) return;
  const payloadStr = JSON.stringify(payload);
  const staleIds = [];
  for (const sub of subs) {
    const pushSub = {
      endpoint: sub.endpoint,
      keys: { p256dh: sub.keys_p256dh, auth: sub.keys_auth }
    };
    try {
      await webPush.sendNotification(pushSub, payloadStr);
    } catch (err) {
      if (err.statusCode === 404 || err.statusCode === 410) {
        staleIds.push(sub.id);
      }
      console.error('Push send error:', err.statusCode || err.message);
    }
  }
  // Clean up expired subscriptions
  if (staleIds.length > 0) {
    const del = db.prepare('DELETE FROM subscriptions WHERE id = ?');
    for (const id of staleIds) del.run(id);
    console.log(`Removed ${staleIds.length} expired push subscriptions`);
  }
}

const insertUpdate = db.prepare('INSERT INTO updates (crew_name, message, photo_url, media_url, timestamp, estimated_lat, estimated_lon, wind_speed, wave_height, sea_temp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');

// Generate video thumbnail using ffmpeg
function generateVideoThumbnail(videoPath, thumbnailPath) {
  return new Promise((resolve, reject) => {
    ffmpeg(videoPath)
      .screenshots({
        timestamps: ['1'],
        filename: path.basename(thumbnailPath),
        folder: path.dirname(thumbnailPath),
        size: '320x?'
      })
      .on('end', () => resolve())
      .on('error', (err) => reject(err));
  });
}
const getUpdates = db.prepare('SELECT * FROM updates ORDER BY id DESC LIMIT 50');
const updateEstimatedLocation = db.prepare('UPDATE updates SET estimated_lat = ?, estimated_lon = ? WHERE id = ?');
const pinUpdate = db.prepare('UPDATE updates SET pinned = ? WHERE id = ?');
const unpinAll = db.prepare('UPDATE updates SET pinned = 0 WHERE pinned = 1');
const getUpdatesNearTime = db.prepare('SELECT id, timestamp FROM updates WHERE estimated_lat IS NOT NULL AND abs(julianday(timestamp) - julianday(?)) < (2.0/24.0)');
const getPositionBefore = db.prepare('SELECT * FROM positions WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1');
const getPositionAfter = db.prepare('SELECT * FROM positions WHERE timestamp > ? ORDER BY timestamp ASC LIMIT 1');

const insertPosition = db.prepare(`
  INSERT INTO positions (lat, lon, speed, course, heading, timestamp,
    wind_speed, wind_dir, wind_gust, wave_height, wave_period, swell_height, sea_temp)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const getLatest = db.prepare('SELECT * FROM positions ORDER BY id DESC LIMIT 1');
const getHistory = db.prepare('SELECT * FROM positions ORDER BY id ASC');

const insertMilestone = db.prepare('INSERT INTO milestones (name, lat, lon, timestamp) VALUES (?, ?, ?, ?)');
const getMilestones = db.prepare('SELECT * FROM milestones ORDER BY id ASC');
const getMilestoneByName = db.prepare('SELECT id FROM milestones WHERE name = ?');

const upsertReaction = db.prepare('INSERT INTO reactions (update_id, emoji, count) VALUES (?, ?, 1) ON CONFLICT(update_id, emoji) DO UPDATE SET count = count + 1');
const getReactionsForUpdate = db.prepare('SELECT emoji, count FROM reactions WHERE update_id = ?');

const insertComment = db.prepare('INSERT INTO comments (update_id, name, message, timestamp) VALUES (?, ?, ?, ?)');
const getCommentsForUpdate = db.prepare('SELECT * FROM comments WHERE update_id = ? ORDER BY id ASC LIMIT 50');

const insertDigest = db.prepare('INSERT OR IGNORE INTO digests (day_number, date, distance_nm, avg_speed, max_speed, positions_count, updates_count, weather_summary, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
const getDigests = db.prepare('SELECT * FROM digests ORDER BY date DESC LIMIT 30');
const getDigestByDate = db.prepare('SELECT id FROM digests WHERE date = ?');
const getPositionsForDate = db.prepare("SELECT * FROM positions WHERE date(timestamp) = ? ORDER BY timestamp ASC");
const getUpdatesCountForDate = db.prepare("SELECT count(*) as cnt FROM updates WHERE date(timestamp) = ?");

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

// --- Estimate Location for Crew Updates ---
function estimateLocation(targetTimestamp) {
  const before = getPositionBefore.get(targetTimestamp);
  const after = getPositionAfter.get(targetTimestamp);

  if (before && after) {
    // Interpolate between two positions
    const tBefore = new Date(before.timestamp).getTime();
    const tAfter = new Date(after.timestamp).getTime();
    const tTarget = new Date(targetTimestamp).getTime();
    const ratio = (tTarget - tBefore) / (tAfter - tBefore);
    return {
      lat: before.lat + (after.lat - before.lat) * ratio,
      lon: before.lon + (after.lon - before.lon) * ratio
    };
  }

  if (before) {
    // Dead reckoning from last known position
    const hoursDiff = (new Date(targetTimestamp).getTime() - new Date(before.timestamp).getTime()) / 3600000;
    const speed = before.speed || 0;
    const courseRad = (before.course || 0) * Math.PI / 180;
    const latRad = before.lat * Math.PI / 180;
    const newLat = before.lat + (speed * hoursDiff * Math.cos(courseRad)) / 60;
    const newLon = before.lon + (speed * hoursDiff * Math.sin(courseRad)) / (60 * Math.cos(latRad));
    return { lat: newLat, lon: newLon };
  }

  return { lat: null, lon: null };
}

// --- Retroactively update crew update locations near a new position ---
function retroactivelyUpdateLocations(newTimestamp) {
  const nearbyUpdates = getUpdatesNearTime.all(newTimestamp);
  for (const update of nearbyUpdates) {
    const loc = estimateLocation(update.timestamp);
    if (loc.lat != null) {
      updateEstimatedLocation.run(loc.lat, loc.lon, update.id);
    }
  }
}

// --- Milestone Detection ---
const MILESTONE_WAYPOINTS = ROUTE_WAYPOINTS.filter(w => w.name !== 'Open Sea' && w.name !== 'Ismailia');
const MILESTONE_RADIUS_NM = 30;

function checkMilestones(lat, lon, timestamp) {
  for (const wp of MILESTONE_WAYPOINTS) {
    const dist = haversineNm(lat, lon, wp.lat, wp.lon);
    if (dist <= MILESTONE_RADIUS_NM) {
      const existing = getMilestoneByName.get(wp.name);
      if (!existing) {
        insertMilestone.run(wp.name, lat, lon, timestamp);
        console.log(`Milestone reached: ${wp.name} at ${dist.toFixed(1)} nm`);
        // Send milestone push notification
        if (canSendNotification()) {
          sendPushToAll({ title: '\uD83C\uDFC1 BLUE MARINE', body: `Passed ${wp.name}!`, type: 'milestone' }).catch(err => console.error('Push error:', err.message));
        }
      }
    }
  }
}

// --- Save Position ---
async function savePosition(lat, lon, speed, course, heading, timestamp) {
  // Calculate speed from previous position
  let calculatedSpeed = null;
  const prev = getLatest.get();
  if (prev) {
    const distNm = haversineNm(prev.lat, prev.lon, lat, lon);
    const hoursElapsed = (new Date(timestamp).getTime() - new Date(prev.timestamp).getTime()) / 3600000;
    if (hoursElapsed > 0.01) { // at least ~36 seconds
      calculatedSpeed = Math.round((distNm / hoursElapsed) * 10) / 10;
    }
  }

  const weather = await fetchWeather(lat, lon);
  insertPosition.run(
    lat, lon, speed, course, heading, timestamp,
    weather.wind_speed, weather.wind_dir, weather.wind_gust,
    weather.wave_height, weather.wave_period, weather.swell_height, weather.sea_temp
  );
  // Update calculated_speed on the just-inserted row
  if (calculatedSpeed != null) {
    db.prepare('UPDATE positions SET calculated_speed = ? WHERE id = (SELECT MAX(id) FROM positions)').run(calculatedSpeed);
  }
  console.log(`Position saved: ${lat.toFixed(4)}, ${lon.toFixed(4)} @ ${speed} kn (calc: ${calculatedSpeed != null ? calculatedSpeed + ' kn' : 'n/a'}) | ${timestamp}`);
  // Update voyage phase
  try {
    const phase = updateStoredPhase(lat, lon);
    console.log(`Phase: ${phase.phase.name}`);
  } catch (err) { console.error('Phase update error:', err.message); }
  // Check for voyage milestones
  try { checkMilestones(lat, lon, timestamp); } catch (err) { console.error('Milestone check error:', err.message); }
  // Retroactively improve crew update locations near this position
  try { retroactivelyUpdateLocations(timestamp); } catch (err) { console.error('Retroactive location update error:', err.message); }
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
// Temporary debug endpoint
app.get('/api/debug', (req, res) => {
  res.json({
    DATA_DIR: process.env.DATA_DIR,
    dataDir: dataDir,
    dbExists: fs.existsSync(path.join(dataDir, 'tracker.db')),
    uploadsExists: fs.existsSync(path.join(dataDir, 'uploads')),
    dataDirContents: fs.existsSync(dataDir) ? fs.readdirSync(dataDir) : 'DIR NOT FOUND'
  });
});

app.get('/api/latest', readLimiter, (req, res) => {
  const row = getLatest.get();
  if (row) {
    // Include stored phase
    const phaseRow = db.prepare("SELECT value FROM config WHERE key = 'current_phase'").get();
    if (phaseRow) {
      try { row.current_phase = JSON.parse(phaseRow.value); } catch (e) {}
    }
  }
  res.json(row || null);
});

app.get('/api/history', readLimiter, (req, res) => {
  const rows = getHistory.all();
  res.json(rows);
});

app.get('/api/milestones', readLimiter, (req, res) => {
  const rows = getMilestones.all();
  res.json(rows);
});

app.get('/api/config', readLimiter, (req, res) => {
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

app.post('/api/position', adminLimiter, async (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.body.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const { lat, lon, speed, course, heading } = req.body;
  if (lat == null || lon == null) {
    return res.status(400).json({ error: 'lat and lon are required' });
  }
  // Duplicate detection: same lat/lon within last 60 seconds
  const recent = db.prepare("SELECT id FROM positions WHERE lat = ? AND lon = ? AND timestamp > datetime('now', '-60 seconds')").get(lat, lon);
  if (recent) {
    return res.status(409).json({ error: 'Duplicate position — already recorded' });
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
  const pin = req.headers['x-admin-pin'] || req.query.pin;
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

app.put('/api/position/:id', adminLimiter, (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.query.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid id' });
  const existing = db.prepare('SELECT * FROM positions WHERE id = ?').get(id);
  if (!existing) return res.status(404).json({ error: 'Position not found' });

  const { lat, lon, speed, course, heading } = req.body;
  const updates = [];
  const values = [];
  if (lat != null) { updates.push('lat = ?'); values.push(lat); }
  if (lon != null) { updates.push('lon = ?'); values.push(lon); }
  if (speed != null) { updates.push('speed = ?'); values.push(speed); }
  if (course != null) { updates.push('course = ?'); values.push(course); }
  if (heading != null) { updates.push('heading = ?'); values.push(heading); }
  if (updates.length === 0) return res.status(400).json({ error: 'No fields to update' });

  values.push(id);
  db.prepare('UPDATE positions SET ' + updates.join(', ') + ' WHERE id = ?').run(...values);
  console.log(`Position ${id} updated by admin`);
  res.json({ success: true });
});

// --- Updates API ---
app.get('/api/updates', readLimiter, (req, res) => {
  const rows = getUpdates.all();
  const result = rows.map(u => {
    const reactions = getReactionsForUpdate.all(u.id);
    const reactionMap = {};
    for (const r of reactions) reactionMap[r.emoji] = r.count;
    const comments = getCommentsForUpdate.all(u.id);
    return { ...u, reactions: reactionMap, comments };
  });
  // Put pinned updates first, then the rest by id desc (already sorted)
  result.sort((a, b) => (b.pinned || 0) - (a.pinned || 0));
  res.json(result);
});

app.post('/api/updates', adminLimiter, upload.array('files', 5), async (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.body.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const { crew_name, message } = req.body;
  const files = req.files || [];
  if (!message && files.length === 0) {
    return res.status(400).json({ error: 'Message or media required' });
  }

  // Check total upload size (100MB max)
  const totalSize = files.reduce((sum, f) => sum + f.size, 0);
  if (totalSize > 100 * 1024 * 1024) {
    // Clean up temp files
    for (const f of files) try { fs.unlinkSync(f.path); } catch (e) {}
    return res.status(400).json({ error: 'Total upload size exceeds 100MB' });
  }

  // Duplicate detection: same message text within last 60 seconds
  if (message) {
    const recent = db.prepare("SELECT id FROM updates WHERE message = ? AND timestamp > datetime('now', '-60 seconds')").get(message);
    if (recent) {
      for (const f of files) try { fs.unlinkSync(f.path); } catch (e) {}
      return res.status(409).json({ error: 'Duplicate update — already posted' });
    }
  }

  const mediaItems = [];
  for (const file of files) {
    try {
      const isVideo = file.mimetype.startsWith('video/');
      const ext = isVideo ? path.extname(file.originalname).toLowerCase() || '.mp4' : '.jpg';
      const basename = crypto.randomBytes(8).toString('hex');
      const filename = basename + ext;
      const outPath = path.join(uploadsDir, filename);

      if (isVideo) {
        // Move video file directly (no compression)
        try { fs.renameSync(file.path, outPath); } catch (e) {
          fs.copyFileSync(file.path, outPath);
          fs.unlinkSync(file.path);
        }
        // Generate thumbnail
        const thumbFilename = basename + '_thumb.jpg';
        const thumbPath = path.join(uploadsDir, thumbFilename);
        try {
          await generateVideoThumbnail(outPath, thumbPath);
        } catch (err) {
          console.error('Thumbnail generation error:', err.message);
        }
        const thumbExists = fs.existsSync(thumbPath);
        mediaItems.push({
          url: '/uploads/' + filename,
          type: 'video',
          thumbnail_url: thumbExists ? '/uploads/' + thumbFilename : null
        });
      } else {
        // Process image with sharp
        const buf = fs.readFileSync(file.path);
        await sharp(buf)
          .resize(800, 800, { fit: 'inside', withoutEnlargement: true })
          .jpeg({ quality: 80 })
          .toFile(outPath);
        try { fs.unlinkSync(file.path); } catch (e) {}
        mediaItems.push({
          url: '/uploads/' + filename,
          type: 'photo',
          thumbnail_url: null
        });
      }
    } catch (err) {
      console.error('Media processing error:', err.message);
      try { fs.unlinkSync(file.path); } catch (e) {}
    }
  }

  // Store media_url as JSON array, keep photo_url for backward compat
  const mediaValue = mediaItems.length > 0 ? JSON.stringify(mediaItems) : null;
  // Legacy photo_url: just photo URLs for backward compat
  const photoUrls = mediaItems.filter(m => m.type === 'photo').map(m => m.url);
  let photoValue = null;
  if (photoUrls.length === 1) photoValue = photoUrls[0];
  else if (photoUrls.length > 1) photoValue = JSON.stringify(photoUrls);

  const timestamp = new Date().toISOString();
  const name = (crew_name || 'Crew').trim().slice(0, 30);
  const loc = estimateLocation(timestamp);

  // Fetch weather at estimated position for the weather stamp
  let wx = { wind_speed: null, wave_height: null, sea_temp: null };
  if (loc.lat != null) {
    try {
      const w = await fetchWeather(loc.lat, loc.lon);
      wx.wind_speed = w.wind_speed;
      wx.wave_height = w.wave_height;
      wx.sea_temp = w.sea_temp;
    } catch (err) {
      console.error('Weather stamp fetch error:', err.message);
    }
  }

  insertUpdate.run(name, message || null, photoValue, mediaValue, timestamp, loc.lat, loc.lon, wx.wind_speed, wx.wave_height, wx.sea_temp);
  console.log(`Update posted by ${name}: ${message || '(media)'} [${mediaItems.length} files] | est. location: ${loc.lat != null ? loc.lat.toFixed(4) + ',' + loc.lon.toFixed(4) : 'none'}`);
  res.json({ success: true });

  // Send push notification (non-blocking)
  if (canSendNotification()) {
    const body = message ? `${name}: ${message.slice(0, 50)}${message.length > 50 ? '...' : ''}` : `${name} posted a new update`;
    sendPushToAll({ title: '\uD83D\uDCF8 BLUE MARINE', body, type: 'update' }).catch(err => console.error('Push error:', err.message));
  }
});

app.delete('/api/updates/:id', adminLimiter, (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.query.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid update id' });
  const result = db.prepare('DELETE FROM updates WHERE id = ?').run(id);
  if (result.changes === 0) return res.status(404).json({ error: 'Update not found' });
  // Clean up related reactions and comments
  db.prepare('DELETE FROM reactions WHERE update_id = ?').run(id);
  db.prepare('DELETE FROM comments WHERE update_id = ?').run(id);
  console.log(`Update ${id} deleted by admin`);
  res.json({ success: true, deleted: id });
});

// --- Reactions API ---
app.post('/api/updates/:id/react', reactLimiter, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid update id' });
  const { emoji } = req.body;
  const allowed = ['\u2764\uFE0F', '\uD83D\uDC4F', '\uD83C\uDF0A', '\u26F5', '\uD83D\uDE4F'];
  if (!emoji || !allowed.includes(emoji)) {
    return res.status(400).json({ error: 'Invalid emoji' });
  }
  upsertReaction.run(id, emoji);
  const reactions = getReactionsForUpdate.all(id);
  const reactionMap = {};
  for (const r of reactions) reactionMap[r.emoji] = r.count;
  res.json({ success: true, reactions: reactionMap });
});

// --- Comments API ---
app.post('/api/updates/:id/comment', commentLimiter, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid update id' });
  const { name, message } = req.body;
  if (!message || !message.trim()) {
    return res.status(400).json({ error: 'Message is required' });
  }
  const cleanName = (name || 'Anonymous').trim().slice(0, 30);
  const cleanMsg = message.trim().slice(0, 500);
  const timestamp = new Date().toISOString();
  insertComment.run(id, cleanName, cleanMsg, timestamp);
  const comments = getCommentsForUpdate.all(id);
  res.json({ success: true, comments });
});

// --- Pin API ---
app.post('/api/updates/:id/pin', adminLimiter, (req, res) => {
  const pin = req.headers['x-admin-pin'] || req.body.pin;
  if (pin !== ADMIN_PIN) {
    return res.status(403).json({ error: 'Invalid PIN' });
  }
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid update id' });
  // Check if already pinned — toggle off
  const update = db.prepare('SELECT pinned FROM updates WHERE id = ?').get(id);
  if (!update) return res.status(404).json({ error: 'Update not found' });
  if (update.pinned) {
    pinUpdate.run(0, id);
    res.json({ success: true, pinned: false });
  } else {
    unpinAll.run();
    pinUpdate.run(1, id);
    res.json({ success: true, pinned: true });
  }
});

// --- Digests API ---
app.get('/api/digests', readLimiter, (req, res) => {
  const rows = getDigests.all();
  res.json(rows);
});

// --- Daily Digest Generation ---
function generateDailyDigest() {
  // Get the first position timestamp to determine day 1
  const firstPos = db.prepare('SELECT timestamp FROM positions ORDER BY id ASC LIMIT 1').get();
  if (!firstPos) return;

  const startDate = new Date(firstPos.timestamp);
  startDate.setUTCHours(0, 0, 0, 0);

  // Generate digest for yesterday (CET = UTC+1, so yesterday in CET)
  const now = new Date();
  const cetOffset = 1; // CET is UTC+1 (simplified)
  const cetNow = new Date(now.getTime() + cetOffset * 3600000);
  const yesterday = new Date(cetNow);
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  const dateStr = yesterday.toISOString().slice(0, 10);

  // Skip if already generated
  if (getDigestByDate.get(dateStr)) return;

  const positions = getPositionsForDate.all(dateStr);
  if (positions.length === 0) return;

  // Calculate distance by summing segments
  let totalDist = 0;
  for (let i = 1; i < positions.length; i++) {
    totalDist += haversineNm(positions[i - 1].lat, positions[i - 1].lon, positions[i].lat, positions[i].lon);
  }

  const speeds = positions.filter(p => p.speed != null).map(p => p.speed);
  const avgSpeed = speeds.length > 0 ? speeds.reduce((a, b) => a + b, 0) / speeds.length : 0;
  const maxSpeed = speeds.length > 0 ? Math.max(...speeds) : 0;

  // Weather summary from position data
  const winds = positions.filter(p => p.wind_speed != null).map(p => p.wind_speed);
  const waves = positions.filter(p => p.wave_height != null).map(p => p.wave_height);
  const temps = positions.filter(p => p.sea_temp != null).map(p => p.sea_temp);
  const weatherParts = [];
  if (winds.length > 0) weatherParts.push('Wind ' + Math.round(Math.max(...winds)) + ' km/h');
  if (waves.length > 0) weatherParts.push('Waves ' + (Math.max(...waves)).toFixed(1) + 'm');
  if (temps.length > 0) weatherParts.push(Math.round(temps.reduce((a, b) => a + b, 0) / temps.length) + '\u00B0C');
  const weatherSummary = weatherParts.join(' \u00B7 ') || null;

  // Count updates for that day
  const updatesCount = getUpdatesCountForDate.get(dateStr).cnt;

  // Calculate day number
  const dayNumber = Math.floor((new Date(dateStr) - startDate) / 86400000) + 1;

  insertDigest.run(dayNumber, dateStr, Math.round(totalDist * 10) / 10, Math.round(avgSpeed * 10) / 10, Math.round(maxSpeed * 10) / 10, positions.length, updatesCount, weatherSummary, new Date().toISOString());
  console.log(`Daily digest generated: Day ${dayNumber} (${dateStr}) — ${totalDist.toFixed(1)} nm, ${positions.length} positions`);
}

// Run digest generation on startup and then every hour
try { generateDailyDigest(); } catch (err) { console.error('Digest generation error:', err.message); }
setInterval(() => {
  try { generateDailyDigest(); } catch (err) { console.error('Digest generation error:', err.message); }
}, 3600000);

// --- Signal Lost Check (every 30 min) ---
let signalLostNotified = false;
function checkSignalLost() {
  try {
    const latest = getLatest.get();
    if (!latest) return;
    const diffMs = Date.now() - new Date(latest.timestamp).getTime();
    const sixHours = 6 * 3600000;
    if (diffMs > sixHours && !signalLostNotified) {
      signalLostNotified = true;
      const coast = nearestCoastName(latest.lat, latest.lon);
      const locStr = coast ? `near ${coast}` : `at ${latest.lat.toFixed(2)}, ${latest.lon.toFixed(2)}`;
      if (canSendNotification()) {
        sendPushToAll({ title: '\u26A0\uFE0F BLUE MARINE', body: `No signal for 6+ hours \u2014 last seen ${locStr}`, type: 'signal_lost' }).catch(err => console.error('Push error:', err.message));
      }
    } else if (diffMs <= sixHours) {
      signalLostNotified = false;
    }
  } catch (err) {
    console.error('Signal lost check error:', err.message);
  }
}

// Server-side nearestCoastName for signal lost notifications
function nearestCoastName(lat, lon) {
  const coasts = [
    { name: 'Ismailia', lat: 30.60, lon: 32.27 },
    { name: 'Port Said', lat: 31.27, lon: 32.30 },
    { name: 'Limassol', lat: 34.67, lon: 33.04 },
    { name: 'Crete', lat: 35.34, lon: 25.14 },
    { name: 'Malta', lat: 35.90, lon: 14.51 },
    { name: 'Sicily', lat: 37.08, lon: 15.29 },
    { name: 'Sardinia', lat: 39.22, lon: 9.12 },
    { name: 'Formentera', lat: 38.71, lon: 1.44 },
    { name: 'Almería', lat: 36.84, lon: -2.46 },
    { name: 'Benalmádena', lat: 36.60, lon: -4.52 }
  ];
  let minDist = Infinity, closest = null;
  for (const c of coasts) {
    const d = haversineNm(lat, lon, c.lat, c.lon);
    if (d < minDist) { minDist = d; closest = c.name; }
  }
  return minDist < 100 ? closest : null;
}

setInterval(checkSignalLost, 30 * 60 * 1000);
setTimeout(checkSignalLost, 60000); // Check 1 min after startup

// --- Push Subscription API ---
app.get('/api/vapid-public-key', readLimiter, (req, res) => {
  res.json({ publicKey: vapidKeys.publicKey });
});

app.post('/api/subscribe', readLimiter, (req, res) => {
  const { endpoint, keys } = req.body;
  if (!endpoint || !keys || !keys.p256dh || !keys.auth) {
    return res.status(400).json({ error: 'Invalid subscription' });
  }
  try {
    db.prepare('INSERT OR REPLACE INTO subscriptions (endpoint, keys_p256dh, keys_auth, timestamp) VALUES (?, ?, ?, ?)').run(endpoint, keys.p256dh, keys.auth, new Date().toISOString());
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Failed to save subscription' });
  }
});

app.post('/api/unsubscribe', readLimiter, (req, res) => {
  const { endpoint } = req.body;
  if (!endpoint) return res.status(400).json({ error: 'Endpoint required' });
  db.prepare('DELETE FROM subscriptions WHERE endpoint = ?').run(endpoint);
  res.json({ success: true });
});

// --- Forecast API ---
app.get('/api/forecast', readLimiter, (req, res) => {
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
  // Seed current phase from latest position on startup
  try {
    const latest = getLatest.get();
    if (latest) {
      const phase = updateStoredPhase(latest.lat, latest.lon);
      console.log(`Phase initialized: ${phase.phase.name}`);
    }
  } catch (err) { console.error('Phase init error:', err.message); }
  connectAIS();
});
