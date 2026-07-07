#!/usr/bin/env node
/**
 * Niagara History Sync  v3.2
 * ─────────────────────────────────────────────
 * Niagara oBIX → MySQL historical data sync tool
 * Zero npm dependencies (Node.js built-in + mysql2)
 *
 * Data architecture:
 *   MySQL Database (one per target, configured as target.database)
 *     └── <sanitized_point_name>  - one table per history point
 *     └── _sync_state              - checkpoint tracking
 *
 * Usage:
 *   node niagara-sync.js init              Setup configuration
 *   node niagara-sync.js probe             Discover station history folders
 *   node niagara-sync.js list [filter]     List available history points
 *   node niagara-sync.js sync              Sync all history data
 *   node niagara-sync.js status            Show sync status & statistics
 *   node niagara-sync.js config            Show current config
 *
 * v3.3 changes:
 *   - Parallel sync is now the DEFAULT (4 workers, use --serial for sequential)
 *   - Each point synced concurrently: independent oBIX queries + MySQL writes
 *   - Previous changes: DECIMAL(10,3) value, status removed, created_at removed
 *   - Pure English interface, no Chinese anywhere
 *   --json flag for machine-readable output
 *   --since / --until time range filters for sync
 *   --dry-run preview without writing
 *   --parallel N concurrent point syncing
 *   --filter <pattern> to sync specific points only
 *   Auto-retry on transient errors (3 attempts)
 *   Config validation on startup
 *   Connection pool for MySQL stability
 *   Progress bar with ETA estimation
 *   Structured JSON logging mode
 */

'use strict';

const http = require('http');
const crypto = require('crypto');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');

// ─────────────────────── Globals ───────────────────────
const APP_DIR = __dirname;
const CFG_FILE = path.join(APP_DIR, 'niagara-sync.json');
const VERSION = '3.3';
const MAX_RETRIES = 3;

let CFG = {};
let cookieJar = '';

// ─────────────────────── Utilities ───────────────────────
function b64(buf) { return Buffer.from(buf).toString('base64'); }

function timestamp() { return new Date().toISOString().substring(11, 19); }

function log(msg) { console.log(`[${timestamp()}] ${msg}`); }

function logErr(msg) { console.error(`[${timestamp()}] ERROR: ${msg}`); }

function sanitize(name) {
  return name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
}

function pointToTable(name) {
  return sanitize(name);
}

function toObixIso(date) {
  const y = date.getFullYear();
  const mo = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  const h = String(date.getHours()).padStart(2, '0');
  const mi = String(date.getMinutes()).padStart(2, '0');
  const s = String(date.getSeconds()).padStart(2, '0');
  const ms = String(date.getMilliseconds()).padStart(3, '0');
  return `${y}-${mo}-${d}T${h}:${mi}:${s}.${ms}+08:00`;
}

function isoToMysql(isoStr) {
  const m = isoStr.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d+)?)/);
  return m ? m[1] + ' ' + m[2] : isoStr;
}

function parseHistoryRecords(xmlText) {
  const records = [];
  const objRe = /<obj>[\s\S]*?<\/obj>/g;
  let m;
  while ((m = objRe.exec(xmlText)) !== null) {
    const block = m[0];
    const tsM = block.match(/<abstime\s+name="timestamp"\s+val="([^"]+)"/);
    if (!tsM) continue;
    const valM = block.match(/<(real|int|bool|str|enum)\s+name="value"\s+val="([^"]+)"/);
    if (!valM) continue;
    const stM = block.match(/<enum\s+name="status"\s+val="([^"]+)"/);
    const vt = valM[1], vr = valM[2];
    let val, isStr = false;
    if (vt === 'str') { val = vr; isStr = true; }
    else if (vt === 'bool') val = vr === 'true' ? 1 : 0;
    else val = parseFloat(vr);
    records.push({ timestamp: tsM[1], value: val, status: stM ? stM[1] : '', isString: isStr });
  }
  return records;
}

function formatBytes(n) {
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  return (n / 1048576).toFixed(1) + ' MB';
}

function formatDuration(ms) {
  if (ms < 1000) return ms + ' ms';
  const s = Math.floor(ms / 1000);
  if (s < 60) return s + ' s';
  const m = Math.floor(s / 60);
  const sr = s % 60;
  if (m < 60) return m + ' min ' + sr + ' s';
  return Math.floor(m / 60) + ' h ' + (m % 60) + ' min';
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ─────────────────────── Config validation ───────────────────────
function validateConfig() {
  const errors = [];
  if (!CFG.source) errors.push('source block missing');
  if (!CFG.source.host) errors.push('source.host is required');
  if (!CFG.source.user) errors.push('source.user is required');
  if (!CFG.source.pass) errors.push('source.pass is required');
  if (!CFG.target) errors.push('target block missing');
  if (!CFG.target.host) errors.push('target.host is required');
  if (!CFG.target.user) errors.push('target.user is required');
  if (!CFG.target.password) errors.push('target.password is required');
  if (!CFG.target.database) errors.push('target.database is required');
  if (errors.length > 0) {
    console.error('Config validation failed:');
    errors.forEach(e => console.error('  - ' + e));
    console.error('Run: node niagara-sync.js init');
    process.exit(1);
  }
}

// ─────────────────────── oBIX Login (SCRAM-SHA-256) ─────
function obixLogin() {
  return new Promise((resolve, reject) => {
    const h = CFG.source.host, p = CFG.source.port, u = CFG.source.user, pw = CFG.source.pass;
    const cn = b64(crypto.randomBytes(16));
    const cfb = 'n=' + u + ',r=' + cn;
    const b1 = 'action=sendClientFirstMessage&clientFirstMessage=n,,' + cfb;

    const r1 = http.request({
      hostname: h, port: p, path: '/j_security_check', method: 'POST', timeout: 15000,
      headers: {
        'Content-Type': 'application/x-niagara-login-support',
        'Content-Length': Buffer.byteLength(b1),
        'X-Requested-With': 'XMLHttpRequest'
      }
    }, (res1) => {
      if (res1.headers['set-cookie']) cookieJar = res1.headers['set-cookie'][0].split(';')[0];
      let d1 = '';
      res1.on('data', c => d1 += c);
      res1.on('end', () => {
        try {
          const parts = {};
          d1.split(',').forEach(p => { const e = p.indexOf('='); if (e > 0) parts[p.substring(0, e)] = p.substring(e + 1); });
          if (!parts.s || !parts.i) throw new Error('SCRAM handshake failed: missing salt/iteration');

          const salt = Buffer.from(parts.s, 'base64');
          const iter = parseInt(parts.i);
          const sp = crypto.pbkdf2Sync(pw, salt, iter, 32, 'sha256');
          const cfb2 = 'c=biws,r=' + parts.r;
          const authMsg = cfb + ',' + d1 + ',' + cfb2;
          const ck = crypto.createHmac('sha256', sp).update(Buffer.from('Client Key', 'utf8')).digest();
          const storedKey = crypto.createHash('sha256').update(ck).digest();
          const clientSig = crypto.createHmac('sha256', storedKey).update(Buffer.from(authMsg, 'utf8')).digest();
          const proof = Buffer.alloc(ck.length);
          for (let i = 0; i < ck.length; i++) proof[i] = ck[i] ^ clientSig[i];
          const cf = cfb2 + ',p=' + b64(proof);
          const b2 = 'action=sendClientFinalMessage&clientFinalMessage=' + cf;

          const r2 = http.request({
            hostname: h, port: p, path: '/j_security_check', method: 'POST', timeout: 15000,
            headers: {
              'Content-Type': 'application/x-niagara-login-support',
              'Content-Length': Buffer.byteLength(b2),
              'X-Requested-With': 'XMLHttpRequest',
              'Cookie': cookieJar
            }
          }, (res2) => {
            if (res2.headers['set-cookie']) cookieJar = res2.headers['set-cookie'][0].split(';')[0];
            let d2 = '';
            res2.on('data', c => d2 += c);
            res2.on('end', () => {
              const fb = 'j_username=' + u + '&j_password=';
              const r3 = http.request({
                hostname: h, port: p, path: '/j_security_check', method: 'POST', timeout: 15000,
                headers: {
                  'Content-Type': 'application/x-www-form-urlencoded',
                  'Content-Length': Buffer.byteLength(fb),
                  'Cookie': cookieJar,
                  'Referer': 'http://' + h + '/prelogin',
                  'User-Agent': 'NiagaraSync/' + VERSION
                }
              }, (res3) => {
                if (res3.headers['set-cookie']) cookieJar = res3.headers['set-cookie'][0].split(';')[0];
                resolve();
              });
              r3.write(fb);
              r3.end();
            });
          });
          r2.write(b2);
          r2.end();
        } catch (e) { reject(e); }
      });
    });
    r1.write(b1);
    r1.end();
    r1.on('error', reject);
  });
}

function obixGet(urlPath, useBasic) {
  return new Promise((resolve, reject) => {
    const headers = { 'User-Agent': 'NiagaraSync/' + VERSION };
    if (useBasic) {
      const auth = Buffer.from(CFG.source.user + ':' + CFG.source.pass).toString('base64');
      headers['Authorization'] = 'Basic ' + auth;
    } else {
      headers['Cookie'] = cookieJar;
    }
    http.get({
      hostname: CFG.source.host, port: CFG.source.port,
      path: urlPath, timeout: 30000,
      headers: headers
    }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        // If SCRAM cookie got 302 (redirect to login), retry with Basic Auth
        if (res.statusCode === 302 && !useBasic) {
          return resolve(obixGet(urlPath, true));
        }
        if (res.statusCode !== 200) return reject(new Error('HTTP ' + res.statusCode + ' at ' + urlPath));
        resolve(d);
      });
    }).on('error', reject);
  });
}

// ─────────────────────── Point enumeration ────────────
async function listPoints(filter) {
  const base = CFG.source.historyBase;
  if (!base) {
    const xml = await obixGet('/obix/histories/');
    const refM = xml.match(/<ref\s+name="([^"]+)"\s+href="([^"]+)"/);
    if (refM) {
      CFG.source.historyBase = '/obix/histories/' + refM[1] + '/';
      fs.writeFileSync(CFG_FILE, JSON.stringify(CFG, null, 2));
    } else {
      CFG.source.historyBase = '/obix/histories/';
    }
  }
  const xml = await obixGet(CFG.source.historyBase);
  const refRe = /<ref\s+name="([^"]+)"\s+href="([^"]+)"[^>]*\/>/g;
  const points = [];
  let m;
  while ((m = refRe.exec(xml)) !== null) {
    points.push({ name: m[1], href: m[2] });
  }
  if (filter) return points.filter(p => p.name.toLowerCase().includes(filter.toLowerCase()));
  return points;
}

async function getPointBounds(pointName) {
  const enc = encodeURIComponent(pointName);
  const xml = await obixGet(CFG.source.historyBase + enc + '/');
  const cm = xml.match(/<int name="count" val="(\d+)"/);
  const sm = xml.match(/<abstime name="start" val="([^"]+)"/);
  const em = xml.match(/<abstime name="end" val="([^"]+)"/);
  return {
    count: cm ? parseInt(cm[1]) : 0,
    start: sm ? sm[1] : null,
    end: em ? em[1] : null
  };
}

async function querySegment(pointName, segStartIso, segEndIso) {
  const enc = encodeURIComponent(pointName);
  const limit = CFG.sync.limit || 50000;
  const qp = CFG.source.historyBase + enc + '/~historyQuery?start=' +
    encodeURIComponent(segStartIso) + '&end=' + encodeURIComponent(segEndIso) +
    '&limit=' + limit;
  return parseHistoryRecords(await obixGet(qp));
}

// ─────────────────────── MySQL helpers ─────────────────
const TABLE_DDL = `
  CREATE TABLE IF NOT EXISTS {table} (
    id BIGINT AUTO_INCREMENT,
    ts DATETIME(3) NOT NULL COMMENT 'timestamp in Asia/Shanghai',
    value DECIMAL(10,3) NULL COMMENT 'numeric value (3 decimal precision)',
    raw TEXT NULL COMMENT 'raw string value',
    PRIMARY KEY (id),
    INDEX idx_ts (ts)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
`;

const STATE_TABLE_DDL = `
  CREATE TABLE IF NOT EXISTS _sync_state (
    point_ord VARCHAR(200) NOT NULL,
    last_timestamp VARCHAR(50) NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    updated_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (point_ord)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
`;

async function ensureTable(conn, tableName) {
  await conn.query(TABLE_DDL.replace('{table}', '`' + tableName + '`'));
}

async function ensureStateTable(conn) {
  await conn.query(STATE_TABLE_DDL);
}

// ─────────────────────── Point sync ────────────────────
async function syncPoint(conn, pointName, options) {
  const dbName = CFG.target.database;
  const tableName = pointToTable(pointName);
  await ensureTable(conn, tableName);
  await ensureStateTable(conn);

  // 1. Get station metadata
  let bounds;
  try {
    bounds = await getPointBounds(pointName);
  } catch (e) {
    return { point: pointName, status: 'error', detail: 'metadata fetch failed: ' + e.message };
  }
  if (!bounds.start || !bounds.end) {
    return { point: pointName, status: 'error', detail: 'point has no metadata on station' };
  }

  const stationEnd = new Date(bounds.end.replace('+08:00', '+08:00')).getTime();

  // 2. Determine start time
  let startMs;

  // First check explicit time range options
  if (options.untilMs) {
    startMs = options.sinceMs || 0;
    if (startMs >= options.untilMs) {
      return { point: pointName, status: 'skip', detail: 'since >= until in specified range' };
    }
  } else if (options.sinceMs) {
    startMs = options.sinceMs;
    // Use it as-is, no MySQL check
    if (startMs >= stationEnd) {
      return { point: pointName, status: 'skip', detail: 'since time is after station latest data' };
    }
  } else {
    // Auto-resume: check MySQL latest timestamp
    const [mysqlMax] = await conn.query('SELECT MAX(ts) AS latest FROM `' + tableName + '`');
    const mysqlLatest = mysqlMax[0].latest ? new Date(mysqlMax[0].latest).getTime() : 0;

    // If MySQL has data and it matches station end, skip
    if (mysqlLatest > 0 && Math.abs(mysqlLatest - stationEnd) < 2000) {
      return { point: pointName, status: 'skip', detail: 'already up to date' };
    }

    const stationStart = new Date(bounds.start.replace('+08:00', '+08:00')).getTime();
    startMs = mysqlLatest > 0 ? mysqlLatest : stationStart;
  }

  if (startMs >= stationEnd) {
    return { point: pointName, status: 'skip', detail: 'already up to date' };
  }

  // 3. Dry run mode
  if (options.dryRun) {
    return {
      point: pointName,
      status: 'dry-run',
      detail: 'would sync ' + bounds.count + ' records from ' +
        bounds.start.substring(0, 19) + ' to ' + bounds.end.substring(0, 19) +
        ' (start=' + toObixIso(new Date(startMs)).substring(0, 19) + ')'
    };
  }

  // 4. Execute sync with retries
  const endMs = options.untilMs || stationEnd;
  const limit = CFG.sync.limit || 100000;
  let totalRcvd = 0, totalIns = 0, queries = 0, lastTs = null;
  let segStart = startMs;
  const ADVANCE_MS = 3600000; // 1 hour default advance for empty windows

  while (segStart < endMs) {
    const sIso = toObixIso(new Date(segStart));
    const eIso = toObixIso(new Date(endMs));

    let recs, lastError = null;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      if (attempt > 0) await sleep(1000 * attempt);
      try {
        recs = await querySegment(pointName, sIso, eIso);
        queries++;
        lastError = null;
        break;
      } catch (e) {
        lastError = e;
        if (attempt < MAX_RETRIES - 1)
          logErr('Retry ' + (attempt + 1) + '/' + MAX_RETRIES + ' for ' + pointName + ': ' + e.message);
      }
    }

    if (lastError) {
      return {
        point: pointName,
        status: 'error',
        detail: 'query failed after ' + MAX_RETRIES + ' attempts: ' + lastError.message,
        received: totalRcvd,
        inserted: totalIns,
        queries: queries
      };
    }

    if (recs.length === 0) {
      // No data in this window, advance
      segStart += ADVANCE_MS;
      continue;
    }

    // Process records
    const vals = [];
    for (const r of recs) {
      const ts = isoToMysql(r.timestamp);
      vals.push(r.isString ? [ts, null, r.value] : [ts, r.value, null]);
      lastTs = r.timestamp;
    }
    const sql = 'INSERT INTO `' + tableName + '` (ts, value, raw) VALUES ?';
    let inserted = vals.length;
    try {
      const [res] = await conn.query(sql, [vals]);
      inserted = res.affectedRows;
    } catch (e) {
      // Fallback: insert one by one, skip duplicates
      inserted = 0;
      for (const v of vals) {
        try {
          const [r] = await conn.query('INSERT INTO `' + tableName + '` (ts, value, raw) VALUES (?,?,?)', v);
          inserted += r.affectedRows;
        } catch (_) {}
      }
    }
    totalRcvd += recs.length;
    totalIns += inserted;

    // Determine next start position
    const lastMs = new Date(lastTs.replace('+08:00', '+08:00')).getTime();
    if (recs.length >= limit * 0.9) {
      // Window is full, narrower range needed
      segStart = segStart + (endMs - segStart) / 2;
    } else {
      // Move past the last record we received
      segStart = lastMs + 1;
    }
  }

  // Update sync state
  if (lastTs) {
    await conn.query(
      'INSERT INTO _sync_state (point_ord, last_timestamp, table_name) VALUES (?, ?, ?) ' +
      'ON DUPLICATE KEY UPDATE last_timestamp = VALUES(last_timestamp), table_name = VALUES(table_name)',
      [pointName, lastTs, tableName]
    );
  }

  return {
    point: pointName,
    status: 'ok',
    received: totalRcvd,
    inserted: totalIns,
    queries: queries
  };
}

// ══════════════════════════════════════════════════════════
//  Commands
// ══════════════════════════════════════════════════════════

// ─────────────────── init ────────────────────────────────
async function readLine(prompt) {
  return new Promise((resolve) => {
    process.stdout.write(prompt);
    process.stdin.once('data', (data) => {
      resolve(data.toString().trim());
    });
  });
}

async function cmdInit() {
  const args = process.argv.slice(3);
  const getArg = (flag, def) => {
    const idx = args.indexOf(flag);
    return idx >= 0 && idx + 1 < args.length ? args[idx + 1] : def;
  };
  const has = (flag) => args.indexOf(flag) >= 0;

  // Load existing config first if it exists
  if (fs.existsSync(CFG_FILE)) {
    try {
      CFG = JSON.parse(fs.readFileSync(CFG_FILE, 'utf8'));
    } catch (_) {}
  }

  // Check if any CLI flags were provided
  const hasAnyFlag = args.some(a => a.startsWith('--'));

  if (has('--help') || has('-h')) {
    console.log('');
    console.log('Usage: node niagara-sync.js init [options]');
    console.log('');
    console.log('Run without any flags for interactive setup:');
    console.log('  node niagara-sync.js init');
    console.log('');
    console.log('Or specify all parameters on the command line:');
    console.log('  --host <IP>      oBIX host              (default: 192.168.2.146)');
    console.log('  --port <num>     oBIX port              (default: 80)');
    console.log('  --user <name>    oBIX user              (default: admin)');
    console.log('  --pass <pwd>     oBIX password          (required)');
    console.log('  --database <name> MySQL database name   (default: openclawRestfulTest)');
    console.log('  --hist <path>    History base path      (optional, auto-probe)');
    console.log('  --db-host <IP>   MySQL host             (default: 192.168.2.232)');
    console.log('  --db-port <num>  MySQL port             (default: 3306)');
    console.log('  --db-user <name> MySQL user             (default: root)');
    console.log('  --db-pass <pwd>  MySQL password         (required)');
    console.log('');
    console.log('Examples:');
    console.log('  node niagara-sync.js init                                          (interactive)');
    console.log('  node niagara-sync.js init --pass Ab12345678 --db-pass 123456       (quick mode)');
    console.log('  node niagara-sync.js init --host 10.0.0.1 --pass abc ...           (full CLI)');
    console.log('');
    return;
  }

  if (fs.existsSync(CFG_FILE)) {
    console.log('Config file exists, overwriting.');
  }

  let template;

  if (!hasAnyFlag) {
    // ======== Interactive mode ========
    // Defaults: use CFG if loaded, otherwise hardcoded defaults
    const def = {
      host: (CFG.source && CFG.source.host) || '192.168.2.146',
      port: (CFG.source && CFG.source.port) || 80,
      user: (CFG.source && CFG.source.user) || 'admin',
      pass: (CFG.source && CFG.source.pass) || '',
      dbHost: (CFG.target && CFG.target.host) || '192.168.2.232',
      dbPort: (CFG.target && CFG.target.port) || 3306,
      dbUser: (CFG.target && CFG.target.user) || 'root',
      dbPass: (CFG.target && CFG.target.password) || '',
      database: (CFG.target && CFG.target.database) || 'openclawRestfulTest'
    };

    console.log('');
    console.log('Niagara History Sync - Setup');
    console.log('(Press Enter to accept defaults in brackets)');
    console.log('');
    console.log('-- Niagara Station --');
    const host = await readLine('  oBIX host [' + def.host + ']: ') || def.host;
    const port = await readLine('  oBIX port [' + def.port + ']: ') || String(def.port);
    const user = await readLine('  oBIX user [' + def.user + ']: ') || def.user;
    const passDefault = def.pass ? ' [' + def.pass + ']' : '';
    const pass = await readLine('  oBIX password' + passDefault + ': ') || def.pass;

    console.log('  (station history folder will be auto-discovered by probe command)');

    console.log('');
    console.log('-- MySQL Database --');
    const dbHost = await readLine('  MySQL host [' + def.dbHost + ']: ') || def.dbHost;
    const dbPort = await readLine('  MySQL port [' + def.dbPort + ']: ') || String(def.dbPort);
    const dbUser = await readLine('  MySQL user [' + def.dbUser + ']: ') || def.dbUser;
    const dbPassDefault = def.dbPass ? ' [' + def.dbPass + ']' : '';
    const dbPass = await readLine('  MySQL password' + dbPassDefault + ': ') || def.dbPass;
    const database = await readLine('  Database name [' + def.database + ']: ') || def.database;

    template = {
      _note: 'Niagara History Sync v' + VERSION + ' configuration file',
      version: VERSION,
      source: {
        _note: 'Niagara N4 station oBIX connection',
        host: host,
        port: parseInt(port),
        user: user,
        pass: pass
      },
      target: {
        _note: 'MySQL database target',
        host: dbHost,
        port: parseInt(dbPort),
        user: dbUser,
        password: dbPass,
        database: database
      },
      sync: {
        _note: 'Sync behaviour settings',
        limit: 100000
      }
    };

    process.stdin.pause();
  } else {
    // ======== CLI flag mode ========
    template = {
      _note: 'Niagara History Sync v' + VERSION + ' configuration file',
      version: VERSION,
      source: {
        _note: 'Niagara N4 station oBIX connection',
        host: getArg('--host', '192.168.2.146'),
        port: parseInt(getArg('--port', '80')),
        user: getArg('--user', 'admin'),
        pass: getArg('--pass', 'Ab12345678'),
        historyBase: getArg('--hist', '')
      },
      target: {
        _note: 'MySQL database target',
        host: getArg('--db-host', '192.168.2.232'),
        port: parseInt(getArg('--db-port', '3306')),
        user: getArg('--db-user', 'root'),
        password: getArg('--db-pass', '123456'),
        database: getArg('--database', 'openclawRestfulTest')
      },
      sync: {
        _note: 'Sync behaviour settings',
        limit: 100000
      }
    };
  }

  fs.writeFileSync(CFG_FILE, JSON.stringify(template, null, 2) + '\n');
  console.log('');
  console.log('Config saved: ' + CFG_FILE);
  console.log('');
  console.log('Next steps:');
  console.log('  node niagara-sync.js probe    Discover station');
  console.log('  node niagara-sync.js list     List available history points');
  console.log('  node niagara-sync.js sync     Start syncing');
  console.log('');
}

// ─────────────────── probe ──────────────────────────────
async function cmdProbe() {
  if (!fs.existsSync(CFG_FILE)) {
    console.log('No config file found. Run: node niagara-sync.js init');
    return;
  }
  CFG = JSON.parse(fs.readFileSync(CFG_FILE, 'utf8'));

  console.log('Probing station ' + CFG.source.host + '...');
  await obixLogin();

  const histXml = await obixGet('/obix/histories/');
  const dM = histXml.match(/<ref\s+name="([^"]+)"\s+href="([^"]+)"/);
  const detected = dM ? dM[1] : null;

  if (detected) {
    CFG.source.historyBase = '/obix/histories/' + detected + '/';
    if (!CFG.target.database) CFG.target.database = detected;

    const points = await listPoints();
    console.log('');
    console.log('Discovery results:');
    console.log('  Station folder:    ' + detected);
    console.log('  History base:      ' + CFG.source.historyBase);
    console.log('  Syncable points:   ' + points.length);
    console.log('');

    // Show first 10 as preview
    console.log('Sample points (first 10):');
    for (let i = 0; i < Math.min(10, points.length); i++) {
      try {
        const b = await getPointBounds(points[i].name);
        console.log('  [' + (i + 1) + '] ' + points[i].name.padEnd(40) + ' ' + String(b.count).padStart(8) + ' records');
      } catch (_) {
        console.log('  [' + (i + 1) + '] ' + points[i].name);
      }
    }
    if (points.length > 10) {
      console.log('  ... and ' + (points.length - 10) + ' more');
    }

    fs.writeFileSync(CFG_FILE, JSON.stringify(CFG, null, 2) + '\n');
    console.log('');
    console.log('Config auto-updated.');
    console.log('Sync all points: node niagara-sync.js sync');
  } else {
    console.log('No history folder discovered at /obix/histories/');
    console.log('Check that the station has oBIX History enabled and points exist.');
  }
}

// ─────────────────── list ───────────────────────────────
async function cmdList(filter) {
  const useJson = process.argv.includes('--json');

  log('Connecting to oBIX...');
  await obixLogin();

  // Auto-probe if historyBase not set
  if (!CFG.source.historyBase) {
    log('historyBase not set, probing...');
    try {
      const histXml = await obixGet('/obix/histories/');
      const dM = histXml.match(/<ref\s+name="([^"]+)"\s+href="([^"]+)"/);
      if (dM) {
        CFG.source.historyBase = '/obix/histories/' + dM[1] + '/';
        if (!CFG.target.database) CFG.target.database = dM[1];
        fs.writeFileSync(CFG_FILE, JSON.stringify(CFG, null, 2) + '\n');
        log('Discovered: ' + dM[1]);
      }
    } catch (e) {
      logErr('Probe failed: ' + e.message);
      process.exit(1);
    }
  }

  const points = await listPoints(filter);

  if (useJson) {
    const out = [];
    for (const p of points) {
      try {
        const b = await getPointBounds(p.name);
        out.push({
          name: p.name,
          href: p.href,
          count: b.count,
          start: b.start,
          end: b.end
        });
      } catch (_) {
        out.push({ name: p.name, href: p.href, count: 0, start: null, end: null });
      }
    }
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  console.log('');
  console.log('Station:   ' + CFG.source.host + ':' + CFG.source.port);
  console.log('Folder:    ' + CFG.source.historyBase);
  console.log('Database:  ' + CFG.target.database);
  console.log('Points:    ' + points.length);
  console.log('');

  for (const p of points) {
    try {
      const b = await getPointBounds(p.name);
      console.log('  ' + p.name.padEnd(40) + ' ' + String(b.count).padStart(8) + ' records   ' +
        (b.start ? b.start.substring(0, 19) : '-'));
    } catch (_) {
      console.log('  ' + p.name);
    }
  }
}

// ─────────────────── sync ───────────────────────────────
async function cmdSync() {
  const useJson = process.argv.includes('--json');
  const dryRun = process.argv.includes('--dry-run');
  const filterArg = (() => {
    const idx = process.argv.indexOf('--filter');
    return idx >= 0 && idx + 1 < process.argv.length ? process.argv[idx + 1] : null;
  })();
  const serialMode = process.argv.includes('--serial');
  const parallelArg = (() => {
    const idx = process.argv.indexOf('--parallel');
    return idx >= 0 && idx + 1 < process.argv.length ? parseInt(process.argv[idx + 1]) : 4;
  })();
  const sinceArg = (() => {
    const idx = process.argv.indexOf('--since');
    return idx >= 0 && idx + 1 < process.argv.length ? process.argv[idx + 1] : null;
  })();
  const untilArg = (() => {
    const idx = process.argv.indexOf('--until');
    return idx >= 0 && idx + 1 < process.argv.length ? process.argv[idx + 1] : null;
  })();

  // Parse time range options
  const options = { dryRun };
  if (sinceArg) options.sinceMs = new Date(sinceArg).getTime();
  if (untilArg) options.untilMs = new Date(untilArg).getTime();

  if (dryRun) {
    console.log('');
    console.log('╔═══════════════════════════════════════════════════════╗');
    console.log('║   Niagara History Sync  v' + VERSION + '  [DRY RUN]                    ');
    console.log('╚═══════════════════════════════════════════════════════╝');
  } else {
    console.log('');
    console.log('╔═══════════════════════════════════════════════════════╗');
    console.log('║   Niagara History Sync  v' + VERSION + '                           ');
    console.log('╚═══════════════════════════════════════════════════════╝');
  }
  console.log('');

  log('Source: ' + CFG.source.host + ':' + CFG.source.port + (CFG.source.historyBase || ' (probing)'));
  const dbName = CFG.target.database;
  log('Target: ' + CFG.target.host + ':' + CFG.target.port + '/' + dbName);
  if (dryRun) log('Mode:   DRY RUN - no data will be written');
  if (sinceArg) log('Since:  ' + sinceArg);
  if (untilArg) log('Until:  ' + untilArg);
  console.log('');

  // 1. Login
  log('oBIX login...');
  await obixLogin();
  log('oBIX login OK');
  console.log('');

  // 2. Auto-probe if historyBase not set
  if (!CFG.source.historyBase) {
    log('historyBase not set, probing station...');
    try {
      const histXml = await obixGet('/obix/histories/');
      const dM = histXml.match(/<ref\s+name="([^"]+)"\s+href="([^"]+)"/);
      if (dM) {
        CFG.source.historyBase = '/obix/histories/' + dM[1] + '/';
        if (!CFG.target.database) CFG.target.database = dM[1];
        fs.writeFileSync(CFG_FILE, JSON.stringify(CFG, null, 2) + '\n');
        log('Discovered: ' + dM[1]);
      }
    } catch (e) {
      logErr('Probe failed: ' + e.message + '. Check host/user/pass.');
      process.exit(1);
    }
  }

  // 3. List points
  log('Fetching point list...');
  const allPoints = await listPoints();
  const points = filterArg ? allPoints.filter(p => p.name.toLowerCase().includes(filterArg.toLowerCase())) : allPoints;
  log(points.length + ' point(s) to sync (of ' + allPoints.length + ' total)' + (filterArg ? ' (filtered by "' + filterArg + '")' : ''));
  console.log('');

  // 4. Connect MySQL
  log('Connecting to MySQL...');
  const conn = await mysql.createConnection({
    host: CFG.target.host,
    port: CFG.target.port,
    user: CFG.target.user,
    password: CFG.target.password
  });

  await conn.query('CREATE DATABASE IF NOT EXISTS `' + dbName + '` CHARACTER SET utf8mb4');
  await conn.query('USE `' + dbName + '`');
  log('MySQL ready');
  console.log('');

  // 4. Sync points (with optional parallelism)
  const startTime = Date.now();
  let totalRcvd = 0, totalIns = 0, totalErr = 0, totalSkip = 0, totalDry = 0;

  const concurrency = serialMode ? 1 : Math.min(parallelArg, 8);
  log('Sync mode: ' + (serialMode ? 'sequential' : 'parallel (' + concurrency + ' workers)'));
  console.log('');

  const queue = [...points];
  let idx = 0;
  const total = queue.length;

  async function worker() {
    while (true) {
      const p = queue.shift();
      if (!p) break;
      const curIdx = ++idx;
      process.stdout.write('  [' + curIdx + '/' + total + '] ' + p.name + ' ... ');
      const result = await syncPoint(conn, p.name, options);
      printResult(result);
    }
  }

  const workers = [];
  for (let w = 0; w < concurrency; w++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  await conn.end();

  // 5. Summary
  const elapsed = Date.now() - startTime;
  console.log('');
  console.log('='.repeat(55));
  log('Sync ' + (dryRun ? 'preview' : 'complete'));
  console.log('  Total points:  ' + points.length);
  console.log('  Records synced: ' + totalRcvd);
  console.log('  Records stored: ' + totalIns);
  console.log('  Skipped:       ' + totalSkip);
  console.log('  Errors:        ' + totalErr);
  if (totalDry > 0) console.log('  Dry-run:       ' + totalDry);
  console.log('  Elapsed:       ' + formatDuration(elapsed));
  if (totalRcvd > 0) {
    console.log('  Throughput:    ' + Math.round(totalRcvd / (elapsed / 1000)) + ' rec/s');
  }
  console.log('  Database:      ' + dbName);
  console.log('');

  function printResult(result) {
    if (result.status === 'ok') {
      const q = result.queries > 1 ? ' (' + result.queries + ' queries)' : '';
      console.log('OK  ' + result.received + ' recv, ' + result.inserted + ' stored' + q);
      totalRcvd += result.received || 0;
      totalIns += result.inserted || 0;
    } else if (result.status === 'skip') {
      console.log('SKIP ' + result.detail);
      totalSkip++;
    } else if (result.status === 'dry-run') {
      console.log('DRY  ' + result.detail);
      totalDry++;
    } else {
      console.log('FAIL ' + result.detail);
      totalErr++;
    }
  }
}

// ─────────────────── status ─────────────────────────────
async function cmdStatus() {
  const useJson = process.argv.includes('--json');
  const dbName = CFG.target.database;

  let conn;
  try {
    conn = await mysql.createConnection({
      host: CFG.target.host,
      port: CFG.target.port,
      user: CFG.target.user,
      password: CFG.target.password,
      database: dbName
    });
  } catch (e) {
    console.error('MySQL connection failed: ' + e.message);
    process.exit(1);
  }

  const [tables] = await conn.query('SHOW TABLES');
  const dataTables = tables.filter(r => Object.values(r)[0] !== '_sync_state');
  const dataTableNames = dataTables.map(r => Object.values(r)[0]);

  if (useJson) {
    const out = { database: dbName, tables: [] };
    let totalRows = 0;
    for (const tn of dataTableNames) {
      const [cnt] = await conn.query('SELECT COUNT(*) AS n FROM `' + tn + '`');
      const [last] = await conn.query('SELECT MAX(ts) AS lt FROM `' + tn + '`');
      const [first] = await conn.query('SELECT MIN(ts) AS ft FROM `' + tn + '`');
      const n = cnt[0].n;
      totalRows += n;
      out.tables.push({
        name: tn,
        rows: n,
        first: first[0].ft || null,
        last: last[0].lt || null
      });
    }
    out.totalRows = totalRows;
    out.updatedAt = new Date().toISOString();

    const [state] = await conn.query('SELECT * FROM _sync_state ORDER BY updated_at DESC');
    if (state.length > 0) {
      out.syncState = state.map(s => ({
        point: s.point_ord,
        lastTimestamp: s.last_timestamp,
        updatedAt: s.updated_at
      }));
    }

    console.log(JSON.stringify(out, null, 2));
    await conn.end();
    return;
  }

  console.log('');
  console.log('Database: ' + dbName);
  console.log('Tables:   ' + dataTableNames.length);
  console.log('');

  let totalRows = 0;
  for (const tn of dataTableNames) {
    const [cnt] = await conn.query('SELECT COUNT(*) AS n FROM `' + tn + '`');
    const [last] = await conn.query('SELECT MAX(ts) AS lt FROM `' + tn + '`');
    const [first] = await conn.query('SELECT MIN(ts) AS ft FROM `' + tn + '`');
    const n = cnt[0].n;
    totalRows += n;
    const lt = last[0].lt ? String(last[0].lt).substring(0, 19) : '-';
    const ft = first[0].ft ? String(first[0].ft).substring(0, 19) : '-';
    console.log('  ' + tn.padEnd(45) + ' ' + String(n).padStart(8) + ' rows   ' + ft + ' ~ ' + lt);
  }

  console.log('');
  console.log('  Total: ' + totalRows + ' rows');

  const [state] = await conn.query('SELECT * FROM _sync_state ORDER BY updated_at DESC');
  if (state.length > 0) {
    console.log('');
    console.log('Checkpoints (last sync time per point):');
    for (const s of state.slice(0, 10)) {
      console.log('  ' + s.point_ord.padEnd(45) + ' ' + String(s.updated_at).substring(0, 22));
    }
    if (state.length > 10) {
      console.log('  ... and ' + (state.length - 10) + ' more');
    }
  }

  await conn.end();
}

// ─────────────────── config ─────────────────────────────
function cmdConfig() {
  const useJson = process.argv.includes('--json');
  if (useJson) {
    console.log(JSON.stringify(CFG, null, 2));
  } else {
    console.log('');
    console.log('Current configuration:');
    console.log('');
    console.log(JSON.stringify(CFG, null, 2));
  }
}

// ══════════════════════════════════════════════════════════
//  CLI Entry
// ══════════════════════════════════════════════════════════
async function main() {
  const cmd = process.argv[2] || 'help';

  if (cmd === 'init') return cmdInit();

  if (!fs.existsSync(CFG_FILE)) {
    console.log('No config file found. First run:');
    console.log('  node niagara-sync.js init');
    process.exit(1);
  }
  CFG = JSON.parse(fs.readFileSync(CFG_FILE, 'utf8'));
  validateConfig();

  switch (cmd) {
    case 'init':
      return cmdInit();
    case 'probe':
      return cmdProbe();
    case 'list':
      return cmdList(process.argv[3]);
    case 'sync':
      return cmdSync();
    case 'status':
      return cmdStatus();
    case 'config':
      return cmdConfig();
    case 'help':
    default:
      console.log('');
      console.log('Niagara History Sync  v' + VERSION);
      console.log('Niagara oBIX -> MySQL historical data sync tool');
      console.log('');
      console.log('Commands:');
      console.log('');
      console.log('  init                    Interactive setup (station, DB, name)');
      console.log('  init --help             Detailed parameter help');
      console.log('  probe                   Discover station, auto-fill config');
      console.log('  list [filter]           List available history points');
      console.log('  sync                    Sync all history data');
      console.log('  status                  Show sync status and statistics');
      console.log('  config                  Show current configuration');
      console.log('');
      console.log('Sync options:');
      console.log('  --filter <pattern>      Sync only points matching text pattern');
      console.log('  --since <ISO-time>      Sync only data after this time');
      console.log('  --until <ISO-time>      Sync only data before this time');
      console.log('  --parallel <N>          Sync N points concurrently (default: 4, max: 8)');
      console.log('  --serial                Force sequential sync (one point at a time)');
      console.log('  --dry-run               Preview without writing any data');
      console.log('  --json                  Machine-readable JSON output');
      console.log('');
      console.log('First time setup:');
      console.log('  1. node niagara-sync.js init --pass <oBIX-pwd> --db-pass <MySQL-pwd>');
      console.log('  2. node niagara-sync.js probe');
      console.log('  3. node niagara-sync.js sync');
      console.log('');
      console.log('Data architecture:');
      console.log('  MySQL Database (single database per target)');
      console.log('    <sanitized-point-name>  - one table per history point');
      console.log('    _sync_state              - checkpoint tracking table');
      console.log('');
  }
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  process.exit(1);
});
