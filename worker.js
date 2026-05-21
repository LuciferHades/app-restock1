const JSON_HEADERS = {
  'content-type': 'application/json; charset=utf-8',
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'POST, OPTIONS',
  'access-control-allow-headers': 'content-type, authorization'
};

const D1_BATCH_SIZE = 100;

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response('', { headers: JSON_HEADERS });
    if (request.method !== 'POST') return json({ success: false, error: 'POST only' }, 405);

    const auth = request.headers.get('authorization') || '';
    if (env.API_TOKEN && auth !== `Bearer ${env.API_TOKEN}`) {
      return json({ success: false, error: 'Unauthorized' }, 401);
    }

    try {
      const body = await request.json();
      const action = body.action;
      const payload = body.payload || {};

      if (action === 'health') return json({ success: true, provider: 'D1_WORKER' });
      if (action === 'setupSchema') return json(await setupSchema(env.DB, payload.tables || []));
      if (action === 'readRows') return json(await readRows(env.DB, payload));
      if (action === 'appendRows') return json(await appendRows(env.DB, payload));
      if (action === 'writeRows') return json(await writeRows(env.DB, payload));
      if (action === 'clearTable') return json(await clearTable(env.DB, payload.table));
      if (action === 'rowCount') return json(await rowCount(env.DB, payload.table));
      if (action === 'updateWhere') return json(await updateWhere(env.DB, payload));
      if (action === 'deleteWhere') return json(await deleteWhere(env.DB, payload));

      return json({ success: false, error: `Unknown action: ${action}` }, 400);
    } catch (error) {
      return json({ success: false, error: String(error && error.message ? error.message : error) }, 500);
    }
  }
};

async function setupSchema(db, tables) {
  for (const def of tables) {
    const table = ident(def.name);
    const headers = cleanHeaders(def.headers || []);
    const columns = headers.map(h => `${quoteIdent(h)} TEXT`).join(', ');
    await db.prepare(`CREATE TABLE IF NOT EXISTS ${quoteIdent(table)} (_id INTEGER PRIMARY KEY AUTOINCREMENT${columns ? ', ' + columns : ''})`).run();

    const existing = await db.prepare(`PRAGMA table_info(${quoteIdent(table)})`).all();
    const existingCols = new Set((existing.results || []).map(col => col.name));
    for (const header of headers) {
      if (!existingCols.has(header)) {
        await db.prepare(`ALTER TABLE ${quoteIdent(table)} ADD COLUMN ${quoteIdent(header)} TEXT`).run();
      }
    }

    for (const col of ['JobId', 'Location', 'ItemKey', 'SessionId', 'Status', 'SourceRowNum']) {
      if (headers.includes(col)) {
        await db.prepare(`CREATE INDEX IF NOT EXISTS ${quoteIdent(`idx_${table}_${col}`)} ON ${quoteIdent(table)} (${quoteIdent(col)})`).run();
      }
    }
  }
  return { success: true, tablesCreated: tables.length };
}

async function readRows(db, payload) {
  const table = ident(payload.table);
  const headers = cleanHeaders(payload.headers || []);
  const startRow = Number(payload.startRow || 1);
  const dataOffset = Math.max(0, startRow - 2);
  const cols = headers.map(quoteIdent).join(', ');
  const result = await db.prepare(`SELECT ${cols} FROM ${quoteIdent(table)} ORDER BY _id LIMIT 1000000 OFFSET ?`).bind(dataOffset).all();
  const rows = (result.results || []).map(obj => headers.map(h => obj[h] ?? ''));
  return { success: true, rows: startRow <= 1 ? [headers].concat(rows) : rows };
}

async function appendRows(db, payload) {
  const table = ident(payload.table);
  const headers = cleanHeaders(payload.headers || []);
  const rows = payload.rows || [];
  if (!rows.length) return { success: true, rowsAppended: 0 };

  const cols = headers.map(quoteIdent).join(', ');
  const marks = headers.map(() => '?').join(', ');
  const sql = `INSERT INTO ${quoteIdent(table)} (${cols}) VALUES (${marks})`;
  const statements = rows.map(row => db.prepare(sql).bind(...headers.map((_, i) => valueForD1(row[i]))));
  for (let i = 0; i < statements.length; i += D1_BATCH_SIZE) {
    await db.batch(statements.slice(i, i + D1_BATCH_SIZE));
  }
  return { success: true, rowsAppended: rows.length };
}

async function writeRows(db, payload) {
  const table = ident(payload.table);
  const startRow = Number(payload.startRow || 1);
  let rows = payload.rows || [];
  if (startRow <= 1 && rows.length) rows = rows.slice(1);
  await clearTable(db, table);
  return appendRows(db, { ...payload, rows });
}

async function clearTable(db, tableName) {
  const table = ident(tableName);
  await db.prepare(`DELETE FROM ${quoteIdent(table)}`).run();
  return { success: true };
}

async function rowCount(db, tableName) {
  const table = ident(tableName);
  const result = await db.prepare(`SELECT COUNT(*) AS count FROM ${quoteIdent(table)}`).first();
  return { success: true, rowCount: Number(result && result.count) || 0 };
}

async function updateWhere(db, payload) {
  const table = ident(payload.table);
  const values = payload.values || {};
  const keys = Object.keys(values).map(ident);
  if (!keys.length) return { success: true, rowsUpdated: 0 };

  const where = buildWhere(payload.filters || {}, payload.options || {});
  const setSql = keys.map(k => `${quoteIdent(k)} = ?`).join(', ');
  const params = keys.map(k => valueForD1(values[k])).concat(where.params);
  const result = await db.prepare(`UPDATE ${quoteIdent(table)} SET ${setSql}${where.sql}`).bind(...params).run();
  return { success: true, rowsUpdated: result.meta?.changes || 0 };
}

async function deleteWhere(db, payload) {
  const table = ident(payload.table);
  const where = buildWhere(payload.filters || {}, payload.options || {});
  const result = await db.prepare(`DELETE FROM ${quoteIdent(table)}${where.sql}`).bind(...where.params).run();
  return { success: true, deletedRows: result.meta?.changes || 0 };
}

function buildWhere(filters, options) {
  const clauses = [];
  const params = [];

  Object.keys(filters || {}).forEach(rawKey => {
    const key = ident(rawKey);
    clauses.push(`${quoteIdent(key)} = ?`);
    params.push(valueForD1(filters[rawKey]));
  });

  const between = options.between || {};
  Object.keys(between).forEach(rawKey => {
    const key = ident(rawKey);
    const range = between[rawKey] || [];
    clauses.push(`CAST(${quoteIdent(key)} AS REAL) BETWEEN ? AND ?`);
    params.push(Number(range[0]) || 0, Number(range[1]) || 0);
  });

  return {
    sql: clauses.length ? ` WHERE ${clauses.join(' AND ')}` : '',
    params
  };
}

function cleanHeaders(headers) {
  return headers.map(ident).filter(Boolean);
}

function ident(value) {
  const clean = String(value || '').replace(/[^A-Za-z0-9_]/g, '');
  if (!clean) throw new Error('Invalid identifier');
  return clean;
}

function quoteIdent(value) {
  return `"${ident(value)}"`;
}

function valueForD1(value) {
  if (value === undefined || value === null) return '';
  return String(value);
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}
