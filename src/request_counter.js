// src/request_counter.js
// Per-API-key request counting with a durable MongoDB sink.
//
// One document per (key, source, UTC day) so usage is directly searchable per
// API key, per source, and date-wise. The request path only ever touches the
// in-memory `buffer` (an O(1) Map increment). All MongoDB I/O is deferred to a
// periodic flush timer, so counting never adds latency to a request or response.

import { MongoClient } from 'mongodb';

let client = null;
let collection = null;
let buffer = new Map();
let timer = null;

// Current UTC day, cached until midnight so increment() doesn't format a date
// per request.
let dayStr = null;
let dayEndMs = 0;

/**
 * Returns the current UTC day as `YYYY-MM-DD`, recomputed only when the clock
 * crosses into a new day.
 * @returns {string} - The current UTC day.
 */
function currentDay() {
  const now = Date.now();
  if (now >= dayEndMs) {
    const d = new Date(now);
    dayStr = d.toISOString().slice(0, 10);
    dayEndMs = Date.UTC(
      d.getUTCFullYear(),
      d.getUTCMonth(),
      d.getUTCDate() + 1,
    );
  }
  return dayStr;
}

/**
 * Initialise the counter: connect to MongoDB and start the periodic flush timer.
 * @param {object} opts - Initialisation options.
 * @param {string} [opts.uri] - MongoDB connection string (required unless `collection` is provided).
 * @param {string} [opts.dbName] - Database name.
 * @param {string} [opts.collectionName] - Collection name.
 * @param {number} [opts.flushIntervalMs] - Flush interval in milliseconds.
 * @param {object} [opts.collection] - Pre-built collection to use instead of connecting (for tests).
 * @returns {Promise<void>} - Resolves once connected and the flush timer is running.
 */
export async function init({
  uri,
  dbName,
  collectionName,
  flushIntervalMs = 10000,
  collection: injectedCollection,
} = {}) {
  if (injectedCollection) {
    collection = injectedCollection;
  } else {
    // The driver connects lazily on first use, so startup is never blocked by an
    // unreachable MongoDB; connection errors surface at flush time and are handled.
    client = new MongoClient(uri);
    collection = client.db(dbName).collection(collectionName);
    // Secondary indexes for the query patterns the data exists to serve:
    // per-key date ranges and cross-key daily reports. createIndexes is
    // idempotent; failure (e.g. MongoDB unreachable) must not block startup.
    collection
      .createIndexes([{ key: { key: 1, date: 1 } }, { key: { date: 1 } }])
      .catch((err) =>
        console.warn(`[usage] index creation failed: ${err.message}`),
      );
  }
  timer = setInterval(() => {
    flush().catch((err) =>
      console.warn(`[usage] flush failed: ${err.message}`),
    );
  }, flushIntervalMs);
  timer.unref?.(); // don't keep the process alive just for flushing
}

/**
 * Record one request for the given API key (in-memory, non-blocking). Counts
 * accumulate per (key, source, UTC day); the day is captured at increment time
 * so requests near midnight attribute to the correct day regardless of when
 * the flush runs.
 * @param {string} key - The API key.
 * @param {string} [source] - Where the key was read from (query param or header name).
 * @returns {void}
 */
export function increment(key, source) {
  const day = currentDay();
  const id = `${day}|${source}|${key}`;
  const entry = buffer.get(id);
  if (entry) {
    entry.count += 1;
  } else {
    buffer.set(id, { key, source, day, count: 1 });
  }
}

/**
 * Flush the buffered counts to MongoDB as a single batched upsert. Each buffer
 * entry maps to one daily-bucket document whose `_id` is the deterministic
 * composite `${day}|${source}|${key}`, making concurrent upserts from multiple
 * server instances race-free without a unique compound index.
 * On failure the counts are merged back into the buffer so they are not lost.
 * @returns {Promise<void>} - Resolves once the batch has been written (or on no-op).
 */
export async function flush() {
  if (!collection || buffer.size === 0) {
    return;
  }
  const pending = buffer;
  buffer = new Map(); // swap-and-drain so new increments accumulate independently
  const ops = [];
  for (const [id, { key, source, day, count }] of pending) {
    ops.push({
      updateOne: {
        filter: { _id: id },
        update: {
          $inc: { count },
          $set: { lastSeen: new Date() },
          $setOnInsert: { key, source, date: new Date(`${day}T00:00:00Z`) },
        },
        upsert: true,
      },
    });
  }
  try {
    await collection.bulkWrite(ops, { ordered: false });
  } catch (err) {
    for (const [id, entry] of pending) {
      const existing = buffer.get(id);
      if (existing) {
        existing.count += entry.count;
      } else {
        buffer.set(id, entry);
      }
    }
    console.warn(`[usage] flush failed: ${err.message}`);
  }
}

/**
 * Stop the flush timer, flush any remaining counts, and close the connection.
 * @returns {Promise<void>} - Resolves once the final flush and close complete.
 */
export async function close() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  await flush();
  if (client) {
    await client.close();
  }
  client = null;
  collection = null;
}
