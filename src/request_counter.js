// src/request_counter.js
// Per-API-key request counting with a durable MongoDB sink.
//
// The request path only ever touches the in-memory `buffer` (an O(1) Map
// increment). All MongoDB I/O is deferred to a periodic flush timer, so counting
// never adds latency to a request or response.

import { MongoClient } from 'mongodb';

let client = null;
let collection = null;
let buffer = new Map();
let timer = null;

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
  }
  timer = setInterval(() => {
    flush().catch((err) =>
      console.warn(`[usage] flush failed: ${err.message}`),
    );
  }, flushIntervalMs);
  timer.unref?.(); // don't keep the process alive just for flushing
}

/**
 * Record one request for the given API key (in-memory, non-blocking).
 * @param {string} key - The API key.
 * @param {string} [source] - Where the key was read from (query param or header name).
 * @returns {void}
 */
export function increment(key, source) {
  const entry = buffer.get(key);
  if (entry) {
    entry.count += 1;
    if (source !== undefined) {
      entry.source = source; // latest source wins
    }
  } else {
    buffer.set(key, { count: 1, source });
  }
}

/**
 * Flush the buffered counts to MongoDB as a single batched upsert.
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
  for (const [key, { count, source }] of pending) {
    const update = { $inc: { count }, $set: { lastSeen: new Date() } };
    if (source !== undefined) {
      update.$set.source = source;
    }
    ops.push({
      updateOne: {
        filter: { _id: key },
        update,
        upsert: true,
      },
    });
  }
  try {
    await collection.bulkWrite(ops, { ordered: false });
  } catch (err) {
    for (const [key, { count, source }] of pending) {
      const entry = buffer.get(key);
      if (entry) {
        entry.count += count; // increments during the flush carry a newer source
      } else {
        buffer.set(key, { count, source });
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
