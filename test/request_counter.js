'use strict';

import { expect } from 'chai';
import { extractApiKey, apiKeyHeaders } from '../src/utils.js';
import * as counter from '../src/request_counter.js';

/**
 * Build a minimal Express-like request stub for extractApiKey.
 * @param {object} opts - Stub options.
 * @param {object} [opts.query] - The req.query object.
 * @param {object} [opts.headers] - Header name -> value map (matched case-insensitively).
 * @returns {object} - A stub request with query and a case-insensitive get().
 */
function stubReq({ query = {}, headers = {} } = {}) {
  const lower = {};
  for (const [name, value] of Object.entries(headers)) {
    lower[name.toLowerCase()] = value;
  }
  return { query, get: (name) => lower[name.toLowerCase()] };
}

describe('extractApiKey', function () {
  it('prefers the ?key= query param over headers', function () {
    const req = stubReq({ query: { key: 'q' }, headers: { 'x-api-key': 'h' } });
    expect(extractApiKey(req)).to.deep.equal({ key: 'q', source: 'key' });
  });

  it('reads the key from each supported header when no query key', function () {
    for (const header of apiKeyHeaders) {
      const req = stubReq({ headers: { [header]: 'v' } });
      expect(extractApiKey(req), header).to.deep.equal({
        key: 'v',
        source: header,
      });
    }
  });

  it('reads the key from each supported name as a query param', function () {
    for (const name of apiKeyHeaders) {
      const req = stubReq({ query: { [name]: 'v' } });
      expect(extractApiKey(req), name).to.deep.equal({
        key: 'v',
        source: name,
      });
    }
  });

  it('prefers a query param over a header of a different supported name', function () {
    const req = stubReq({
      query: { apikey: 'q' },
      headers: { 'x-api-key': 'h' },
    });
    expect(extractApiKey(req)).to.deep.equal({ key: 'q', source: 'apikey' });
  });

  it('matches header names case-insensitively and reports the configured name', function () {
    expect(
      extractApiKey(stubReq({ headers: { 'X-API-KEY': 'v' } })),
    ).to.deep.equal({ key: 'v', source: 'x-api-key' });
  });

  it('returns undefined when no key is present', function () {
    expect(extractApiKey(stubReq())).to.equal(undefined);
  });

  it('takes the first value when the query key is an array', function () {
    expect(
      extractApiKey(stubReq({ query: { key: ['a', 'b'] } })),
    ).to.deep.equal({ key: 'a', source: 'key' });
  });
});

describe('request_counter', function () {
  afterEach(async function () {
    await counter.close();
  });

  it('buffers increments and flushes them as batched upserts', async function () {
    const ops = [];
    const collection = {
      bulkWrite: async (o) => {
        ops.push(...o);
      },
    };
    await counter.init({ collection, flushIntervalMs: 1e9 });
    counter.increment('a', 'key');
    counter.increment('a', 'x-apikey');
    counter.increment('b', 'key');
    await counter.flush();

    const a = ops.find((o) => o.updateOne.filter._id === 'a');
    const b = ops.find((o) => o.updateOne.filter._id === 'b');
    expect(a.updateOne.update.$inc.count).to.equal(2);
    expect(a.updateOne.update.$set.source).to.equal('x-apikey'); // latest wins
    expect(a.updateOne.upsert).to.equal(true);
    expect(b.updateOne.update.$inc.count).to.equal(1);
    expect(b.updateOne.update.$set.source).to.equal('key');
  });

  it('does not call the collection when the buffer is empty', async function () {
    let called = false;
    const collection = {
      bulkWrite: async () => {
        called = true;
      },
    };
    await counter.init({ collection, flushIntervalMs: 1e9 });
    await counter.flush();
    expect(called).to.equal(false);
  });

  it('re-buffers counts when a flush fails so they are not lost', async function () {
    const ops = [];
    let failNext = true;
    const collection = {
      bulkWrite: async (o) => {
        if (failNext) {
          failNext = false;
          throw new Error('boom');
        }
        ops.push(...o);
      },
    };
    await counter.init({ collection, flushIntervalMs: 1e9 });
    counter.increment('x');
    await counter.flush(); // fails -> re-buffers
    await counter.flush(); // succeeds

    const x = ops.find((o) => o.updateOne.filter._id === 'x');
    expect(x.updateOne.update.$inc.count).to.equal(1);
  });
});
