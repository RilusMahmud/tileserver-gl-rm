'use strict';

import { expect } from 'chai';
import {
  extractAccessToken,
  extractHeaderApiKeys,
  apiKeyHeaders,
} from '../src/utils.js';
import * as counter from '../src/request_counter.js';

/**
 * Build a minimal Express-like request stub for the key extractors.
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

describe('extractAccessToken', function () {
  it('reads the access token from the ?key= query param', function () {
    expect(extractAccessToken(stubReq({ query: { key: 'q' } }))).to.equal('q');
  });

  it('ignores headers (the access token is the ?key= query param only)', function () {
    const req = stubReq({ headers: { 'x-api-key': 'h' } });
    expect(extractAccessToken(req)).to.equal(undefined);
  });

  it('returns undefined when no ?key= is present', function () {
    expect(extractAccessToken(stubReq())).to.equal(undefined);
  });

  it('takes the first value when ?key= is an array', function () {
    expect(
      extractAccessToken(stubReq({ query: { key: ['a', 'b'] } })),
    ).to.equal('a');
  });
});

describe('extractHeaderApiKeys', function () {
  it('reads each supported header with the header name as source', function () {
    for (const header of apiKeyHeaders) {
      const req = stubReq({ headers: { [header]: 'v' } });
      expect(extractHeaderApiKeys(req), header).to.deep.equal([
        { key: 'v', source: header },
      ]);
    }
  });

  it('matches header names case-insensitively and reports the configured name', function () {
    expect(
      extractHeaderApiKeys(stubReq({ headers: { 'X-API-KEY': 'v' } })),
    ).to.deep.equal([{ key: 'v', source: 'x-api-key' }]);
  });

  it('ignores the ?key= query param (headers only)', function () {
    expect(
      extractHeaderApiKeys(stubReq({ query: { key: 'q' } })),
    ).to.deep.equal([]);
  });

  it('returns an empty array when no supported header is present', function () {
    expect(extractHeaderApiKeys(stubReq())).to.deep.equal([]);
  });

  it('returns every present header without validation', function () {
    const req = stubReq({ headers: { 'x-api-key': 'A', 'x-apikey': 'B' } });
    expect(extractHeaderApiKeys(req)).to.deep.equal([
      { key: 'A', source: 'x-api-key' },
      { key: 'B', source: 'x-apikey' },
    ]);
  });

  it('skips header values longer than the max length (unusable as _id)', function () {
    const big = 'z'.repeat(513);
    expect(
      extractHeaderApiKeys(stubReq({ headers: { 'x-apikey': big } })),
    ).to.deep.equal([]);
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
