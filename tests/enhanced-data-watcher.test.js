import { test, describe } from 'node:test';
import assert from 'node:assert/strict';

import EnhancedDataWatcher from '../enhanced-data-watcher.js';

// The watcher only needs a pool that supports .on() until start() is called,
// so a stub keeps these tests free of any database dependency.
const stubPool = { on() {} };
const watcher = new EnhancedDataWatcher(stubPool, '/tmp/watch-dir');

describe('parseFilename', () => {
  test('parses a valid enhanced-analysis filename', () => {
    const result = watcher.parseFilename('BTC_enhanced_analysis_20260728_213000.json');
    assert.equal(result.asset, 'BTC');
    assert.equal(result.timestamp, '20260728_213000');
    assert.equal(result.filename, 'BTC_enhanced_analysis_20260728_213000.json');
    assert.deepEqual(result.datetime, new Date('2026-07-28T21:30:00Z'));
  });

  test('parses multi-letter asset symbols', () => {
    const result = watcher.parseFilename('HYPE_enhanced_analysis_20260101_000000.json');
    assert.equal(result.asset, 'HYPE');
  });

  for (const [label, name] of [
    ['lowercase asset', 'btc_enhanced_analysis_20260728_213000.json'],
    ['missing timestamp', 'BTC_enhanced_analysis_.json'],
    ['short timestamp', 'BTC_enhanced_analysis_2026_2130.json'],
    ['wrong extension', 'BTC_enhanced_analysis_20260728_213000.txt'],
    ['different prefix', 'BTC_raw_analysis_20260728_213000.json'],
    ['empty string', ''],
  ]) {
    test(`rejects ${label}`, () => {
      assert.equal(watcher.parseFilename(name), null);
    });
  }
});

describe('validateJsonStructure', () => {
  const validPayload = {
    current_price: 63908.5,
    clusters: { long_clusters: [{ price: 63000 }], short_clusters: [] },
  };

  test('accepts a payload with price and long clusters', () => {
    assert.ok(watcher.validateJsonStructure(validPayload));
  });

  test('accepts a payload with only short clusters', () => {
    const payload = {
      current_price: 100,
      clusters: { short_clusters: [{ price: 110 }] },
    };
    assert.ok(watcher.validateJsonStructure(payload));
  });

  for (const [label, payload] of [
    ['null data', null],
    ['undefined data', undefined],
    ['missing price', { clusters: validPayload.clusters }],
    ['string price', { current_price: '63908.5', clusters: validPayload.clusters }],
    ['missing clusters', { current_price: 63908.5 }],
    ['empty clusters object', { current_price: 63908.5, clusters: {} }],
  ]) {
    test(`rejects ${label}`, () => {
      assert.ok(!watcher.validateJsonStructure(payload));
    });
  }
});

describe('sanitizeNumericValue', () => {
  test('passes through normal numbers', () => {
    assert.equal(watcher.sanitizeNumericValue(42.5, 'price'), 42.5);
    assert.equal(watcher.sanitizeNumericValue(-10, 'delta'), -10);
    assert.equal(watcher.sanitizeNumericValue(0, 'zero'), 0);
  });

  test('parses numeric strings', () => {
    assert.equal(watcher.sanitizeNumericValue('63908.5', 'price'), 63908.5);
  });

  test('returns null for null, undefined, and non-numeric input', () => {
    assert.equal(watcher.sanitizeNumericValue(null, 'x'), null);
    assert.equal(watcher.sanitizeNumericValue(undefined, 'x'), null);
    assert.equal(watcher.sanitizeNumericValue('not-a-number', 'x'), null);
  });

  test('caps values that exceed the database limit, preserving sign', () => {
    const max = 1000;
    assert.equal(watcher.sanitizeNumericValue(5000, 'big', max), 1000);
    assert.equal(watcher.sanitizeNumericValue(-5000, 'big-negative', max), -1000);
  });

  test('does not cap values at or below the limit', () => {
    assert.equal(watcher.sanitizeNumericValue(1000, 'edge', 1000), 1000);
  });
});
