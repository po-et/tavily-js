const assert = require('node:assert/strict');
const http = require('node:http');
const { test } = require('node:test');
const { tavily } = require('../dist/index.js');

// All requests stay on this synthetic local endpoint, including environments
// which configure Tavily-specific or standard HTTP proxies.
process.env.TAVILY_HTTP_PROXY = '';
process.env.TAVILY_HTTPS_PROXY = '';
process.env.NO_PROXY = '127.0.0.1,localhost';

function bounded(promise, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} did not settle`)), 1000);
    promise.then(value => { clearTimeout(timer); resolve(value); }, error => { clearTimeout(timer); reject(error); });
  });
}

async function endpoint(t) {
  let respond;
  let close;
  const received = new Promise(resolve => { respond = resolve; });
  const closed = new Promise(resolve => { close = resolve; });
  const sockets = new Set();
  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    assert.equal(req.url, '/research');
    assert.equal(req.method, 'POST');
    assert.equal(JSON.parse(Buffer.concat(chunks)).stream, true);
    res.once('close', close);
    res.writeHead(200, { 'content-type': 'text/event-stream', connection: 'close' });
    res.flushHeaders();
    respond(res);
  });
  server.on('connection', socket => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
  });
  const client = tavily({ apiKey: 'synthetic-research-key', apiBaseURL: `http://127.0.0.1:${server.address().port}` });
  const stream = await client.research('Synthetic research question', { stream: true });
  assert.equal(stream[Symbol.asyncIterator](), stream);
  return { stream, response: await received, closed };
}

test('return before the first read closes the HTTP response', { timeout: 4000 }, async t => {
  const { stream, closed } = await endpoint(t);
  assert.deepEqual(await stream.return(), { done: true, value: undefined });
  await bounded(closed, 'unread response close');
  assert.deepEqual(await stream.return(), { done: true, value: undefined });
});

test('throw before the first read closes the HTTP response and retains the error', { timeout: 4000 }, async t => {
  const { stream, closed } = await endpoint(t);
  const reason = new Error('stop synthetic research');
  await assert.rejects(stream.throw(reason), error => error === reason);
  await bounded(closed, 'unread response close');
});

test('normal completion yields every byte and closes the response', { timeout: 4000 }, async t => {
  const { stream, response, closed } = await endpoint(t);
  response.write('data: first\n\n');
  response.end('data: second\n\n');
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  assert.equal(Buffer.concat(chunks).toString(), 'data: first\n\ndata: second\n\n');
  await bounded(closed, 'completed response close');
  assert.deepEqual(await stream.return(), { done: true, value: undefined });
});

test('breaking after a chunk closes a response that has not ended', { timeout: 4000 }, async t => {
  const { stream, response, closed } = await endpoint(t);
  response.write('data: first\n\n');
  for await (const chunk of stream) {
    assert.equal(chunk.toString(), 'data: first\n\n');
    break;
  }
  await bounded(closed, 'early-break response close');
});

test('a transport failure still rejects the reader', { timeout: 4000 }, async t => {
  const { stream, response, closed } = await endpoint(t);
  response.write('data: first\n\n');
  assert.equal((await stream.next()).value.toString(), 'data: first\n\n');
  response.destroy();
  await assert.rejects(stream.next(), error => error instanceof Error);
  await bounded(closed, 'failed response close');
});

test('return interrupts an active read instead of waiting for another chunk', { timeout: 4000 }, async t => {
  const { stream, closed } = await endpoint(t);
  const reading = stream.next().then(value => ({ value }), error => ({ error }));
  const returning = stream.return();
  assert.deepEqual(await bounded(returning, 'pending read return'), { done: true, value: undefined });
  const result = await bounded(reading, 'pending read');
  assert.ok(result.error instanceof Error);
  await bounded(closed, 'cancelled response close');
});

test('throw interrupts an active read and retains the supplied error', { timeout: 4000 }, async t => {
  const { stream, closed } = await endpoint(t);
  const reading = stream.next().then(value => ({ value }), error => ({ error }));
  const reason = new Error('stop pending synthetic research');
  const throwing = stream.throw(reason);
  await assert.rejects(bounded(throwing, 'pending read throw'), error => error === reason);
  const result = await bounded(reading, 'pending read');
  assert.ok(result.error instanceof Error);
  await bounded(closed, 'cancelled response close');
});
