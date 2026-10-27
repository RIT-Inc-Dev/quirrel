// SSRF prevention hooks into Node's http.Agent#createConnection, which Bun's
// HTTP client doesn't call, so this runs the built server on Node.js
// (`bun run test:node`, after `bun run build`).
import assert from "node:assert/strict";
import http from "node:http";
import { createRequire } from "node:module";
import { after, before, test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

const require = createRequire(import.meta.url);
const { run } = require("../../dist/cjs/src/api/test/runQuirrel.js");

let target;
let targetUrl;
let lastRequestBody = null;

before(async () => {
  target = http.createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      lastRequestBody = body;
      res.end("OK");
    });
  });
  await new Promise((resolve) => target.listen(0, "127.0.0.1", resolve));
  targetUrl = `http://127.0.0.1:${target.address().port}/database/drop`;
});

after(() => {
  target.close();
});

async function enqueue(enableSSRFPrevention) {
  lastRequestBody = null;
  const quirrel = await run("Mock", { enableSSRFPrevention });
  try {
    const { port } = quirrel.server.address();
    const res = await fetch(
      `http://127.0.0.1:${port}/queues/${encodeURIComponent(targetUrl)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ body: "dropthebase" }),
      },
    );
    assert.equal(res.status, 201);
    await delay(500);
  } finally {
    await quirrel.teardown();
  }
}

test("blocks requests to private addresses when enabled", async () => {
  await enqueue(true);
  assert.equal(lastRequestBody, null);
});

test("delivers to private addresses when disabled", async () => {
  await enqueue(false);
  assert.equal(lastRequestBody, "dropthebase");
});
