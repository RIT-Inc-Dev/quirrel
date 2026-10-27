import { afterEach, beforeEach } from "bun:test";

// bun runs every test file in one process, so restore env vars that a test
// (e.g. NODE_ENV=production) changed before the next one starts.
let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
});

afterEach(() => {
  for (const key of Object.keys(process.env)) {
    if (!(key in snapshot)) delete process.env[key];
  }
  Object.assign(process.env, snapshot);
});
