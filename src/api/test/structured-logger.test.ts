import { expect, test } from "bun:test";
import request from "supertest";
import { run } from "./runQuirrel";

test("starts with the structured logger (Docker default)", async () => {
  process.env.LOG_LEVEL = "silent";
  const { server, teardown } = await run("Mock", {
    logger: "structured",
  });
  try {
    const res = await request(server).get("/health");
    expect(res.status).toBe(200);
  } finally {
    await teardown();
  }
});
