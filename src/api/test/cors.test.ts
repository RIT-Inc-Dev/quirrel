import { expect, test } from "bun:test";
import request from "supertest";
import { run } from "./runQuirrel";

// The development UI runs on another origin and deletes / invokes jobs
test.each(["DELETE", "PUT"])("CORS preflight allows %s", async (method) => {
  const { server, teardown } = await run("Mock");
  try {
    const res = await request(server)
      .options("/queues/endpoint/id")
      .set("Origin", "http://localhost:1234")
      .set("Access-Control-Request-Method", method);
    expect(res.headers["access-control-allow-methods"]).toContain(method);
  } finally {
    await teardown();
  }
});
