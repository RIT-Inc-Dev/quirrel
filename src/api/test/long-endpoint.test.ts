import { expect, test } from "bun:test";
import request from "supertest";
import { run } from "./runQuirrel";

// Endpoints are URL-encoded into a single path param, which easily exceeds
// fastify's default maxParamLength of 100
test("routes endpoints longer than 100 characters", async () => {
  const { server, teardown } = await run("Mock");
  try {
    const endpoint = encodeURIComponent(
      `https://example.com/api/${"queue/".repeat(30)}`,
    );
    expect(endpoint.length).toBeGreaterThan(100);

    const res = await request(server).get(`/queues/${endpoint}/missing-id`);
    expect(res.status).toBe(404);
  } finally {
    await teardown();
  }
});
