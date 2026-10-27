import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import delay from "delay";
import fastify, { type FastifyInstance } from "fastify";
import request from "supertest";
import { getAddress } from "../../client/test/util";
import { run } from "./runQuirrel";

describe("SSRF Prevention", () => {
  let server: FastifyInstance;
  let teardown: () => Promise<void>;

  let lastRequestBody: any = null;

  beforeAll(async () => {
    server = fastify();
    server.post("/database/drop", (request, reply) => {
      lastRequestBody = request.body;
      reply.status(200).send("OK");
    });

    await server.listen({
      port: 0,
    });
  });

  afterAll(async () => {
    await server.close();
    await teardown();
  });

  function testWithSSR(mode: "enabled" | "disabled") {
    // The filter hooks into Node's http.Agent#createConnection, which Bun's
    // HTTP client doesn't call. test/node/ssrf-prevention.test.mjs covers this
    // case on Node.js; the IP check itself is in ssrf-filter.test.ts.
    const unsupported = mode === "enabled" && "Bun" in globalThis;
    test.skipIf(unsupported)(`ssrf prevention ${mode}`, async () => {
      const res = await run("Mock", {
        enableSSRFPrevention: mode === "enabled",
      });
      const quirrel = res.server;
      teardown = res.teardown;

      await request(quirrel)
        .post(
          "/queues/" +
            encodeURIComponent(`${getAddress(server.server)}/database/drop`),
        )
        .send({ body: "dropthebase" })
        .expect(201);

      await delay(500);

      if (mode === "enabled") {
        expect(lastRequestBody).toBeNull();
      } else {
        expect(lastRequestBody).toEqual("dropthebase");
      }
    });
  }

  testWithSSR("enabled");
  testWithSSR("disabled");
});
