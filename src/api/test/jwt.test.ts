import { afterAll, beforeAll, describe, it } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import type http from "node:http";
import jwt from "jsonwebtoken";
import request from "supertest";
import { run } from "./runQuirrel";

const { privateKey, publicKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicKeyEncoding: { type: "spki", format: "pem" },
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
});

describe("JWT Authentication", () => {
  let quirrel: http.Server;
  let teardown: () => Promise<void>;

  beforeAll(async () => {
    const res = await run("Mock", {
      passphrases: ["root"],
      jwtPublicKey: publicKey,
    });
    quirrel = res.server;
    teardown = res.teardown;
  });

  afterAll(async () => {
    await teardown();
  });

  describe("when using a freshly signed token", () => {
    it("allows & increments usage", async () => {
      const validToken = jwt.sign({}, Buffer.from(privateKey), {
        subject: "jason web token",
        algorithm: "RS512",
      });

      await request(quirrel)
        .get("/queues")
        .auth(validToken, { type: "bearer" })
        .expect(200);

      await request(quirrel)
        .delete("/usage")
        .auth("ignored", "root")
        .expect(200, { "jason web token": 1 });
    });
  });

  describe("when using an invalid token", () => {
    it("blocks", async () => {
      const invalidToken = jwt.sign({}, Buffer.from(privateKey), {
        subject: "jason web token",
        notBefore: "1d",
        algorithm: "RS512",
      });

      await request(quirrel)
        .get("/queues")
        .auth(invalidToken, { type: "bearer" })
        .expect(401);
    });
  });
});
