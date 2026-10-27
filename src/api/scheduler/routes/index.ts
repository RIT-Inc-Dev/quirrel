import * as path from "node:path";
import fastifyStatic from "@fastify/static";
import type { FastifyPluginCallback } from "fastify";

const alternativeEntries = ["/", "/cron", "/pending", "/activity-log"];

const index: FastifyPluginCallback = (fastify, opts, done) => {
  fastify.register(fastifyStatic, {
    root: path.join(__dirname, "../../../../../ui"),
    wildcard: false,
    redirect: true,
    schemaHide: true,
    setHeaders(reply) {
      const auth = fastify.adminBasedAuthEnabled;
      const config = [auth];
      reply.header("Set-Cookie", `Quirrel-UI-Config=${config.join("-")}`);
    },
  });

  alternativeEntries.forEach((entryPoint) => {
    fastify.get(
      entryPoint,
      { schema: { hide: true } },
      async (request, reply) => {
        return reply.sendFile("fastify.html");
      },
    );
  });

  done();
};

export default index;
