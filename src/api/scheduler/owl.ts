import type Owl from "@quirrel/owl";
import type { FastifyPluginCallback } from "fastify";
import fp from "fastify-plugin";
import { createOwl } from "../shared/owl";

declare module "fastify" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyInstance {
    owl: Owl<"every" | "cron">;
  }
}

const owlPlugin: FastifyPluginCallback = async (fastify, _opts, done) => {
  const owl = await createOwl(() => fastify.redisFactory(), fastify.logger);

  fastify.decorate("owl", owl);

  done();
};

export default (fp as any)(owlPlugin) as FastifyPluginCallback;
