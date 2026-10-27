import type Owl from "@quirrel/owl";
import type { FastifyPluginAsync } from "fastify";
import fp from "fastify-plugin";
import { createOwl } from "../shared/owl";

declare module "fastify" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyInstance {
    owl: Owl<"every" | "cron">;
  }
}

const owlPlugin: FastifyPluginAsync = async (fastify) => {
  const owl = await createOwl(() => fastify.redisFactory(), fastify.logger);

  fastify.decorate("owl", owl);
};

export default fp(owlPlugin);
