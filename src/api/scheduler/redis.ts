import type { FastifyPluginCallback } from "fastify";
import fp from "fastify-plugin";
import type { Redis } from "ioredis";

declare module "fastify" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyInstance {
    redis: Redis;
    redisFactory: () => Redis;
  }
}

type RedisPluginOpts = {
  redisFactory: () => Redis;
};

const redisPlugin: FastifyPluginCallback<RedisPluginOpts> = (
  fastify,
  { redisFactory },
  done,
) => {
  const client = redisFactory();
  fastify.decorate("redis", client);
  fastify.decorate("redisFactory", redisFactory);

  fastify.addHook("onClose", async () => {
    await client.quit();
  });

  client.addListener("ready", done);
};

export default (fp as any)(
  redisPlugin,
) as FastifyPluginCallback<RedisPluginOpts>;
