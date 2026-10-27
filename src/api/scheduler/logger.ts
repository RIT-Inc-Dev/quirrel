import type { FastifyPluginCallback } from "fastify";
import fp from "fastify-plugin";
import type { Logger } from "../shared/logger";

declare module "fastify" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyInstance {
    logger?: Logger;
  }
}

type LoggerPluginOpts = {
  logger?: Logger;
};

const loggerPlugin: FastifyPluginCallback<LoggerPluginOpts> = (
  fastify,
  { logger },
  done,
) => {
  fastify.decorate("logger", logger);

  done();
};

export default (fp as any)(
  loggerPlugin,
) as FastifyPluginCallback<LoggerPluginOpts>;
