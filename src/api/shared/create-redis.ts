import fs from "node:fs";
import IORedis, { type RedisOptions as _RedisOptions } from "ioredis";
import IORedisMock from "ioredis-mock";

type RedisOptions = _RedisOptions & {
  tlsCa?: { path?: string; base64?: string };
};

function getRandomPort() {
  return 3000 + Math.floor(Math.random() * 1000);
}

export function createRedisFactory(
  redisUrl?: string,
  options: RedisOptions = {},
): () => IORedis {
  if (!redisUrl) {
    let redis: IORedisMock | undefined;
    return () => {
      if (!redis) {
        redis = new IORedisMock({
          port: getRandomPort(),
        });
        return redis;
      }

      return redis.duplicate() as any;
    };
  }

  let redis: IORedis | undefined;
  return () => {
    if (!redis) {
      if (options.tlsCa?.path && fs.existsSync(options.tlsCa.path)) {
        options.tls = {
          ...options.tls,
          ca: fs.readFileSync(options.tlsCa.path, "utf8"),
        };
      }
      if (options.tlsCa?.base64) {
        options.tls = {
          ...options.tls,
          ca: Buffer.from(options.tlsCa?.base64, "base64").toString("ascii"),
        };
      }
      redis = new IORedis(redisUrl, options);
      return redis;
    }

    return redis.duplicate();
  };
}
