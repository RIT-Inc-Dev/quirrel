import type { AddressInfo } from "node:net";
import cors from "@fastify/cors";
import fastifyRateLimit from "@fastify/rate-limit";
import swagger from "@fastify/swagger";
import swaggerUi from "@fastify/swagger-ui";
import fastify from "fastify";
import type { Redis } from "ioredis";
import pack from "../../../package.json";
import type { Logger } from "../shared/logger";
import { StructuredLogger } from "../shared/structured-logger";
import basicAuthPlugin from "./basic-auth";
import { jobsRepoPlugin } from "./jobs-repo";
import loggerPlugin from "./logger";
import owlPlugin from "./owl";
import posthogPlugin from "./posthog";
import redisPlugin from "./redis";
import activityPlugin from "./routes/activity";
import health from "./routes/health";
import indexRoute from "./routes/index";
import queues from "./routes/queues";
import tokensRoute from "./routes/tokens";
import usageRoute from "./routes/usage";
import sentryPlugin from "./sentry";
import telemetry from "./telemetry";
import tokenAuthPlugin from "./token-auth";
import tokensRepoPlugin from "./tokens";

export type QuirrelServerConfig = {
  redisFactory: () => Redis;
  port?: number;
  host?: string;
  passphrases?: string[];
  jwtPublicKey?: string;
  runningInDocker?: boolean;
  disableTelemetry?: boolean;
  logger?: Logger;
  postHogApiKey?: string;
  rateLimiter?: {
    max?: number;
  };
};

declare module "fastify" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyInstance {
    adminBasedAuthEnabled: boolean;
  }
}

export async function createServer({
  port = 9181,
  host = "0.0.0.0",
  runningInDocker = false,
  redisFactory,
  passphrases,
  disableTelemetry,
  logger,
  jwtPublicKey,
  rateLimiter,
  postHogApiKey,
}: QuirrelServerConfig) {
  const app = fastify({
    loggerInstance: logger instanceof StructuredLogger ? logger.log : undefined,
    routerOptions: { maxParamLength: 500 },
  });

  if (!disableTelemetry) {
    app.register(sentryPlugin);
  }

  if (postHogApiKey) {
    app.register(posthogPlugin, {
      apiKey: postHogApiKey,
    });
  }

  app.register(fastifyRateLimit, {
    enableDraftSpec: true,
    max: rateLimiter?.max ?? 10000,
    keyGenerator: (req) => req.headers.authorization ?? req.ip,
  });

  app.register(cors, {
    origin: "*",
    // @fastify/cors >= 10 only allows simple methods by default; the
    // development UI (another origin) also deletes and updates jobs
    methods: ["GET", "HEAD", "PUT", "PATCH", "POST", "DELETE"],
  });

  const enableAdminBasedAuth = !!passphrases?.length;

  app.decorate("adminBasedAuthEnabled", enableAdminBasedAuth);

  app.register(swagger, {
    openapi: {
      info: {
        title: "Quirrel API",
        description: "The Queueing Solution for Serverless.",
        version: pack.version,
        contact: {
          email: "info@quirrel.dev",
          name: "Quirrel",
          url: "https://quirrel.dev",
        },
        license: {
          name: "MIT License",
          url: "https://github.com/quirrel-dev/quirrel/blob/main/LICENSE",
        },
      },
      externalDocs: {
        url: "https://docs.quirrel.dev",
        description: "Find general documentation here",
      },
      components: {
        securitySchemes: {
          Token:
            enableAdminBasedAuth || jwtPublicKey
              ? {
                  type: "http",
                  scheme: "bearer",
                  description: `Main auth scheme. Tokens can be issued via ${
                    enableAdminBasedAuth
                      ? jwtPublicKey
                        ? "JWT / Admin"
                        : "Admin"
                      : "JWT"
                  }.`,
                }
              : undefined,
          Admin: enableAdminBasedAuth
            ? {
                type: "http",
                scheme: "basic",
                description:
                  "Used for admin tasks like issuing new tokens. Username is ignored, password is specified via environment variables.",
              }
            : undefined,
          Impersonation: enableAdminBasedAuth
            ? {
                type: "http",
                scheme: "basic",
                description:
                  "Username must be the token ID to be impersonated, password is admin password.",
              }
            : undefined,
        } as any,
      },
      tags: [
        {
          name: "Queueing",
        },
        {
          name: "DX",
        },
        {
          name: "Admin",
        },
      ],
    },
  });

  app.register(swaggerUi, {
    routePrefix: "/documentation",
  });

  app.register(loggerPlugin, {
    logger,
  });

  app.register(redisPlugin, { redisFactory });
  app.register(owlPlugin);

  app.register(tokenAuthPlugin, {
    enable: enableAdminBasedAuth,
    passphrases: passphrases ?? [],
    jwtPublicKey,
  });

  app.register(jobsRepoPlugin);

  if (!disableTelemetry) {
    app.register(telemetry, { runningInDocker });
  }

  if (passphrases) {
    app.register(basicAuthPlugin, { passphrases });
    app.register(usageRoute, { prefix: "/usage" });

    if (!jwtPublicKey) {
      app.register(tokensRepoPlugin);
      app.register(tokensRoute, { prefix: "/tokens" });
    }
  }

  app.register(health, { prefix: "/health" });
  app.register(queues, { prefix: "/queues" });
  app.register(activityPlugin, { prefix: "/activity" });

  app.register(indexRoute);

  app.ready(async () => {
    app.swagger();

    app.telemetrist?.dispatch("ready");

    if (passphrases) {
      app.telemetrist?.dispatch("auth enabled");
    }

    if (port !== 9181) {
      app.telemetrist?.dispatch("custom port");
    }
  });

  await app.listen({
    port,
    host,
  });

  const { address, port: runningPort } = app.server.address() as AddressInfo;

  logger?.started(`${address}:${runningPort}`, !disableTelemetry);

  return {
    server: app.server,
    app,
    port,
    async close() {
      app.log.info("Closing.");
      await app.close();
    },
  };
}
