import type { IncomingHttpHeaders, IncomingMessage } from "node:http";
import basicAuth from "basic-auth";
import type {
  FastifyPluginCallback,
  FastifyReply,
  FastifyRequest,
  preValidationHookHandler,
} from "fastify";
import fp from "fastify-plugin";
import jwt from "jsonwebtoken";
import { UsageMeter } from "../shared/usage-meter";

type TokenAuthService = {
  authenticate(
    request: FastifyRequest | IncomingMessage,
  ): Promise<string | null>;
};

declare module "fastify" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyInstance {
    tokenAuth: TokenAuthService;
    tokenAuthPreValidation: preValidationHookHandler;
  }

  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyRequest {
    tokenId: string;
  }
}

type TokenAuthPluginOpts = {
  enable: boolean;
  passphrases: string[];
  jwtPublicKey?: string;
};

const tokenAuthServicePlugin: FastifyPluginCallback<TokenAuthPluginOpts> = (
  fastify,
  opts,
  done,
) => {
  // always set by tokenAuthPreValidation before handlers read it
  fastify.decorateRequest("tokenId", "");
  fastify.decorate(
    "tokenAuthPreValidation",
    async (request: FastifyRequest, reply: FastifyReply) => {
      const tokenId = await fastify.tokenAuth.authenticate(request);

      if (tokenId === null) {
        reply.status(401).send("Unauthenticated");
      } else {
        request.tokenId = tokenId;
      }
    },
  );

  function decorateTokenAuth(service: TokenAuthService) {
    fastify.decorate("tokenAuth", service);
  }

  if (!opts.enable) {
    decorateTokenAuth({
      async authenticate() {
        return "anonymous";
      },
    });

    return done();
  }

  async function checkJwt(
    token: string,
  ): Promise<["not_a_jwt"] | ["success", string] | ["invalid"]> {
    const { jwtPublicKey } = opts;
    if (!jwtPublicKey) {
      return ["not_a_jwt"];
    }

    return await new Promise<["not_a_jwt"] | ["success", string] | ["invalid"]>(
      (resolve) => {
        jwt.verify(token, jwtPublicKey, (err, result) => {
          if (err) {
            return resolve(["invalid"]);
          }

          const { sub } = result as { sub?: string };
          if (sub) {
            resolve(["success", sub]);
          } else {
            resolve(["invalid"]);
          }
        });
      },
    );
  }

  async function getTokenID(headers: IncomingHttpHeaders) {
    const { authorization } = headers;
    if (!authorization) {
      return null;
    }

    if (authorization.startsWith("Bearer ")) {
      const [_, token] = authorization.split("Bearer ");
      const [result, jwtTokenId] = await checkJwt(token);
      switch (result) {
        case "not_a_jwt":
          break;
        case "invalid":
          return null;
        case "success":
          return { tokenId: jwtTokenId!, countUsage: true };
      }

      const redisTokenId = await fastify.tokens?.check(token);
      if (redisTokenId) {
        return { tokenId: redisTokenId, countUsage: true };
      }

      return null;
    } else if (authorization.startsWith("Basic ")) {
      const basicCredentials = basicAuth.parse(authorization);

      if (!basicCredentials) {
        return null;
      }

      const isRootUser = opts.passphrases.includes(basicCredentials.pass);
      if (isRootUser) {
        const countUsage = !!headers["x-quirrel-count-usage"];
        return { tokenId: basicCredentials.name, countUsage };
      }
    }

    return null;
  }

  const usageMeter = new UsageMeter(fastify.redis);

  decorateTokenAuth({
    async authenticate(request) {
      const result = await getTokenID(request.headers);
      if (!result) {
        return null;
      }

      const { tokenId, countUsage } = result;

      if (countUsage) {
        usageMeter.record(tokenId);
      }

      return tokenId;
    },
  });

  return done();
};

export default (fp as any)(
  tokenAuthServicePlugin,
) as FastifyPluginCallback<TokenAuthPluginOpts>;
