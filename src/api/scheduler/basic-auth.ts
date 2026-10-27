import fastifyBasicAuth from "@fastify/basic-auth";
import type { FastifyPluginCallback } from "fastify";
import fp from "fastify-plugin";

type BasicAuthPluginOpts = {
  passphrases: string[];
};

const basicAuthPlugin: FastifyPluginCallback<BasicAuthPluginOpts> = async (
  fastify,
  opts,
) => {
  fastify.register(fastifyBasicAuth, {
    validate(username, password, req, reply, done) {
      if (opts.passphrases.includes(password)) {
        done();
      } else {
        done(new Error("Wrong Passphrase"));
      }
    },
  });
};

export default (fp as any)(
  basicAuthPlugin,
) as FastifyPluginCallback<BasicAuthPluginOpts>;
