import type { FastifyPluginCallback } from "fastify";
import fp from "fastify-plugin";
import { PostHog } from "posthog-node";

declare module "fastify" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  interface FastifyInstance {
    postHog?: PostHog;
  }
}

type PostHogPluginOpts = {
  apiKey: string;
};

const posthogPlugin: FastifyPluginCallback<PostHogPluginOpts> = async (
  fastify,
  opts,
) => {
  const postHog = new PostHog(opts.apiKey, {
    host: "https://app.posthog.com",
  });

  fastify.decorate("postHog", postHog);

  fastify.addHook("onClose", () => {
    postHog.shutdown();
  });
};

export default (fp as any)(
  posthogPlugin,
) as FastifyPluginCallback<PostHogPluginOpts>;
