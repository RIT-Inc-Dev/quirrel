import type { Redis } from "ioredis";
import fetch from "node-fetch";
import { PostHog } from "posthog-node";
import { asymmetric, symmetric } from "secure-webhooks";
import type { Logger } from "../shared/logger";
import { createOwl } from "../shared/owl";
import { decodeQueueDescriptor } from "../shared/queue-descriptor";
import { Telemetrist } from "../shared/telemetrist";
import { TokenRepo } from "../shared/token-repo";
import { UsageMeter } from "../shared/usage-meter";
import { ssrfFilter } from "./ssrf-filter";

export type ExecutionError = {
  toString(): string;
  endpoint: string;
  tokenId: string;
  responseBody: string;
  responseStatus: number;
};

export function replaceLocalhostWithDockerHost(_url: string): string {
  const url = new URL(_url);
  if (url.hostname === "localhost") {
    url.hostname = "host.docker.internal";
  }
  return url.toString();
}

export type QuirrelWorkerConfig = {
  redisFactory: () => Redis;
  enableUsageMetering?: boolean;
  runningInDocker?: boolean;
  concurrency?: number;
  disableTelemetry?: boolean;
  logger?: Logger;
  incidentReceiver?: { endpoint: string; passphrase: string };
  webhookSigningPrivateKey?: string;
  enableSSRFPrevention?: boolean;
  postHogApiKey?: string;
};

export async function createWorker({
  redisFactory,
  enableUsageMetering,
  runningInDocker,
  concurrency = 100,
  disableTelemetry,
  logger,
  incidentReceiver,
  webhookSigningPrivateKey,
  enableSSRFPrevention,
  postHogApiKey,
}: QuirrelWorkerConfig) {
  const redisClient = redisFactory();
  const telemetrist = disableTelemetry
    ? undefined
    : new Telemetrist(runningInDocker ?? false);

  const tokenRepo = webhookSigningPrivateKey
    ? undefined
    : new TokenRepo(redisClient);

  let usageMeter: UsageMeter | undefined;
  if (enableUsageMetering) {
    usageMeter = new UsageMeter(redisClient);
  }

  let postHog: PostHog | undefined;
  if (postHogApiKey) {
    postHog = new PostHog(postHogApiKey, {
      host: "https://app.posthog.com",
    });
  }

  const owl = await createOwl(
    redisFactory,
    logger,
    incidentReceiver,
    telemetrist,
  );

  const worker = await owl.createWorker(async (job, ack, span) => {
    let { tokenId, endpoint } = decodeQueueDescriptor(job.queue);
    const body = job.payload;

    endpoint = decodeURIComponent(endpoint);

    const executionDone = logger?.startingExecution({
      tokenId,
      endpoint,
      body,
      id: job.id,
    });

    const headers: Record<string, string> = {
      "Content-Type": "text/plain",
      "x-quirrel-meta": JSON.stringify({
        id: job.id,
        count: job.count,
        exclusive: job.exclusive,
        retry: job.retry,
        nextRepetition: ack.nextExecutionDate,
      }),
    };

    if (tokenId) {
      const payload = body ?? "";
      if (webhookSigningPrivateKey) {
        headers["x-quirrel-signature"] = asymmetric.sign(
          payload,
          webhookSigningPrivateKey,
        );
      } else {
        const token = await tokenRepo?.getById(tokenId);
        if (token) {
          headers["x-quirrel-signature"] = symmetric.sign(payload, token);
        }
      }
    }

    if (runningInDocker) {
      endpoint = replaceLocalhostWithDockerHost(endpoint);
    }

    const [response] = await Promise.all([
      fetch(endpoint, {
        method: "POST",
        body,
        headers,
        agent: enableSSRFPrevention ? ssrfFilter : undefined,
      }),
      usageMeter?.record(tokenId),
    ]);

    postHog?.capture({
      distinctId: tokenId,
      event: "job executed",
      properties: {
        endpoint,
        status: response.status,
      },
    });

    if (response.status >= 200 && response.status < 300) {
      executionDone?.();

      telemetrist?.dispatch("dispatch_job");

      await worker.acknowledger.acknowledge(ack, undefined, span);
    } else {
      const responseBody = await response.text();

      const error: ExecutionError = {
        tokenId,
        endpoint,
        responseBody,
        responseStatus: response.status,
        toString() {
          return responseBody;
        },
      };

      await worker.acknowledger.reportFailure(ack, job, error, {
        dontReschedule: response.status === 404,
      });
    }
  });

  async function close() {
    postHog?.shutdown();
    await worker.close();
    await redisClient.quit();
  }

  return {
    close,
  };
}
