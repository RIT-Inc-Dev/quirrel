import Owl from "@quirrel/owl";
import type { Redis } from "ioredis";
import { cron, every } from "../../shared/repeat";
import type { ExecutionError } from "../worker";
import { IncidentForwarder } from "./incident-forwarder";
import type { Logger } from "./logger";
import { decodeQueueDescriptor, toPlainEndpoint } from "./queue-descriptor";
import type { Telemetrist } from "./telemetrist";

export async function createOwl(
  redisFactory: () => Redis,
  logger?: Logger,
  incidentReceiver?: { endpoint: string; passphrase: string },
  telemetrist?: Telemetrist,
) {
  const incidentForwarder = incidentReceiver
    ? new IncidentForwarder(
        incidentReceiver.endpoint,
        incidentReceiver.passphrase,
      )
    : undefined;

  const owl = new Owl({
    redisFactory: redisFactory as any,
    scheduleMap: {
      every,
      cron,
    },
    logger: logger?.log?.child({ module: "owl" }),
    async onError(ack, job, error: ExecutionError) {
      let { tokenId, endpoint } = decodeQueueDescriptor(job.queue);
      endpoint = toPlainEndpoint(endpoint);

      await incidentForwarder?.dispatch(
        {
          endpoint,
          tokenId,
          payload: job.payload,
          id: job.id,
          runAt: job.runAt,
        },
        {
          body: error.responseBody,
          status: error.responseStatus,
        },
      );

      await telemetrist?.dispatch("execution_errored");
    },
  });

  if (!process.env.SKIP_OWL_MIGRATIONS) {
    await owl.runMigrations();
  }

  return owl;
}
