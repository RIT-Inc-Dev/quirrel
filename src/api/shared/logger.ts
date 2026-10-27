import pino from "pino";
import type { JobDTO } from "../../client/job";
import type { QueuesUpdateCronBody } from "../scheduler/types/queues/update-cron";
import { DxLogger } from "./dx-logger";
import { QuietLogger } from "./quiet-logger";
import { StructuredLogger } from "./structured-logger";

export type Logger = {
  log?: pino.Logger;
  started(address: string, telemetryEnabled: boolean): void;
  jobCreated(
    job: JobDTO & {
      tokenId: string;
    },
  ): void;
  cronUpdated(crons: QueuesUpdateCronBody, deleted: string[]): void;
  jobDeleted(job: { endpoint: string; id: string; tokenId: string }): void;
  /**
   * @returns function to call when execution is done
   */
  startingExecution(job: {
    id: string;
    tokenId: string;
    endpoint: string;
    body: string;
  }): () => void;

  executionErrored(
    job: {
      id: string;
      tokenId: string;
      endpoint: string;
      body: string;
    },
    error: string,
  ): void;
  // ...
};

export type LoggerType = "dx" | "structured" | "none" | "quiet" | Logger;

export function getLogger(
  type?: LoggerType,
  logger = pino({ level: process.env.LOG_LEVEL || "trace" }),
): Logger | undefined {
  if (!type) {
    return undefined;
  }

  if (typeof type !== "string") {
    return type;
  }

  switch (type) {
    case "none":
      return undefined;
    case "dx":
      return new DxLogger();
    case "structured":
      return new StructuredLogger(logger);
    case "quiet":
      return new QuietLogger();
  }
}
