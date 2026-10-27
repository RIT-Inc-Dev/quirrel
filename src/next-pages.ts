import type { IncomingHttpHeaders } from "node:http";
import {
  type DefaultJobOptions,
  type EnqueueJobOptions,
  type EnqueueJobOpts,
  type Job,
  QuirrelClient,
  type QuirrelJobHandler,
  type QuirrelOptions,
} from "./client";
import { registerDevelopmentDefaults } from "./client/config";

export type NextApiRequest = {
  body: any;
  headers: IncomingHttpHeaders;
};

export type NextApiResponse = {
  setHeader(key: string, value: string): void;
  status(code: number): void;
  send(body: string): void;
};

export type NextApiHandler = (
  req: NextApiRequest,
  res: NextApiResponse,
) => void | Promise<void>;

export type {
  DefaultJobOptions,
  EnqueueJobOptions,
  EnqueueJobOpts,
  Job,
  QuirrelJobHandler,
};

registerDevelopmentDefaults({
  applicationPort: 3000,
});

export type Queue<Payload> = Omit<
  QuirrelClient<Payload>,
  "respondTo" | "makeRequest"
>;

export function Queue<Payload>(
  route: string,
  handler: QuirrelJobHandler<Payload>,
  options?: QuirrelOptions,
): Queue<Payload> & NextApiHandler {
  const quirrel = new QuirrelClient<Payload>({
    options,
    handler,
    route,
  });

  async function nextApiHandler(req: NextApiRequest, res: NextApiResponse) {
    const response = await quirrel.respondTo(req.body, req.headers);
    res.status(response.status);
    for (const [header, value] of Object.entries(response.headers)) {
      res.setHeader(header, value);
    }
    res.send(response.body);
  }

  nextApiHandler.enqueue = (payload: Payload, options: EnqueueJobOptions) =>
    quirrel.enqueue(payload, options);

  nextApiHandler.enqueueMany = (
    jobs: { payload: Payload; options?: EnqueueJobOptions }[],
  ) => quirrel.enqueueMany(jobs);

  nextApiHandler.delete = (jobId: string) => quirrel.delete(jobId);

  nextApiHandler.invoke = (jobId: string) => quirrel.invoke(jobId);

  nextApiHandler.get = () => quirrel.get();

  nextApiHandler.getById = (jobId: string) => quirrel.getById(jobId);

  return nextApiHandler;
}

export function CronJob(
  route: string,
  cronSchedule: NonNullable<NonNullable<EnqueueJobOptions["repeat"]>["cron"]>,
  handler: () => Promise<void>,
  options?: QuirrelOptions,
) {
  return Queue(route, handler, options) as unknown;
}
