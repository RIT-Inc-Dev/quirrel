import bodyParser from "body-parser";
import connect from "connect";
import {
  DefaultJobOptions,
  EnqueueJobOptions,
  EnqueueJobOpts,
  Job,
  QuirrelClient,
  QuirrelJobHandler,
  type QuirrelOptions,
} from "./client";

export {
  DefaultJobOptions,
  EnqueueJobOptions,
  EnqueueJobOpts,
  Job,
  QuirrelJobHandler,
};

export type Queue<Payload> = connect.Server &
  Omit<QuirrelClient<Payload>, "respondTo" | "makeRequest">;

declare module "connect" {
  // biome-ignore lint/style/useConsistentTypeDefinitions: module augmentation requires interface
  export interface IncomingMessage {
    body: string;
  }
}

export function Queue<Payload>(
  route: string,
  handler: QuirrelJobHandler<Payload>,
  options?: QuirrelOptions<Payload>,
): Queue<Payload> {
  const quirrel = new QuirrelClient({
    route,
    handler,
    options,
  });

  const server = connect() as Queue<Payload>;

  server.use(bodyParser.text());
  server.use(async (req, res, next) => {
    if (req.url !== `/${route}`) {
      return next();
    }

    const { body, status, headers } = await quirrel.respondTo(
      req.body,
      req.headers as Record<string, string>,
    );

    res.statusCode = status;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.setHeader("X-Content-Type-Options", "nosniff");
    for (const [header, value] of Object.entries(headers)) {
      res.setHeader(header, value);
    }
    res.write(body);
    res.end();
  });

  server.enqueue = (payload, options) => quirrel.enqueue(payload, options);
  server.enqueueMany = (jobs) => quirrel.enqueueMany(jobs);
  server.get = () => quirrel.get();
  server.delete = (id) => quirrel.delete(id);
  server.getById = (id) => quirrel.getById(id);
  server.invoke = (id) => quirrel.invoke(id);

  return server;
}

export function CronJob(
  route: string,
  cronSchedule: NonNullable<NonNullable<EnqueueJobOptions["repeat"]>["cron"]>,
  handler: () => Promise<void>,
  options?: QuirrelOptions,
) {
  return Queue(route, handler, options) as unknown;
}
