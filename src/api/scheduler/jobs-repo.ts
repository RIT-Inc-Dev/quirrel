import { EnqueueJob } from "./types/queues/POST/body";
import { QueuesUpdateCronBody } from "./types/queues/update-cron";
import {
  encodeQueueDescriptor,
  decodeQueueDescriptor,
} from "../shared/queue-descriptor";
import * as uuid from "uuid";
import { cron, embedTimezone, parseTimezonedCron } from "../../shared/repeat";
import Owl, { Closable, Job } from "@quirrel/owl";
import { QueueRepo } from "./queue-repo";
import { Redis } from "ioredis";
import { fastifyDecoratorPlugin } from "./helper/fastify-decorator-plugin";
import * as config from "../../client/config";

interface PaginationOpts {
  cursor: number;
  count?: number;
}

interface JobDTO {
  id: string;
  endpoint: string;
  body: string;
  runAt: string;
  exclusive: boolean;
  retry: number[];
  count: number;
  repeat?: {
    every?: number;
    times?: number;
    count: number;
    cron?: string;
    cronTimezone?: string;
  };
}

export class JobsRepo implements Closable {
  protected producer;
  public readonly queueRepo: QueueRepo;

  constructor(protected readonly owl: Owl<"every" | "cron">, redis: Redis) {
    this.producer = this.owl.createProducer();
    this.queueRepo = new QueueRepo(redis, this);
  }

  private static toJobDTO(job: Job<"every" | "cron">): JobDTO {
    const { endpoint } = decodeQueueDescriptor(job.queue);

    let cron: Pick<NonNullable<JobDTO["repeat"]>, "cron" | "cronTimezone"> = {};
    if (job.schedule?.type === "cron") {
      const [cronExpression, cronTimezone] = parseTimezonedCron(
        job.schedule.meta
      );
      cron = { cron: cronExpression, cronTimezone };
    }

    return {
      id: job.id,
      endpoint,
      body: job.payload,
      runAt: job.runAt.toISOString(),
      exclusive: job.exclusive,
      retry: job.retry,
      count: job.count,
      repeat: job.schedule
        ? {
            ...cron,
            count: job.count,
            every:
              job.schedule?.type === "every" ? +job.schedule.meta : undefined,
            times: job.schedule?.times,
          }
        : undefined,
    };
  }

  public async close() {
    await this.producer.close();
  }

  public async find(
    byTokenId: string,
    endpoint: string,
    { count, cursor }: PaginationOpts
  ) {
    const { newCursor, jobs } = await this.producer.scanQueue(
      encodeQueueDescriptor(byTokenId, endpoint),
      cursor,
      count
    );

    return {
      cursor: newCursor,
      jobs: jobs.map(JobsRepo.toJobDTO),
    };
  }

  public async findAll({ count, cursor }: PaginationOpts) {
    const { newCursor, jobs } = await this.producer.scanQueuePattern(
      encodeQueueDescriptor("*", "*"),
      cursor,
      count
    );

    return {
      cursor: newCursor,
      jobs: jobs.map((job) => {
        const { tokenId } = decodeQueueDescriptor(job.queue);
        return {
          ...JobsRepo.toJobDTO(job),
          tokenId,
        };
      }),
    };
  }

  public async findByTokenId(
    byTokenId: string,
    { count, cursor }: PaginationOpts
  ) {
    const { newCursor, jobs } = await this.producer.scanQueuePattern(
      encodeQueueDescriptor(byTokenId, "*"),
      cursor,
      count
    );

    return {
      cursor: newCursor,
      jobs: jobs.map(JobsRepo.toJobDTO),
    };
  }

  public async findById(tokenId: string, endpoint: string, id: string) {
    const job = await this.producer.findById(
      encodeQueueDescriptor(tokenId, endpoint),
      id
    );
    return job ? JobsRepo.toJobDTO(job) : undefined;
  }

  public async invoke(tokenId: string, endpoint: string, id: string) {
    return await this.producer.invoke(
      encodeQueueDescriptor(tokenId, endpoint),
      id
    );
  }

  public async emptyQueue(tokenId: string, endpoint: string) {
    let cursor = 0;
    const allPromises: Promise<any>[] = [];
    do {
      const { cursor: newCursor, jobs } = await this.find(tokenId, endpoint, {
        cursor,
      });
      cursor = newCursor;

      for (const job of jobs) {
        allPromises.push(this.delete(tokenId, endpoint, job.id));
      }
    } while (cursor !== 0);

    await Promise.all(allPromises);
  }

  public async delete(tokenId: string, endpoint: string, id: string) {
    return await this.producer.delete(
      encodeQueueDescriptor(tokenId, endpoint), // TODO encode
      id
    );
  }

  public async enqueue(
    tokenId: string,
    endpoint: string,
    {
      body,
      runAt: runAtOption,
      id,
      delay,
      repeat,
      override,
      exclusive,
      retry,
    }: EnqueueJob
  ) {
    console.log("tokenId", tokenId)
    console.log('body', body)
    if (typeof id === "undefined") {
      id = uuid.v4();
    }

    let runAt: Date | undefined = undefined;

    if (runAtOption) {
      runAt = new Date(runAtOption);
    } else if (delay) {
      runAt = new Date(Date.now() + delay);
    }

    if (typeof repeat?.times === "number" && repeat.times < 1) {
      throw new Error("repeat.times must be positive");
    }

    let schedule_type: "every" | "cron" | undefined = undefined;
    let schedule_meta: string | undefined = undefined;

    if (repeat?.cron) {
      schedule_type = "cron";

      if (repeat?.cronTimezone) {
        schedule_meta = embedTimezone(repeat.cron, repeat.cronTimezone);
      } else {
        schedule_meta = repeat.cron;
      }

      runAt = cron(runAt ?? new Date(), schedule_meta);
    }

    if (repeat?.every) {
      schedule_type = "every";
      schedule_meta = "" + repeat.every;
    }

    console.log('before created job')
    console.log('tokenId', tokenId)
    console.log('endpoint', endpoint)
    console.log('encodeQueueDescripted', encodeQueueDescriptor(tokenId, endpoint))
    const createdJob = await this.producer.enqueue({
      // TODO encode
      queue: encodeQueueDescriptor(tokenId, endpoint),
      id,
      payload: body ?? "",
      runAt,
      exclusive,
      schedule: schedule_type
        ? {
            type: schedule_type,
            meta: schedule_meta!,
            times: repeat?.times,
          }
        : undefined,
      override,
      retry,
    });
    console.log('createdJob', createdJob)
    await this.queueRepo.add(endpoint, tokenId);

    return JobsRepo.toJobDTO(createdJob);
  }

  public async updateCron(
    tokenId: string,
    { baseUrl, crons, dryRun }: QueuesUpdateCronBody
  ) {
    const deleted: string[] = [];

    const queues = await this.queueRepo.get(tokenId); // `queues:by-token:${tokenId}` の形式で取得
    const queuesOnSameDeployment = queues.filter((q) => q.startsWith(baseUrl));
    console.log('crons', crons)
    console.log('queues', queues);
    console.log('queuesOnSameDeployment', queuesOnSameDeployment)

    if (!dryRun) {
      await Promise.all(
        crons.map(async ({ route, schedule, timezone }) => {
          await this.enqueue( // 1. detectしたcronsをenqueue, jobs:local;http%3A%2F%2Fapp%3A3000%2Fapi%2Fjobs%2FsoftDeleteOldProjectAndConcilDocuments:@cron のkey // 実行されたら消える？, decode 1回
            tokenId, // 2. queues:by-token:local のkey, 値は, http://app:3000/api/jobs/softDeleteOldProjectAndConcilDocuments, プロセスで保持していて、初回だけ保存
            `${config.withoutTrailingSlash(
              baseUrl
            )}/${config.withoutLeadingSlash(route)}`,
            {
              id: "@cron",
              body: "null",
              override: true,
              repeat: { cron: schedule, cronTimezone: timezone },
            }
          );
        })
      );
    }

    const routesThatShouldPersist = crons.map((c) => c.route[0] === '/' ? c.route.slice(1) : c.route );
    console.log('routesThatShouldPersist', routesThatShouldPersist) // api/jobs/deleteOldProjectsAndLetters
    await Promise.all(
      queuesOnSameDeployment.map(async (queue) => {
        // 既にjobとして追加されていて、今回でもjobとしてdetectされた原因は、
        // crons中のrouteでは、文頭の / がないのに、
        // 下の行で queue.slice しているbaseUrlに / が含まれていないので、routeに / が含まれてしまっているため
        // 本番環境の挙動が予測できないので、２つを比較するときに、cronsのrouteもqueueから切り出したrouteも頭に / がある時は削除してから比較する
        // そもそも何故このようなことになっているかは不明
        const unCleansingRoute = queue.slice(baseUrl.length)
        const route = unCleansingRoute[0] === '/' ? unCleansingRoute.slice(1) : unCleansingRoute;
        console.log('match routesThatShouldPersist route', route) // /api/jobs/softDeleteOldProjectAndConcilDocuments
        const shouldPersist = routesThatShouldPersist.includes(route);
        if (shouldPersist) {
          return;
        }
        console.log('NotShouldPersist', queue)

        if (dryRun) {
          const exists = await this.findById(tokenId, queue, "@cron");
          if (exists) {
            deleted.push(queue);
          }
        } else {
          // jobs:local;http%3A%2F%2Fapp%3A3000%2Fapi%2Fjobs%2FsoftDeleteOldProjectAndConcilDocuments:@cron のkeyを削除している可能性が高い
          const result = await this.delete(tokenId, queue, "@cron");
          if (result === "deleted") {
            deleted.push(queue);
          }
        }
      })
    );

    return { deleted };
  }

  public onEvent(
    requesterTokenId: string,
    cb: (
      event: string,
      job: { endpoint: string; id: string; runAt?: string } | JobDTO
    ) => void
  ) {
    const activity = this.owl.createActivity(
      async (event) => {
        if (event.type === "scheduled") {
          cb(
            "scheduled",
            JobsRepo.toJobDTO(event.job as Job<"every" | "cron">)
          );
          return;
        }

        const { endpoint } = decodeQueueDescriptor(event.queue);

        switch (event.type) {
          case "acknowledged":
            cb("completed", { endpoint, id: event.id });
            break;
          case "requested":
            cb("started", { endpoint, id: event.id });
            break;
          case "invoked":
            cb("invoked", { endpoint, id: event.id });
            break;
          case "rescheduled":
            cb("rescheduled", {
              endpoint,
              id: event.id,
              runAt: event.runAt.toISOString(),
            });
            break;
          case "deleted":
            cb("deleted", { endpoint, id: event.id });
            break;
        }
      },
      {
        queue: encodeQueueDescriptor(requesterTokenId, "*"),
      }
    );

    return () => activity.close();
  }
}

declare module "fastify" {
  interface FastifyInstance {
    jobs: JobsRepo;
  }
}

export const jobsRepoPlugin = fastifyDecoratorPlugin(
  "jobs",
  (fastify) => new JobsRepo(fastify.owl, fastify.redis)
);
