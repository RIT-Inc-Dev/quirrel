import type { Redis } from "ioredis";
import { JobsRepo } from "./jobs-repo";

export function idempotent<Args extends any[]>(
  doIt: (...args: Args) => Promise<void>,
  keyBy: (...args: Args) => string = (...args) => args.join(";")
) {
  const executedKeys = new Set<string>();

  return async (...args: Args) => {
    const key = keyBy(...args);
    console.log('queue-repo itempotent exutedkeys', executedKeys)
    if (executedKeys.has(key)) {
      return;
    }

    await doIt(...args);
    executedKeys.add(key);
  };
}

export class QueueRepo {
  constructor(
    private readonly redis: Redis,
    private readonly jobsRepo: JobsRepo
  ) {}

  private isMigrated = process.env.SKIP_QUEUE_REPO_MIGRATION === "true";

  public add = idempotent(async (endpoint: string, tokenId: string) => {
    console.log('queue-repo add', tokenId) // encordedされてない, 追加されないケース
    console.log('queue-repo endpoint', endpoint) // encordedされてないケース, 追加されないケース
    console.log(`queues:by-token:${tokenId}`)
    await this.redis.sadd(`queues:by-token:${tokenId}`, endpoint);
  });

  public async get(tokenId: string) {
    await this.ensureMigrated();

    return await this.redis.smembers(`queues:by-token:${tokenId}`);
  }

  private async ensureMigrated() {
    if (!this.isMigrated) {
      this.isMigrated = (await this.redis.exists(`queues-migrated`)) === 1;
    }

    if (this.isMigrated) {
      return;
    }

    const tokenIdToEndpoints: Record<string, Set<string>> = {};

    let cursor = 0;
    do {
      const result = await this.jobsRepo.findAll({
        count: 1000,
        cursor,
      });

      cursor = result.cursor;

      result.jobs.forEach(({ endpoint, tokenId }) => {
        if (!tokenIdToEndpoints[tokenId]) {
          tokenIdToEndpoints[tokenId] = new Set();
        }

        tokenIdToEndpoints[tokenId].add(endpoint);
      });
    } while (cursor !== 0);

    const pipeline = this.redis.pipeline();
    for (const [tokenId, endpoints] of Object.entries(tokenIdToEndpoints)) {
      pipeline.sadd(`queues:by-token:${tokenId}`, ...endpoints);
    }

    pipeline.set(`queues-migrated`, "ture");

    await pipeline.exec();
  }
}
