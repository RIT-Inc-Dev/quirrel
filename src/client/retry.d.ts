declare module "@vercel/fetch-retry" {
  import { Options as RetryOptions } from "async-retry";
  import type { fetch } from "cross-fetch";
  export default function retry(f: typeof fetch): (
    input: RequestInfo,
    init?: RequestInit & {
      retry?: RetryOptions;
    },
  ) => Promise<Response>;
}
