import delay from "delay";
import { runQuirrel } from "./runQuirrel";
import { test } from "@playwright/test";
import {
  expectToShowAttachingToQuirrel,
  expectToShowJobTable,
} from "./assertions";

let cleanup: (() => Promise<void>)[] = [];

test.beforeEach(() => {
  cleanup = [];
});

test.afterEach(async () => {
  await Promise.all(cleanup.map((clean) => clean()));
});

test("automatically connects when Quirrel is started", async ({ page }) => {
  await page.goto("http://localhost:1234/pending");

  await expectToShowAttachingToQuirrel(page);

  const quirrelServer = await runQuirrel();

  cleanup.push(quirrelServer.cleanup);

  await delay(550);

  await expectToShowJobTable(page);

  await quirrelServer.cleanup();

  await expectToShowAttachingToQuirrel(page);
});
