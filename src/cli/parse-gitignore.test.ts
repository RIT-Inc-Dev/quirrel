import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import spawn from "cross-spawn";
import { getAllGitIgnores } from "./parse-gitignore";

describe("getAllGitIgnores", () => {
  let tmpDir: string;
  let mockedSpawnSync: ReturnType<typeof spyOn<typeof spawn, "sync">>;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "quirrel-gitignore-"));
    mockedSpawnSync = spyOn(spawn, "sync");
  });

  afterEach(() => {
    mockedSpawnSync.mockRestore();
    fs.rmdirSync(tmpDir, { recursive: true });
  });

  test("treats global gitignore paths containing regex metacharacters literally", () => {
    const globalDir = path.join(tmpDir, "home (1)+");
    fs.mkdirSync(globalDir);
    const globalIgnore = path.join(globalDir, ".gitignore");
    fs.writeFileSync(globalIgnore, "secret\n");

    mockedSpawnSync.mockReturnValue({
      status: 0,
      stdout: Buffer.from(`${globalIgnore}\n`),
    } as any);

    const projectDir = path.join(tmpDir, "project");
    fs.mkdirSync(projectDir);

    expect(getAllGitIgnores(projectDir)).toEqual([
      { gitIgnore: "secret\n", prefix: "" },
    ]);
  });
});
