import fs from "fs";
import os from "os";
import path from "path";
import spawn from "cross-spawn";
import { getAllGitIgnores } from "./parse-gitignore";

jest.mock("cross-spawn");

const mockedSpawnSync = spawn.sync as jest.MockedFunction<typeof spawn.sync>;

describe("getAllGitIgnores", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "quirrel-gitignore-"));
  });

  afterEach(() => {
    mockedSpawnSync.mockReset();
    fs.rmdirSync(tmpDir, { recursive: true });
  });

  test("treats global gitignore paths containing regex metacharacters literally", () => {
    const globalDir = path.join(tmpDir, "home (1)+");
    fs.mkdirSync(globalDir);
    const globalIgnore = path.join(globalDir, ".gitignore");
    fs.writeFileSync(globalIgnore, "secret\n");

    mockedSpawnSync.mockReturnValue({
      status: 0,
      stdout: Buffer.from(globalIgnore + "\n"),
    } as any);

    const projectDir = path.join(tmpDir, "project");
    fs.mkdirSync(projectDir);

    expect(getAllGitIgnores(projectDir)).toEqual([
      { gitIgnore: "secret\n", prefix: "" },
    ]);
  });
});
