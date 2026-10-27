import { describe, expect, test } from "bun:test";
import { checkIp } from "./ssrf-filter";

describe("checkIp", () => {
  test.each([
    "127.0.0.1",
    "10.0.0.1",
    "172.16.0.1",
    "192.168.1.1",
    "169.254.169.254",
    "::1",
    "fd00::1",
  ])("blocks non-unicast address %s", (ip) => {
    expect(checkIp(ip)).toBe(false);
  });

  test.each(["8.8.8.8", "2606:4700:4700::1111"])(
    "allows public address %s",
    (ip) => {
      expect(checkIp(ip)).toBe(true);
    },
  );

  test("lets hostnames through to the lookup check", () => {
    expect(checkIp("example.com")).toBe(true);
  });
});
