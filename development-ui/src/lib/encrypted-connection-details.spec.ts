import { test } from "@playwright/test";
import { expect } from "chai";
import { extendTo32Characters } from "./encrypted-connection-details";

test("extendTo32Characters", () => {
  expect(extendTo32Characters("hello world")).to.have.length(32);
  expect(
    extendTo32Characters(
      "longerthan32charactersstringislongerthan32characters",
    ),
  ).to.have.length(32);
});
