import { expect } from "chai";
import { Page } from "@playwright/test";

export async function expectTableCellToEqual(
  row: number,
  column: number,
  value: string,
  _page: Page
) {
  const rowEl = await _page.$(`//tr[${row}]`);
  expect(rowEl).to.exist;
  expect(
    await (await _page.$(`//tr[${row}]/td[${column}]`))?.innerText()
  ).to.equal(value);
}

export async function expectTableToBeEmpty(_page: Page) {
  const table = await _page.$(`tbody`);
  expect(await table?.innerHTML()).to.equal("");
}

export async function expectToShowAttachingToQuirrel(page: Page) {
  const attachingEl = await page.$("#attaching-to-quirrel");
  expect(attachingEl).to.exist;
  expect(await attachingEl?.innerText()).to.equal("Attaching to Quirrel ...");
}

export async function expectToShowJobTable(page: Page) {
  const tableEl = await page.$("[data-test-class=table]");
  expect(tableEl).to.exist;
  expect(await tableEl?.textContent()).to.equal(
    ["Endpoint", "ID", "Run At", "Payload"].join("")
  );
}
