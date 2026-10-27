import request from "supertest";
import { Queue } from "../../connect";

test("connect responds to handler errors as plain text", async () => {
  const queue = Queue("queue", async () => {
    throw new Error("<script>alert(1)</script>");
  });

  const res = await request(queue)
    .post("/queue")
    .set("Content-Type", "text/plain")
    .send(JSON.stringify({ json: "payload" }));

  expect(res.status).toBe(500);
  expect(res.headers["content-type"]).toMatch(/^text\/plain/);
  expect(res.headers["x-content-type-options"]).toBe("nosniff");
  expect(res.text).toBe("Error: <script>alert(1)</script>");
});
