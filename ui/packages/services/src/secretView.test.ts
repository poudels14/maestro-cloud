import { describe, expect, it } from "vitest";
import { secretNames } from "./secretView";

describe("secretNames", () => {
  it("lists dotenv keys and file names without reading their values", () => {
    expect(
      secretNames({
        format: "dotenv",
        mountPath: "/run/secrets/app.env",
        items: { TOKEN: "masked", DATABASE_URL: "masked" }
      })
    ).toEqual(["DATABASE_URL", "TOKEN"]);
    expect(
      secretNames({
        format: "files",
        mountPath: "/run/secrets/etcd",
        files: { "client-key.pem": "masked", "ca.pem": "masked" }
      })
    ).toEqual(["ca.pem", "client-key.pem"]);
  });
});
