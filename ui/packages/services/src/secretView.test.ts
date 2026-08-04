import { describe, expect, it } from "vitest";
import { secretEntries, secretNames, secretSourcePath } from "./secretView";

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

describe("secretEntries", () => {
  it("shows masked resolved values and lets inline values override their source", () => {
    const secrets = {
      format: "dotenv" as const,
      mountPath: "/run/secrets/app.env",
      source: "aws-secret://maestro/production/app",
      items: { TOKEN: "••••line" }
    };

    expect(
      secretEntries(secrets, {
        DATABASE_URL: "••••tion",
        TOKEN: "••••rnal"
      })
    ).toEqual([
      ["DATABASE_URL", "••••tion"],
      ["TOKEN", "••••line"]
    ]);
    expect(secretSourcePath(secrets)).toBe("maestro/production/app");
  });
});
