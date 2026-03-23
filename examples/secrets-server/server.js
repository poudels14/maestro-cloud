import { config } from "dotenv";
import { readFileSync } from "fs";

const SECRETS_FILE = process.env.SECRETS_FILE || "/app/.env";

config({ path: SECRETS_FILE });

const server = Bun.serve({
  port: 3000,
  fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return new Response("ok");
    }

    let rawSecrets = "";
    try {
      rawSecrets = readFileSync(SECRETS_FILE, "utf-8");
    } catch {}

    const env = {};
    for (const [key, value] of Object.entries(process.env)) {
      if (value && !key.startsWith("_")) {
        env[key] = value;
      }
    }

    return Response.json({
      env,
      secretsFile: SECRETS_FILE,
      secretsFileContent: rawSecrets,
      hostname: process.env.HOSTNAME || "unknown",
    }, {
      headers: { "Content-Type": "application/json" },
    });
  },
});

console.log(`secrets-server listening on port ${server.port}`);
