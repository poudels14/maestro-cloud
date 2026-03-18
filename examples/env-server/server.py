import http.server
import json
import os

SECRETS_FILE = os.environ.get("SECRETS_FILE", "/app/.env")
PORT = int(os.environ.get("PORT", "8080"))


def read_secrets():
    secrets = {}
    try:
        with open(SECRETS_FILE) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    secrets[key] = value
    except FileNotFoundError:
        pass
    return secrets


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return

        env_vars = {k: v for k, v in os.environ.items() if not k.startswith("_")}
        secrets = read_secrets()
        body = json.dumps(
            {
                "env": env_vars,
                "secrets": secrets,
                "hostname": os.environ.get("HOSTNAME", "unknown"),
            },
            indent=2,
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, format, *args):
        print(format % args, flush=True)


if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"env-server listening on port {PORT}", flush=True)
    server.serve_forever()
