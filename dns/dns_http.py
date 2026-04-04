import json
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


HTTP_PORT = 80


def build_cluster_entries(my_cluster, my_alias, peers, alias_owners, derive_alias):
    cluster_names = [my_cluster] + sorted(peers.keys())
    entries = []
    for canonical in cluster_names:
        alias = my_alias if canonical == my_cluster else derive_alias(canonical)
        alias_status = "active" if alias_owners.get(alias) == canonical else "conflicted"
        entries.append(
            {
                "clusterName": canonical,
                "clusterAlias": alias,
                "aliasStatus": alias_status,
                "adminUrl": f"http://admin.{canonical}.maestro.internal/",
                "isLocal": canonical == my_cluster,
            }
        )
    return entries


def snapshot_clusters(my_cluster, my_alias, state, lock, derive_alias):
    with lock:
        peers = dict(state.get("peers", {}))
        alias_owners = dict(state.get("alias_owners", {}))
    return {
        "clusterName": my_cluster,
        "clusterAlias": my_alias,
        "clusters": build_cluster_entries(my_cluster, my_alias, peers, alias_owners, derive_alias),
    }


def render_clusters_page(snapshot):
    items = []
    for cluster in snapshot["clusters"]:
        alias_note = (
            f"{escape(cluster['clusterAlias'])} (conflicted)"
            if cluster["aliasStatus"] == "conflicted"
            else escape(cluster["clusterAlias"])
        )
        local_badge = '<span class="badge">local</span>' if cluster["isLocal"] else ""
        items.append(
            f"""
            <li class="cluster-item">
              <div class="cluster-meta">
                <a class="cluster-link" href="{escape(cluster['adminUrl'])}">{escape(cluster['clusterName'])}</a>
                {local_badge}
              </div>
              <div class="cluster-alias">alias: {alias_note}</div>
            </li>
            """
        )

    body = "\n".join(items)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Maestro Clusters</title>
    <style>
      :root {{
        color-scheme: light;
        font-family: ui-sans-serif, system-ui, sans-serif;
      }}
      body {{
        margin: 0;
        background: #f5f5f4;
        color: #1c1917;
      }}
      main {{
        max-width: 720px;
        margin: 0 auto;
        padding: 48px 20px 64px;
      }}
      h1 {{
        margin: 0 0 8px;
        font-size: 30px;
        line-height: 1.1;
      }}
      p {{
        margin: 0;
        color: #57534e;
      }}
      ul {{
        list-style: none;
        padding: 0;
        margin: 32px 0 0;
        display: grid;
        gap: 12px;
      }}
      .cluster-item {{
        background: white;
        border: 1px solid #e7e5e4;
        border-radius: 14px;
        padding: 16px 18px;
      }}
      .cluster-meta {{
        display: flex;
        align-items: center;
        gap: 10px;
      }}
      .cluster-link {{
        color: #0f766e;
        text-decoration: none;
        font-weight: 600;
      }}
      .cluster-link:hover {{
        text-decoration: underline;
      }}
      .cluster-alias {{
        margin-top: 6px;
        color: #78716c;
        font-size: 14px;
      }}
      .badge {{
        border-radius: 999px;
        background: #ccfbf1;
        color: #115e59;
        font-size: 12px;
        font-weight: 600;
        padding: 2px 8px;
      }}
    </style>
  </head>
  <body>
    <main>
      <h1>Connected clusters</h1>
      <p>Use the canonical admin hostname for a specific cluster.</p>
      <ul>{body}</ul>
    </main>
  </body>
</html>
"""


def make_http_handler(my_cluster, my_alias, state, lock, derive_alias):
    class ClusterHttpHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path in ["/api/clusters", "/api/clusters/"]:
                snapshot = snapshot_clusters(my_cluster, my_alias, state, lock, derive_alias)
                payload = json.dumps(snapshot).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif parsed.path == "/":
                snapshot = snapshot_clusters(my_cluster, my_alias, state, lock, derive_alias)
                payload = render_clusters_page(snapshot).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            else:
                self.send_response(302)
                self.send_header("Location", "/")
                self.end_headers()

        def log_message(self, format, *args):
            return

    return ClusterHttpHandler


def http_server_loop(my_cluster, my_alias, state, lock, derive_alias):
    server = ThreadingHTTPServer(
        ("0.0.0.0", HTTP_PORT),
        make_http_handler(my_cluster, my_alias, state, lock, derive_alias),
    )
    server.serve_forever()
