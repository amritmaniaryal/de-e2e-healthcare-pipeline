"""Screenshot the dbt docs site by driving headless Chrome over CDP.

Why this exists: `--headless --screenshot` ignores URL hash fragments, and the
dbt docs site routes entirely by hash, so a plain headless call can only ever
capture the welcome page. Driving Chrome over the DevTools Protocol lets us
create a real page, navigate, click, and capture.

Usage: python tools/shot_dbt_docs.py
"""

import base64
import json
import subprocess
import time
import urllib.request

import websocket

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT = 9333
BASE = "http://localhost:8081"
PROFILE = ".chrome-cdp"


def launch():
    subprocess.run(["rm", "-rf", PROFILE], check=False)
    return subprocess.Popen(
        [
            CHROME,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--hide-scrollbars",
            f"--remote-debugging-port={PORT}",
            f"--user-data-dir={PROFILE}",
            "--window-size=1700,1000",
            "about:blank",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def browser_ws():
    for _ in range(40):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json/version") as r:
                return json.load(r)["webSocketDebuggerUrl"]
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("chrome devtools never came up")


class Cdp:
    def __init__(self, url):
        self.ws = websocket.create_connection(url, timeout=90)
        self.id = 0
        self.session = None

    def call(self, method, **params):
        self.id += 1
        msg = {"id": self.id, "method": method, "params": params}
        if self.session:
            msg["sessionId"] = self.session
        self.ws.send(json.dumps(msg))
        while True:
            reply = json.loads(self.ws.recv())
            if reply.get("id") == self.id:
                if "error" in reply:
                    raise RuntimeError(f"{method}: {reply['error']}")
                return reply.get("result", {})

    def new_page(self):
        target = self.call("Target.createTarget", url="about:blank")
        attached = self.call(
            "Target.attachToTarget", targetId=target["targetId"], flatten=True
        )
        self.session = attached["sessionId"]
        self.call("Page.enable")
        self.call("Runtime.enable")

    def js(self, expression):
        r = self.call(
            "Runtime.evaluate",
            expression=expression,
            awaitPromise=True,
            returnByValue=True,
        )
        return r.get("result", {}).get("value")

    def goto(self, url, settle=5.0):
        self.call("Page.navigate", url=url)
        time.sleep(settle)

    def shot(self, path):
        data = self.call("Page.captureScreenshot", format="png")["data"]
        with open(path, "wb") as f:
            f.write(base64.b64decode(data))


def main():
    proc = launch()
    try:
        cdp = Cdp(browser_ws())
        cdp.new_page()

        # ---- 1. full lineage graph -----------------------------------------
        cdp.goto(f"{BASE}/")
        print("welcome hash:", cdp.js("location.hash"), "| title:", cdp.js("document.title"))
        clicked = cdp.js(
            """
            (() => {
              const b = document.querySelector('[title="View Lineage Graph"]');
              if (!b) return 'graph button not found';
              b.click();
              return 'clicked View Lineage Graph';
            })()
            """
        )
        print("graph button:", clicked)
        time.sleep(7)
        cdp.shot("docs/dbt-lineage.png")
        print("wrote docs/dbt-lineage.png")

        # ---- 2. a mart model page -----------------------------------------
        # Fresh page target: the docs SPA carries the open lineage overlay in
        # the URL (?g_v=1), so reusing the page from step 1 just re-shows the
        # graph instead of the model detail.
        cdp.new_page()
        cdp.goto(f"{BASE}/#!/model/model.healthcare.dim_patients", settle=7)
        print("model hash:", cdp.js("location.hash"))
        print("heading:", cdp.js("(document.querySelector('h1')||{}).innerText"))
        cdp.shot("docs/dbt-model-detail.png")
        print("wrote docs/dbt-model-detail.png")
    finally:
        proc.terminate()
        subprocess.run(["pkill", "-f", f"remote-debugging-port={PORT}"], check=False)
        time.sleep(1)


if __name__ == "__main__":
    main()
