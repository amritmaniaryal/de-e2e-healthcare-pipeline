"""Screenshot the Streamlit dashboard by driving headless Chrome over CDP.

WHY CDP AND NOT --screenshot
    Streamlit renders client-side over a websocket, so a plain
    `chrome --headless --screenshot` fires the shutter before a single chart
    exists. Driving CDP lets us wait for the render, set an exact viewport, and
    capture beyond it -- `captureBeyondViewport` grabs the whole page even though
    the emulated window is only 1000px tall.

WHY DARK
    docs/dbt-lineage.png and docs/airflow-run-detail.png are dark. Streamlit
    follows prefers-color-scheme, so the media override keeps the README's
    screenshots consistent.

COST NOTE
    The page load reruns the Streamlit script. If the server's 10-minute query
    cache has expired this issues fresh queries and resumes the warehouse.

Usage: python tools/shot_dashboard.py
"""

import base64
import json
import subprocess
import time
import urllib.request

import websocket

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT = 9334
PROFILE = ".chrome-dashboard"
URL = "http://localhost:8501"
OUT = "docs/dashboard.png"

WIDTH = 1440
HEIGHT = 1000
SCALE = 2


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
            f"--window-size={WIDTH},{HEIGHT}",
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
        self.ws = websocket.create_connection(url, timeout=120)
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

    def shot(self, path, full_height=None):
        params = {"format": "png", "captureBeyondViewport": True}
        if full_height:
            # A device-metrics override makes Chrome ignore captureBeyondViewport
            # on its own, so the region has to be stated explicitly.
            params["clip"] = {
                "x": 0,
                "y": 0,
                "width": WIDTH,
                "height": full_height,
                "scale": 1,
            }
        data = self.call("Page.captureScreenshot", **params)["data"]
        with open(path, "wb") as f:
            f.write(base64.b64decode(data))


def main():
    proc = launch()
    try:
        cdp = Cdp(browser_ws())
        cdp.new_page()

        cdp.call(
            "Emulation.setEmulatedMedia",
            features=[{"name": "prefers-color-scheme", "value": "dark"}],
        )
        cdp.call(
            "Emulation.setDeviceMetricsOverride",
            width=WIDTH,
            height=HEIGHT,
            deviceScaleFactor=SCALE,
            mobile=False,
        )

        cdp.call("Page.navigate", url=URL)
        # Streamlit: page load, websocket handshake, then chart render.
        time.sleep(12)

        print("title:", cdp.js("document.title"))
        print("charts rendered:", cdp.js("document.querySelectorAll('.js-plotly-plot, canvas, svg').length"))

        # Streamlit scrolls inside its own container, so document.body.scrollHeight
        # is only ever the viewport height. Measure the app container instead,
        # then grow the emulated viewport to fit it and let the app lay out again.
        candidates = cdp.js(
            """
            (() => {
              const sel = [
                '[data-testid="stAppViewContainer"]',
                '[data-testid="stMain"]',
                '[data-testid="stVerticalBlock"]',
                'section.main',
                'body',
              ];
              const out = {};
              for (const s of sel) {
                const el = document.querySelector(s);
                out[s] = el ? el.scrollHeight : null;
              }
              return out;
            })()
            """
        )
        print("scroll heights:", candidates)
        page_height = max([h for h in candidates.values() if h] or [HEIGHT])

        if page_height > HEIGHT:
            cdp.call(
                "Emulation.setDeviceMetricsOverride",
                width=WIDTH,
                height=page_height,
                deviceScaleFactor=SCALE,
                mobile=False,
            )
            time.sleep(3)  # let Streamlit relayout into the taller viewport

        # Trim to the last element rather than the container, which carries
        # trailing padding that shows up as a dead band at the bottom.
        content_bottom = cdp.js(
            """
            (() => {
              const el = document.querySelector('[data-testid="stVerticalBlock"]');
              return el ? Math.ceil(el.getBoundingClientRect().bottom + 40) : null;
            })()
            """
        )
        if content_bottom:
            page_height = content_bottom

        print("capture height (css px):", page_height)
        cdp.shot(OUT, full_height=page_height)
        print(f"wrote {OUT}")
    finally:
        proc.terminate()
        subprocess.run(["pkill", "-f", f"remote-debugging-port={PORT}"], check=False)
        time.sleep(1)


if __name__ == "__main__":
    main()
