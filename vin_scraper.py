# ============================================================
# vin_scraper.py  —  Standalone GitHub Actions Version
# ============================================================
# Reads  : Vin + Website URL from Google Sheet
# Writes : Website Image URL back to same sheet
#
# Credentials come from GitHub Secrets:
#   GOOGLE_CREDENTIALS  — service account JSON (full contents)
#   SPREADSHEET_ID      — your Google Sheet ID
#
# This file runs on GitHub's servers automatically.
# No laptop, no Colab needed.
# ============================================================

import os, re, time, logging, queue, threading, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib            import Path
from urllib.parse       import urljoin, urlparse

import pandas as pd
from bs4        import BeautifulSoup, Tag
from selenium   import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from tqdm       import tqdm

import gspread
from google.oauth2.service_account import Credentials

# ── Silence noisy loggers ────────────────────────────────────────────────────
logging.basicConfig(level=logging.CRITICAL)
for _lib in ("selenium", "urllib3", "WDM"):
    logging.getLogger(_lib).setLevel(logging.CRITICAL)

# ============================================================
# CONFIG  ← only section you ever need to edit
# ============================================================

# Read from GitHub Actions secrets (environment variables)
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1xCeo0hkn9N9mNKTnb2VdaYQ_77Q9eZEDcdghwrICw4w")
WORKSHEET_NAME = "website data"

DEFAULT_WAIT    = 8
DEFAULT_WORKERS = 4    # GitHub has full CPU — safe to use 4 workers
RETRY_WAIT      = 20
RETRY_WORKERS   = 2

TRIGGER_MODE           = "if_pending"  # scrape only rows missing image URL
SCHEDULE_INTERVAL_MINS = 0             # no loop — GitHub Actions handles scheduling

IS_COLAB  = False
IS_GITHUB = os.environ.get("GITHUB_ACTIONS") == "true"

# Scale workers based on pending count
# Small batch (< 50)  → 2 workers  (avoid overkill)
# Medium batch (< 200)→ 4 workers
# Large batch (200+)  → 6 workers  (GitHub can handle it)
def _get_workers(total):
    if total < 50:   return min(2, total)
    if total < 200:  return min(4, total)
    return min(6, total)

# ============================================================
# CONSTANTS
# ============================================================

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

IMG_ATTRS = ("src", "data-src", "data-lazy-src", "data-original",
             "data-full", "data-zoom-image", "data-img")
IMG_EXTS  = {".jpg", ".jpeg", ".png", ".webp", ".avif"}

JUNK_RE = re.compile(
    r"(logo|icon|favicon|badge|1x1|spacer|blank|placeholder|spinner"
    r"|social|share|\.svg|play\.png|sprite|tracking|pixel"
    r"|dealer.?logo|no.?image|noimage|default.?car|generic"
    r"|gallery.?image.?icon|image.?count|loading"
    r"|srp.?banner|drive.?into.?summer|drive.?into.?winter|uploads\.asbury"
    r"|sales.?event.?banner|promo.?banner|homepage.?banner"
    r"|slide.?bg|hero.?img|header.?img|site.?bg)",
    re.IGNORECASE,
)

VEHICLE_CDN_RE = re.compile(
    r"(vehicle-images\.carscommerce|inv\.assets\.|dealerinspire"
    r"|dealerpictures|homenetiol|homenetmedia|fleximages|flickfusion"
    r"|spincars|motortrend|evox|imagin\.studio|picture\.dealer"
    r"|img\.dealer|photos\.dealer|cloudfront.*inv|vinimage"
    r"|content\.speedshift|images\.dealer\.com|media\.dealer"
    r"|pictures\.dealer|img\.autotrader|vehicle\.capitalone)",
    re.IGNORECASE,
)

# ============================================================
# IMAGE HELPERS
# ============================================================

def is_valid_img(url):
    if not url or not url.startswith("http"):
        return False
    if JUNK_RE.search(url):
        return False
    if VEHICLE_CDN_RE.search(url):
        return True
    path = urlparse(url).path.lower()
    if Path(path).suffix in IMG_EXTS:
        return True
    if any(k in path for k in ("/photo", "/image", "/img", "/vehicle",
                                "/inventory", "/cars", "/stock",
                                "/media", "/content", "/cdn")):
        return True
    return False

def get_img_src(tag, base):
    for attr in IMG_ATTRS:
        val = (tag.get(attr) or "").strip()
        if val and not val.startswith("data:"):
            full = urljoin(base, val)
            if is_valid_img(full):
                return full
    return ""

# ============================================================
# STRATEGY 1 — VIN in image URL
# ============================================================

def find_by_vin_in_image_url(soup, vin, base):
    vin_lower = vin.lower()
    for tag in soup.find_all("img"):
        for attr in IMG_ATTRS:
            src = (tag.get(attr) or "").strip()
            if src and not src.startswith("data:"):
                full = urljoin(base, src)
                if vin_lower in full.lower() and is_valid_img(full):
                    return full
    return ""

# ============================================================
# STRATEGY 2 — JS Card Detector
# ============================================================

JS_CARD_DETECTOR = "\n".join([
    "(function(vin) {",
    "  var vinLower = vin.toLowerCase();",
    "  function getAttr(img) {",
    "    var a=['src','data-src','data-lazy-src','data-original','data-full','data-img'];",
    "    for(var i=0;i<a.length;i++){",
    "      var v=img.getAttribute(a[i])||'';",
    "      if(v&&v.indexOf('data:')!==0&&v.indexOf('http')===0)return v.trim();",
    "    }",
    "    return img.src||'';",
    "  }",
    "  function isJunk(s) {",
    "    s=s.toLowerCase();",
    "    var b=['logo','icon','favicon','badge','1x1','spacer','blank','placeholder',",
    "           'spinner','social','share','.svg','sprite','tracking','pixel',",
    "           'noimage','no-image','default-car','generic','loading',",
    "           'srp-banner','srpbanner','promo-banner','homepage-banner','uploads.asbury',",
    "           'slide-bg','hero-img','header-img'];",
    "    for(var i=0;i<b.length;i++)if(s.indexOf(b[i])!==-1)return true;",
    "    return false;",
    "  }",
    "  var anchor=null;",
    "  var inputs=document.querySelectorAll('input');",
    "  for(var i=0;i<inputs.length;i++){",
    "    if((inputs[i].value||'').toLowerCase().indexOf(vinLower)!==-1){anchor=inputs[i];break;}",
    "  }",
    "  if(!anchor){",
    "    var all=document.querySelectorAll('*');",
    "    for(var i=0;i<all.length;i++){",
    "      var el=all[i];",
    "      if(el.children.length>3)continue;",
    "      var txt=(el.innerText||el.textContent||'').trim().toLowerCase();",
    "      if(txt.indexOf(vinLower)!==-1&&txt.length<60){anchor=el;break;}",
    "    }",
    "  }",
    "  var resultsAnchor=null;",
    "  var all2=document.querySelectorAll('*');",
    "  for(var i=0;i<all2.length;i++){",
    "    var el=all2[i];",
    "    if(el.children.length>6)continue;",
    "    var txt=(el.innerText||el.textContent||'').trim();",
    "    if(txt.length>100)continue;",
    "    if(/[0-9]+ vehicle.{0,15}match|[0-9]+\\s+used|[0-9]+\\s+new|[0-9]+\\s+result|showing [0-9]+/i.test(txt)){resultsAnchor=el;break;}",
    "  }",
    "  var finalAnchor=null;",
    "  if(anchor&&resultsAnchor){",
    "    finalAnchor=(anchor.compareDocumentPosition(resultsAnchor)&4)?resultsAnchor:anchor;",
    "  } else { finalAnchor=anchor||resultsAnchor; }",
    "  var imgs=document.querySelectorAll('img');",
    "  for(var j=0;j<imgs.length;j++){",
    "    var img=imgs[j]; var src=getAttr(img);",
    "    if(!src||isJunk(src))continue;",
    "    if(finalAnchor&&!(finalAnchor.compareDocumentPosition(img)&4))continue;",
    "    var ir=img.getBoundingClientRect();",
    "    if(ir.width<60||ir.width>900)continue;",
    "    return src;",
    "  }",
    "  return '';",
    "})(arguments[0]);"
])

def find_by_card_detector(driver, vin):
    try:
        result = driver.execute_script(JS_CARD_DETECTOR, vin)
        if result and isinstance(result, str) and result.startswith("http"):
            if not JUNK_RE.search(result):
                return result
    except Exception:
        pass
    return ""

# ============================================================
# STRATEGY 3 — VIN Proximity
# ============================================================

def find_by_vin_proximity(soup, vin, base):
    vin_upper = vin.upper()
    SKIP_TAGS = {"input","textarea","select","button","script",
                 "style","meta","head","nav","header","footer"}
    SKIP_RE   = re.compile(
        r"(site.?header|site.?footer|site.?nav|top.?nav|main.?nav"
        r"|srp.?banner|promo.?banner|hero.?banner|modal.?overlay|advertisement)",
        re.IGNORECASE)

    vin_elements = []
    for tag in soup.find_all(True):
        if not isinstance(tag, Tag) or tag.name in SKIP_TAGS:
            continue
        direct = "".join(str(c) for c in tag.children
                         if not isinstance(c, Tag)).upper().strip()
        if vin_upper in direct:
            vin_elements.append(tag)

    for vin_el in vin_elements:
        anc_cls = " ".join(" ".join(p.get("class") or [])
                           for p in vin_el.parents if isinstance(p, Tag))
        if SKIP_RE.search(anc_cls):
            continue
        node = vin_el
        for _ in range(12):
            node = node.parent
            if node is None or node.name in ("html","body","[document]"):
                break
            if SKIP_RE.search(" ".join(node.get("class") or [])):
                break
            srcs = [get_img_src(img, base) for img in node.find_all("img")]
            srcs = [s for s in srcs if s]
            if srcs:
                for s in srcs:
                    if VEHICLE_CDN_RE.search(s): return s
                for s in srcs:
                    if vin_upper.lower() in s.lower(): return s
                return srcs[0]
    return ""

# ============================================================
# MAIN SCRAPE  (all strategies + scroll retries)
# ============================================================

JS_INPUT_ANCHOR = """
(function(vin) {
    var vinLower=vin.toLowerCase();
    function getAttr(img){
        var a=['src','data-src','data-lazy-src','data-original','data-full'];
        for(var i=0;i<a.length;i++){
            var v=img.getAttribute(a[i])||'';
            if(v&&v.indexOf('data:')!==0&&v.indexOf('http')===0)return v.trim();
        }
        return img.src||'';
    }
    function isJunk(s){
        s=s.toLowerCase();
        var b=['logo','icon','favicon','1x1','spacer','sprite','noimage',
               'placeholder','srp-banner','promo-banner','slide-bg','uploads.asbury'];
        for(var i=0;i<b.length;i++)if(s.indexOf(b[i])!==-1)return true;
        return false;
    }
    var anchor=null;
    var inputs=document.querySelectorAll('input');
    for(var i=0;i<inputs.length;i++){
        if((inputs[i].value||'').toLowerCase().indexOf(vinLower)!==-1){anchor=inputs[i];break;}
    }
    if(!anchor)return '';
    var imgs=document.querySelectorAll('img');
    for(var j=0;j<imgs.length;j++){
        var src=getAttr(imgs[j]);
        if(!src||isJunk(src))continue;
        var pos=anchor.compareDocumentPosition(imgs[j]);
        if(pos&4){var r=imgs[j].getBoundingClientRect();if(r.width>60&&r.width<900)return src;}
    }
    return '';
})(arguments[0]);
"""

def get_thumbnail(driver, url, vin, wait_secs):
    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    try:
        driver.get(url)
    except Exception as e:
        return "", f"load error: {e}"

    deadline = time.time() + wait_secs
    while time.time() < deadline:
        if vin.upper() in driver.page_source.upper(): break
        time.sleep(0.5)
    else:
        time.sleep(1)

    try:
        driver.execute_script("window.scrollTo(0,400);"); time.sleep(0.8)
        driver.execute_script("window.scrollTo(0,0);");   time.sleep(0.3)
    except Exception: pass

    soup = BeautifulSoup(driver.page_source, "lxml")

    img = find_by_vin_in_image_url(soup, vin, base)
    if img: return img, "VIN in img URL"

    img = find_by_card_detector(driver, vin)
    if img: return img, "card detector"

    img = find_by_vin_proximity(soup, vin, base)
    if img: return img, "VIN proximity"

    for scroll, label in [(500, "scroll"), (800, "late")]:
        try:
            driver.execute_script(f"window.scrollTo(0,{scroll});"); time.sleep(2)
            s = BeautifulSoup(driver.page_source, "lxml")
            for m, fn in [
                (f"VIN in img URL ({label})", lambda x: find_by_vin_in_image_url(x, vin, base)),
                (f"card detector ({label})",  lambda x: find_by_card_detector(driver, vin)),
                (f"VIN proximity ({label})",  lambda x: find_by_vin_proximity(x, vin, base)),
            ]:
                img = fn(s)
                if img: return img, m
        except Exception: pass

    try:
        img = driver.execute_script(JS_INPUT_ANCHOR, vin)
        if img and isinstance(img, str) and img.startswith("http"):
            return img, "input anchor"
    except Exception: pass

    return "", "not found"

# ============================================================
# CHROME DRIVER
# ============================================================

def _chrome_opts(headless=True):
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    for arg in [
        "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
        "--disable-software-rasterizer", "--window-size=1440,900",
        "--disable-blink-features=AutomationControlled",
        "--log-level=3", "--silent",
        "--disable-features=OptimizationGuideModelDownloading,"
        "OptimizationHintsFetching,OptimizationTargetPrediction,OptimizationHints",
        f"--user-agent={USER_AGENT}",
    ]:
        opts.add_argument(arg)
    opts.add_experimental_option("excludeSwitches", ["enable-automation","enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = "eager"
    # Container-safe flags (applies to GitHub Actions and Colab)
    for arg in [
        "--no-zygote", "--no-first-run", "--disable-extensions",
        "--disable-default-apps", "--disable-background-networking",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding", "--disable-sync",
        "--disable-translate", "--metrics-recording-only",
        "--mute-audio", "--no-default-browser-check",
        "--safebrowsing-disable-auto-update",
    ]:
        opts.add_argument(arg)
    return opts

def make_driver(headless=True):
    import shutil
    from webdriver_manager.chrome import ChromeDriverManager
    opts = _chrome_opts(headless)
    errors = []
    chrome = ("/usr/bin/google-chrome-stable"
              if os.path.isfile("/usr/bin/google-chrome-stable")
              else shutil.which("google-chrome-stable") or shutil.which("google-chrome"))
    if chrome:
        try:
            opts.binary_location = chrome
            svc = Service(ChromeDriverManager().install(), log_output=open(os.devnull,"w"))
            d = webdriver.Chrome(service=svc, options=opts)
            d.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
                {"source":"Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
            return d
        except Exception as e: errors.append(f"wdm: {e}")
        try:
            opts2 = _chrome_opts(headless); opts2.binary_location = chrome
            d = webdriver.Chrome(options=opts2)
            d.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
                {"source":"Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
            return d
        except Exception as e: errors.append(f"selenium-mgr: {e}")
    try:
        d = webdriver.Chrome(options=_chrome_opts(headless))
        return d
    except Exception as e: errors.append(f"PATH: {e}")
    raise RuntimeError(
        "Chrome failed to start.\n"
        "Run Cell 1 first, then retry.\n"
        "Errors:\n" + "\n".join(f"  • {e}" for e in errors))

def verify_driver():
    print("  Pre-flight check...", end=" ", flush=True)
    d = make_driver(); d.get("about:blank"); d.quit()
    print("OK")

# ============================================================
# SHEET WRITER  (live background updates)
# ============================================================

# ============================================================
# TRIGGER HELPERS
# ============================================================

def check_trigger(worksheet, gc) -> tuple:
    """
    Returns (should_run: bool, reason: str).

    if_pending  → True if any row has VIN + URL but no image URL
    sheet_flag  → True if Apps Script wrote "RUN" to _trigger!A1
    manual      → always True
    schedule    → always True (loop handles timing)
    """
    if TRIGGER_MODE == "manual" or TRIGGER_MODE == "schedule":
        return True, "manual run"

    if TRIGGER_MODE == "if_pending":
        records = worksheet.get_all_records()
        pending = [r for r in records
                   if r.get("Vin") and r.get("Website URL")
                   and not r.get("Website Image URL")]
        if pending:
            return True, f"{len(pending)} rows pending"
        return False, "no pending rows — sheet is up to date"

    if TRIGGER_MODE == "sheet_flag":
        try:
            ss       = gc.open_by_key(SPREADSHEET_ID)
            tsheet   = ss.worksheet("_trigger")
            flag     = str(tsheet.acell("A1").value or "").strip().upper()
            pending  = tsheet.acell("B1").value or 0
            if flag == "RUN":
                return True, f"Apps Script flag = RUN  ({pending} pending)"
            return False, f"Apps Script flag = {flag} — nothing to do"
        except Exception as e:
            return False, f"_trigger sheet not found ({e}) — run setupTriggers() in Apps Script"

    return True, "unknown mode — running anyway"


def clear_sheet_flag(gc):
    """Called after a successful run to reset the Apps Script flag to IDLE."""
    if TRIGGER_MODE != "sheet_flag":
        return
    try:
        ss     = gc.open_by_key(SPREADSHEET_ID)
        tsheet = ss.worksheet("_trigger")
        tsheet.update("A1", [["IDLE"]])
        tsheet.update("B1", [[0]])
        tsheet.update("C1", [[f"cleared: {time.strftime('%Y-%m-%d %H:%M:%S')}"]])
        print("  _trigger flag reset to IDLE")
    except Exception as e:
        print(f"  Warning: could not clear flag: {e}")


def _col_letter(n):
    result = ""
    while n:
        n, r = divmod(n-1, 26)
        result = chr(65+r) + result
    return result

class SheetWriter:
    FLUSH_EVERY = 5
    FLUSH_SECS  = 4

    def __init__(self, ws, col_letter):
        self._ws   = ws
        self._col  = col_letter
        self._q    = queue.Queue()
        self._stop = threading.Event()
        self._t    = threading.Thread(target=self._run, daemon=True)
        self._t.start()

    def put(self, row, url):
        self._q.put((row, url))

    def _flush(self, pending):
        if not pending: return
        try:
            self._ws.batch_update([
                {"range": f"{self._col}{r}", "values": [[u]]}
                for r, u in pending])
        except Exception as e:
            tqdm.write(f"  [SheetWriter] {e}")

    def _run(self):
        pending = []; last = time.time()
        while not self._stop.is_set() or not self._q.empty():
            try:
                pending.append(self._q.get(timeout=0.5))
                if len(pending) >= self.FLUSH_EVERY:
                    self._flush(pending); pending = []; last = time.time()
            except queue.Empty:
                if pending and time.time()-last >= self.FLUSH_SECS:
                    self._flush(pending); pending = []; last = time.time()
        self._flush(pending)

    def stop(self):
        self._stop.set(); self._t.join()

# ============================================================
# WORKER
# ============================================================

def worker(rows, wait_secs, results, pbar, counters, lock, sw, row_map):
    driver = make_driver()
    try:
        for row in rows:
            vin = str(row["vin"]).strip()
            url = str(row["url"]).strip()
            if not vin or not url: continue

            img, method = get_thumbnail(driver, url, vin, wait_secs)
            found = bool(img)
            entry = {"VIN": vin, "Page URL": url, "Thumbnail URL": img,
                     "Status": "Found" if found else "Not found", "Method": method}

            with lock:
                results.append(entry)
                if found:
                    counters["found"] += 1
                    tqdm.write(f"  ✓  {vin}  [{method}]")
                    tqdm.write(f"      {img}")
                    sr = row_map.get(vin.upper())
                    if sr: sw.put(sr, img)
                else:
                    counters["failed"] += 1
                    tqdm.write(f"  ✗  {vin}")
                pbar.set_postfix({"✓": counters["found"], "✗": counters["failed"]})
                pbar.update(1)
    finally:
        driver.quit()

def retry_worker(rows, results_map, pbar, counters, lock, sw, row_map):
    driver = make_driver()
    try:
        for row in rows:
            vin = str(row["vin"]).strip()
            url = str(row["url"]).strip()
            if not vin or not url: pbar.update(1); continue
            try: driver.get(url)
            except Exception: pbar.update(1); continue

            deadline = time.time() + RETRY_WAIT
            while time.time() < deadline:
                if vin.upper() in driver.page_source.upper(): break
                time.sleep(0.5)
            else: time.sleep(2)

            try:
                for y in (300,700,1200,1800,1200,700,0):
                    driver.execute_script(f"window.scrollTo(0,{y});"); time.sleep(0.6)
            except Exception: pass

            img, method = get_thumbnail(driver, url, vin, wait_secs=0)
            found = bool(img)
            with lock:
                entry = results_map.get(vin.upper())
                if entry and found:
                    entry.update({"Thumbnail URL": img, "Status": "Found",
                                  "Method": f"retry:{method}"})
                    counters["found"] += 1; counters["failed"] -= 1
                    tqdm.write(f"  ↺  {vin}  [retry:{method}]")
                    tqdm.write(f"      {img}")
                    sr = row_map.get(vin.upper())
                    if sr: sw.put(sr, img)
                else:
                    tqdm.write(f"  ✗  {vin}  still not found")
                pbar.update(1)
    finally:
        driver.quit()

# ============================================================
# MAIN
# ============================================================

def main():
    env = "GitHub Actions" if IS_GITHUB else "Local"
    print(f"\n  VIN Thumbnail Scraper  ─  {env}")
    print(f"  Trigger mode : {TRIGGER_MODE}")
    print(f"  {'─'*52}")
    verify_driver()
    print()

    # Auth via Google Service Account (stored as GOOGLE_CREDENTIALS env var)
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS environment variable not set.\n"
            "Add your service account JSON as a GitHub secret named GOOGLE_CREDENTIALS."
        )
    creds_info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    gc = gspread.Client(auth=creds)

    # ── Schedule loop ─────────────────────────────────────────────────────────
    # TRIGGER_MODE = "schedule" keeps looping; all other modes run once.
    run_number = 0
    while True:
        run_number += 1
        if TRIGGER_MODE == "schedule" and run_number > 1:
            print(f"\n  Waiting {SCHEDULE_INTERVAL_MINS} min before next check …")
            time.sleep(SCHEDULE_INTERVAL_MINS * 60)

        sheet     = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.worksheet(WORKSHEET_NAME)

        # ── Condition check ───────────────────────────────────────────────────
        should_run, reason = check_trigger(worksheet, gc)
        print(f"  Trigger check : {reason}")

        if not should_run:
            print("  Nothing to do — exiting.\n")
            if TRIGGER_MODE != "schedule":
                break
            continue

        full_df = pd.DataFrame(worksheet.get_all_records())

        # Input rows
        df = (full_df
              .rename(columns={"Vin":"vin","Website URL":"url"})[["vin","url"]]
              .dropna(subset=["url"]))
        df["url"] = df["url"].str.strip()
        df = df[df["url"] != ""].reset_index(drop=True)

        # In if_pending / sheet_flag mode — only process rows without an image
        if TRIGGER_MODE in ("if_pending", "sheet_flag"):
            img_col_vals = full_df.get("Website Image URL", pd.Series(dtype=str)).fillna("")
            df = df[img_col_vals.reindex(df.index, fill_value="").values == ""].reset_index(drop=True)
            print(f"  Pending rows  : {len(df)} (skipping already-scraped)")

        rows      = df.to_dict("records")
        total     = len(rows)
        n_workers = _get_workers(total)

        if total == 0:
            print("  All rows already have images — nothing to scrape.\n")
            clear_sheet_flag(gc)
            if TRIGGER_MODE != "schedule": break
            continue

        # Sheet column setup
        headers = worksheet.row_values(1)
        IMG_COL = "Website Image URL"
        if IMG_COL not in headers:
            headers.append(IMG_COL)
            worksheet.update_cell(1, len(headers), IMG_COL)
        img_col_letter = _col_letter(headers.index(IMG_COL) + 1)
        vin_col        = headers.index("Vin") + 1
        all_vins       = worksheet.col_values(vin_col)
        row_map        = {str(v).strip().upper(): i
                          for i, v in enumerate(all_vins, 1)
                          if i > 1 and str(v).strip()}

        print(f"  Total      : {total} URLs")
        print(f"  Workers    : {n_workers}  |  Wait: {DEFAULT_WAIT}s")
        print(f"  Img column : {IMG_COL} ({img_col_letter})")
        print(f"  Live write : every {SheetWriter.FLUSH_EVERY} finds or {SheetWriter.FLUSH_SECS}s\n")

        results  = []; counters = {"found":0,"failed":0}
        lock     = threading.Lock()
        sw       = SheetWriter(worksheet, img_col_letter)
        pbar     = tqdm(total=total, ncols=70, colour="green",
                        bar_format="  {l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")
        t0       = time.time()

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = [pool.submit(worker, rows[i::n_workers], DEFAULT_WAIT,
                                   results, pbar, counters, lock, sw, row_map)
                       for i in range(n_workers)]
            for f in as_completed(futures):
                if f.exception(): tqdm.write(f"  Worker error: {f.exception()}")

        pbar.close(); sw.stop()

        # Retry pass
        failed1 = [r for r in results if r["Status"] == "Not found"]
        if failed1:
            print(f"\n  {'─'*52}")
            print(f"  Pass 1: {len(failed1)} not found — retrying ({RETRY_WAIT}s wait) …\n")
            rmap      = {r["VIN"].upper(): r for r in results}
            rrows     = [{"vin":r["VIN"],"url":r["Page URL"]} for r in failed1]
            nr        = min(RETRY_WORKERS, len(rrows))
            sw2       = SheetWriter(worksheet, img_col_letter)
            rpbar     = tqdm(total=len(rrows), ncols=70, colour="yellow",
                             bar_format="  {l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
            with ThreadPoolExecutor(max_workers=nr) as pool:
                futures = [pool.submit(retry_worker, rrows[i::nr], rmap,
                                       rpbar, counters, lock, sw2, row_map)
                           for i in range(nr)]
                for f in as_completed(futures):
                    if f.exception(): tqdm.write(f"  Retry error: {f.exception()}")
            rpbar.close(); sw2.stop()

        elapsed = time.time() - t0

        # Safety-net full rewrite
        rmap_final = {r["VIN"]: r["Thumbnail URL"] for r in results}
        full_df[IMG_COL] = full_df["Vin"].map(rmap_final)
        full_df = full_df.fillna("").astype(str)
        worksheet.clear()
        worksheet.update([full_df.columns.tolist()] + full_df.values.tolist())

        # Clear Apps Script flag
        clear_sheet_flag(gc)

        # Summary
        still_failed = [r for r in results if r["Status"] == "Not found"]
        retry_found  = len(failed1) - len(still_failed) if failed1 else 0
        print(f"\n  {'─'*52}")
        print(f"  ✓  Found  (pass 1) : {counters['found'] - retry_found}")
        if failed1: print(f"  ↺  Found  (retry)  : {retry_found}")
        print(f"  ✓  Found  (total)  : {counters['found']}")
        print(f"  ✗  Not found       : {len(still_failed)}")
        if still_failed:
            print(f"\n  Still-failed VINs:")
            for r in still_failed: print(f"    • {r['VIN']}  →  {r['Page URL']}")
        print(f"\n  Time  : {elapsed:.1f}s")
        print(f"  Google Sheet updated ✓")

        if TRIGGER_MODE != "schedule":
            break

        print(f"\n  Next check in {SCHEDULE_INTERVAL_MINS} min  (stop cell to exit)\n")

# ============================================================
# RUN
# ============================================================

# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
