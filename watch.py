#!/usr/bin/env python3
"""Train seat watcher, run on a schedule by GitHub Actions.

For every watch in config.json that is due: checks seat availability on each
train, re-checks longer segments of the same train (earlier boarding / later
drop), lists special trains, and sends a phone alert (ntfy) when something new
opens. Writes README.md (status page), history.json and state.json.

Data: ConfirmTkt's public search endpoints (unofficial, cached availability).
Always confirm on IRCTC before booking.

  python3 watch.py            run the watches that are due
  python3 watch.py --force    run every enabled watch now
  python3 watch.py --dry-run  run everything, send nothing, save no state
"""
import json
import os
import re
import sys
import time
import threading
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
API = "https://cttrainsapi.confirmtkt.com/api/v1/trains"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
WORKERS = 4
PAUSE = 0.2
IST = timezone(timedelta(hours=5, minutes=30))
DIRS = ("Onward", "Return")
ORDER = {"direct": 0, "extended": 1, "nearby": 2}


def load(name, default):
    try:
        with open(os.path.join(HERE, name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def save(name, data):
    with open(os.path.join(HERE, name), "w") as f:
        json.dump(data, f, indent=1, sort_keys=True)
        f.write("\n")


def http_json(url, tries=3):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last = e
            time.sleep(2 * (i + 1))
    raise RuntimeError("%s -> %s" % (url.split("?")[0], last))


def now_ist():
    return datetime.now(IST)


def days(start, end):
    a = datetime.strptime(start, "%Y-%m-%d")
    b = max(a, datetime.strptime(end, "%Y-%m-%d"))
    return [(a + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(min((b - a).days + 1, 14))]


def shift(date, n):
    return (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=n)).strftime("%Y-%m-%d")


def pretty(date):
    return datetime.strptime(date, "%Y-%m-%d").strftime("%a %d %b")


def parse(av):
    """-> (kind, seats). kind: AVL, RAC, WL, NO."""
    s = (av or "").upper()
    m = re.search(r"(?:AVAILABLE|AVL|CURR_AVBL)[-\s#]*0*(\d+)", s)
    if m:
        return "AVL", int(m.group(1))
    m = re.search(r"RAC\s*0*(\d+)", s)
    if m:
        return "RAC", int(m.group(1))
    m = re.search(r"WL\s*0*(\d+)\s*$", s) or re.search(r"WL\s*0*(\d+)", s)
    if m:
        return "WL", int(m.group(1))
    return "NO", 0


def age(cache_time):
    try:
        t = datetime.strptime(cache_time[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=IST)
        mins = max(0, int((now_ist() - t).total_seconds() // 60))
        return "%dm" % mins if mins < 60 else ("%dh" % (mins // 60) if mins < 2880 else "%dd" % (mins // 1440))
    except Exception:
        return "?"


class Check:
    """One full check of one watch."""

    def __init__(self, watch, schedules):
        self.w = watch
        self.schedules = schedules
        self.cache = {}
        self.lock = threading.Lock()
        self.errors = []
        self.opts = []
        self.specials = {}   # (train, from, to, date) -> train entry
        self.spec_dir = {}   # train number -> direction
        self.pool = ThreadPoolExecutor(WORKERS)

    def search(self, key):
        src, dst, date = key
        with self.lock:
            if key in self.cache:
                return self.cache[key]
        q = urllib.parse.urlencode({
            "sourceStationCode": src, "destinationStationCode": dst,
            "dateOfJourney": datetime.strptime(date, "%Y-%m-%d").strftime("%d-%m-%Y"),
            "addAvailabilityCache": "true", "excludeMultiTicketAlternates": "false",
            "excludeBoostAlternates": "false", "sortBy": "DEFAULT", "enableNearby": "true",
            "enableTG": "true", "tGPlan": "CTG-15", "showTGPrediction": "false",
            "tgColor": "DEFAULT", "showPredictionGlobal": "true"})
        try:
            data = http_json(API + "/search?" + q).get("data") or {}
            res = (data.get("trainList") or [], data.get("nearbyTrains") or [])
        except Exception as e:
            self.errors.append(str(e))
            res = ([], [])
        time.sleep(PAUSE)
        with self.lock:
            self.cache[key] = res
        return res

    def fetch(self, keys):
        return list(self.pool.map(self.search, keys))

    def halts(self, train):
        c = self.schedules.get(train)
        if c and time.time() - c["at"] < 7 * 86400:
            return c["halts"]
        try:
            d = http_json(API + "/schedule?trainNo=" + train)
            halts = [{"code": s["StationCode"], "day": int(s["Day"])} for s in d.get("Schedule") or []]
        except Exception as e:
            self.errors.append(str(e))
            return c["halts"] if c else []
        if halts:
            self.schedules[train] = {"at": time.time(), "halts": halts}
        return halts

    def options(self, t, date, typ, note, direction, travel):
        cache = t.get("availabilityCache") or {}
        for cls in self.w["classes"]:
            v = cache.get(cls)
            if not v:
                continue
            kind, n = parse(v.get("availability"))
            self.opts.append({
                "train": t["trainNumber"], "name": (t.get("trainName") or "").strip(),
                "from": t["fromStnCode"], "to": t["toStnCode"], "date": date, "travel": travel,
                "dep": t.get("departureTime") or "", "cls": cls, "raw": v.get("availability") or "",
                "kind": kind, "seats": n, "fare": v.get("fare") or "", "age": age(v.get("cacheTime") or ""),
                "type": typ, "note": note, "dir": direction})

    def note_specials(self, trains, date, direction):
        for t in trains:
            if t["trainNumber"].startswith("0"):
                self.specials[(t["trainNumber"], t["fromStnCode"], t["toStnCode"], date)] = t
                self.spec_dir.setdefault(t["trainNumber"], direction)

    def run(self):
        w = self.w
        legs = [("Onward", w["from"], w["to"], days(w["start"], w["end"]), w.get("special_watch_origins", []))]
        ret = w.get("return")
        if ret:
            for t in w["to"]:
                legs.append(("Return", t, [w["from"]], days(ret["start"], ret["end"]), []))

        for direction, src, dests, dates, extra_origins in legs:
            base = [(src, d, dt) for dt in dates for d in dests]
            trains = {}
            for (s, d, dt), (direct, nearby) in zip(base, self.fetch(base)):
                self.note_specials(direct + nearby, dt, direction)
                for t in direct:
                    self.options(t, dt, "direct", "", direction, dt)
                    trains.setdefault((t["trainNumber"], dt), set()).add(d)
                for t in nearby:
                    self.options(t, dt, "nearby", "nearby station: %s->%s" % (t["fromStnCode"], t["toStnCode"]),
                                 direction, dt)

            # Longer segments of the same train: earlier boarding and/or later drop.
            jobs = {}
            for (no, dt), targets in sorted(trains.items()):
                halts = self.halts(no)
                codes = [h["code"] for h in halts]
                if src not in codes:
                    continue
                i = codes.index(src)
                served = [k for k in range(i + 1, len(codes)) if codes[k] in targets]
                if not served:
                    continue
                j, last = served[-1], len(codes) - 1
                earlier = ([0] + list(range(max(1, i - 3), i))) if i > 0 else []
                later = list(range(j + 1, min(len(codes), j + 3)))
                if j < last and last not in later:
                    later.append(last)

                def add(a, b, no=no, dt=dt, halts=halts, codes=codes, i=i, j=j):
                    d2 = shift(dt, halts[a]["day"] - halts[i]["day"])
                    parts = []
                    if a != i:
                        parts.append("change boarding point to %s" % src)
                    if b != j:
                        parts.append("get down at %s" % codes[j])
                    note = "book %s->%s on %s, %s" % (codes[a], codes[b], pretty(d2), ", ".join(parts))
                    jobs.setdefault((codes[a], codes[b], d2), []).append((no, dt, note))

                for a in earlier:
                    add(a, j)
                for b in later:
                    add(i, b)
                if earlier and later:
                    add(0, last)

            keys = sorted(jobs)
            for key, (direct, nearby) in zip(keys, self.fetch(keys)):
                self.note_specials(direct, key[2], direction)
                for no, dt, note in jobs[key]:
                    for t in direct:
                        if t["trainNumber"] == no:
                            self.options(t, key[2], "extended", note, direction, dt)

            # Extra origins, only to spot special trains heading to the same places.
            extra = [(o, d, dt) for dt in dates for o in extra_origins for d in (dests[0], dests[-1]) if o != src]
            for (o, d, dt), (direct, nearby) in zip(extra, self.fetch(extra)):
                self.note_specials(direct, dt, direction)

        self.pool.shutdown()
        # Plain bookings win over longer-segment or nearby duplicates of the same thing.
        self.opts.sort(key=lambda o: ORDER[o["type"]])
        seen, uniq = set(), []
        for o in self.opts:
            k = (o["train"], o["from"], o["to"], o["date"], o["cls"])
            if k not in seen:
                seen.add(k)
                uniq.append(o)
        self.opts = uniq
        return self


# ---------- text ----------

def fits(o, need):
    return o["kind"] == "AVL" and o["seats"] >= need


def rank_key(o, need, classes):
    return (not fits(o, need), o["kind"] != "AVL", ORDER[o["type"]], o["date"],
            classes.index(o["cls"]) if o["cls"] in classes else 9, -o["seats"])


def line(o, need):
    tag = "FULL GROUP" if fits(o, need) else ("RAC" if o["kind"] == "RAC" else "partial")
    s = "%s | %s %s | %s->%s dep %s\n   %s: %d seats (%s) Rs%s, data %s old" % (
        pretty(o["date"]), o["train"], o["name"], o["from"], o["to"], o["dep"],
        o["cls"], o["seats"], tag, o["fare"], o["age"])
    if o["note"]:
        s += "\n   -> " + o["note"]
    return s


def special_rows(c, direction):
    w = c.w
    home = {w["from"]} if direction == "Onward" else set(w["to"])
    grouped = {}
    for (no, f, t, dt), tr in c.specials.items():
        if c.spec_dir.get(no) == direction:
            grouped.setdefault(no, []).append((dt, f, t, tr))
    rows = []
    for no in sorted(grouped, key=lambda n: min(r[0] for r in grouped[n])):
        items = sorted(grouped[no], key=lambda r: (r[0], r[1]))
        pick = ([r for r in items if r[1] in home] or items)[0]
        cache = pick[3].get("availabilityCache") or {}
        cells = ["%s %s" % (k, cache[k].get("availability")) for k in w["classes"]
                 if k in cache and cache[k].get("availability")]
        boards = []
        for dt, f, t, x in items:
            b = "%s %s %s" % (f, pretty(dt), x.get("departureTime") or "")
            if b not in boards:
                boards.append(b)
        rows.append((no, (pick[3].get("trainName") or "").strip(), ", ".join(cells) or "no data", ", ".join(boards)))
    return rows


def alert_body(c, new, new_spec, hits, need):
    """Alert text split into ONWARD and RETURN blocks."""
    direct_full = {(o["train"], o["date"], o["cls"]) for o in hits if o["type"] == "direct" and fits(o, need)}
    blocks, shown = [], 0
    for d in DIRS:
        rows = [o for o in new if o["dir"] == d and not (
            o["type"] != "direct" and (o["train"], o["travel"], o["cls"]) in direct_full)]
        rows.sort(key=lambda o: rank_key(o, need, c.w["classes"]))
        sp = [k for k in new_spec if c.spec_dir.get(k.split()[0]) == d]
        if not rows and not sp:
            continue
        shown += len(rows)
        b = ["===== %s =====" % d.upper()] + [line(o, need) for o in rows[:8]]
        if len(rows) > 8:
            b.append("(+%d more on the status page)" % (len(rows) - 8))
        if sp:
            b += ["New special trains:"] + ["  " + k for k in sp[:10]]
        blocks.append("\n".join(b))
    return "\n\n".join(blocks), shown


def status_md(c, need):
    """Markdown status for one watch."""
    w, out = c.w, []
    hits = sorted([o for o in c.opts if o["kind"] in ("AVL", "RAC")], key=lambda o: rank_key(o, need, w["classes"]))
    for d in DIRS:
        if d == "Return" and not w.get("return"):
            continue
        out += ["### %s" % d, ""]
        mine = [o for o in hits if o["dir"] == d]
        out.append("**Open now: %d**" % len(mine))
        out.append("")
        if mine:
            out += ["| Date | Train | Segment | Dep | Class | Seats | Fare | Data age | How |", "|---|---|---|---|---|---|---|---|---|"]
            for o in mine[:40]:
                seats = ("**%d** (all %d)" % (o["seats"], need)) if fits(o, need) else (
                    "RAC %d" % o["seats"] if o["kind"] == "RAC" else str(o["seats"]))
                out.append("| %s | %s %s | %s→%s | %s | %s | %s | ₹%s | %s | %s |" % (
                    pretty(o["date"]), o["train"], o["name"], o["from"], o["to"], o["dep"], o["cls"], seats,
                    o["fare"], o["age"], o["note"] or "direct"))
            if len(mine) > 40:
                out.append("")
                out.append("…and %d more." % (len(mine) - 40))
            out.append("")

        by = {}
        for o in c.opts:
            if o["type"] == "direct" and o["dir"] == d:
                by.setdefault((o["date"], o["dep"], o["train"], o["name"], o["from"], o["to"]), []).append(o)
        merged = {}
        for k in sorted(by):
            cells = ", ".join("%s %s" % (o["cls"], o["raw"] if o["kind"] != "NO" else "Regret") for o in
                              sorted(by[k], key=lambda o: w["classes"].index(o["cls"])))
            merged.setdefault(k[:5] + (cells,), []).append(k[5])
        out += ["<details><summary>Waitlist status (%d rows)</summary>" % len(merged), "",
                "| Date | Train | Segment | Dep | Status |", "|---|---|---|---|---|"]
        for k in sorted(merged):
            out.append("| %s | %s %s | %s→%s | %s | %s |" % (pretty(k[0]), k[2], k[3], k[4], "/".join(merged[k]), k[1], k[5]))
        out += ["", "</details>", ""]

        sp = special_rows(c, d)
        if sp:
            out += ["**Special trains: %d**" % len(sp), "", "| Train | Status | Boards at |", "|---|---|---|"]
            out += ["| %s %s | %s | %s |" % r for r in sp]
            out.append("")
    return out, hits


def notify(topic, title, body, urgent):
    if not topic:
        return False
    req = urllib.request.Request(
        "https://ntfy.sh/" + urllib.parse.quote(topic), data=body[:3800].encode("utf-8"),
        headers={"Title": title.encode("ascii", "ignore").decode(), "Tags": "train",
                 "Priority": "urgent" if urgent else "default"})
    try:
        urllib.request.urlopen(req, timeout=20).read()
        return True
    except Exception as e:
        print("ntfy failed:", e)
        return False


def main():
    cfg = load("config.json", None)
    if not cfg:
        sys.exit("config.json missing or invalid")
    force, dry = "--force" in sys.argv, "--dry-run" in sys.argv
    state = load("state.json", {})
    history = load("history.json", [])
    schedules = load("schedules.json", {})
    sections = state.setdefault("sections", {})
    topic = os.environ.get("NTFY_TOPIC") or cfg.get("ntfy_topic", "")
    now = time.time()

    for w in cfg["watches"]:
        wid = w["id"]
        st = state.setdefault("watches", {}).setdefault(wid, {})
        title = "%s → %s" % (w["from"], ", ".join(w["to"]))
        if not w.get("enabled", True):
            st["status"] = "Paused"
            continue
        # A scheduled start can be a few minutes early or late, so allow some slack.
        if not (force or dry) and now - st.get("last_run", 0) < w.get("interval_minutes", 60) * 60 - 420:
            continue
        print("Checking %s ..." % title)
        c = Check(w, schedules).run()
        need = w["passengers"]
        st["last_run"] = now
        st["searches"], st["errors"] = len(c.cache), len(c.errors)
        print("  %d searches, %d errors, %d options" % (len(c.cache), len(c.errors), len(c.opts)))
        if not c.opts:
            st["status"] = ("No trains found. Check the station codes and dates." if not c.errors
                            else "Could not reach the data source (%s)" % c.errors[0][:120])
            continue
        md, hits = status_md(c, need)
        st["status"] = "OK"
        st["open"] = len(hits)
        sections[wid] = md

        cur = {"|".join([o["train"], o["from"], o["to"], o["date"], o["cls"]]): o for o in hits}
        old = st.get("hits", {})
        new = [o for k, o in cur.items() if k not in old or o["seats"] > old[k]]
        spec_keys = sorted({"%s %s" % (k[0], (v.get("trainName") or "").strip()) for k, v in c.specials.items()})
        new_spec = [k for k in spec_keys if k not in st.get("specials", [])]
        body, shown = alert_body(c, new, new_spec, hits, need)
        print("  %d new option(s), %d new special(s)" % (len(new), len(new_spec)))
        if dry:
            print(body)
            continue
        if body:
            full = any(fits(o, need) for o in new)
            head = ("Seats for all %d" % need) if full else (
                "%d new seat option(s)" % shown if shown else "New special train(s)")
            sent = notify(topic, "%s - %s" % (head, title.replace("→", "->")), body, full)
            history.append({"at": now, "watch": wid, "route": title, "title": head, "urgent": full,
                            "body": body, "sent": sent})
        st["hits"] = {k: o["seats"] for k, o in cur.items()}
        st["specials"] = sorted(set(st.get("specials", [])) | set(spec_keys))

    if dry:
        return
    history = history[-200:]
    save("state.json", state)
    save("history.json", history)
    save("schedules.json", schedules)

    # Status page
    stamp = now_ist().strftime("%d %b %Y, %H:%M IST")
    out = ["# Train watch", "", "Updated %s. Availability is ConfirmTkt's cached copy; confirm on IRCTC before booking." % stamp,
           "", "| Watch | Status | Open options | Last check | Checks every |", "|---|---|---|---|---|"]
    for w in cfg["watches"]:
        st = state["watches"].get(w["id"], {})
        last = datetime.fromtimestamp(st["last_run"], IST).strftime("%d %b %H:%M") if st.get("last_run") else "not yet"
        ok = st.get("status", "Waiting for first check")
        icon = "🟢 Running" if ok == "OK" else ("⏸ Paused" if ok == "Paused" else "🟠 " + ok)
        out.append("| **%s → %s** | %s | %s | %s | %d min |" % (
            w["from"], ", ".join(w["to"]), icon, st.get("open", "–"), last, w.get("interval_minutes", 60)))
    out.append("")
    out += ["## Notifications sent", ""]
    if not history:
        out += ["None yet.", ""]
    for h in reversed(history[-15:]):
        when = datetime.fromtimestamp(h["at"], IST).strftime("%d %b %H:%M")
        out += ["<details><summary>%s <b>%s</b> — %s (%s)%s</summary>" % (
            "✅" if h["urgent"] else "🔔", h["title"], h["route"], when, "" if h["sent"] else " · not sent to phone"),
            "", "```", h["body"], "```", "", "</details>", ""]
    for w in cfg["watches"]:
        if w["id"] in sections:
            dates = "%s to %s" % (pretty(w["start"]), pretty(w["end"]))
            if w.get("return"):
                dates += ", return %s to %s" % (pretty(w["return"]["start"]), pretty(w["return"]["end"]))
            out += ["## %s → %s" % (w["from"], ", ".join(w["to"])), "",
                    "%s · %d passengers · %s" % (dates, w["passengers"], "/".join(w["classes"])), ""]
            out += sections[w["id"]]
    with open(os.path.join(HERE, "README.md"), "w") as f:
        f.write("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
