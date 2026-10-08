import hmac, json, os, re, sqlite3, threading, time
from concurrent.futures import ThreadPoolExecutor
import paramiko
from flask import Flask, Response, jsonify, request, send_file

DATA = os.environ.get("DATA_DIR", "/data")
CFG, SET, DB, DEV = (os.path.join(DATA, n) for n in ("config.json", "settings.json", "stats.db", "devices.json"))
# Read-only /proc reads work on every UniFi device; mca-dump adds model/firmware/client count where present.
CMD = "for f in uptime loadavg meminfo stat net/dev; do echo @@$f; cat /proc/$f; done; echo @@mca; mca-dump 2>/dev/null"
app, wake, dblock, prev = Flask(__name__), threading.Event(), threading.Lock(), {}


def cfg():
    try:
        with open(CFG) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}  # config.json is optional


def env_ssh():
    m = {"user": "SSH_USER", "password": "SSH_PASSWORD", "key_file": "SSH_KEY_FILE", "port": "SSH_PORT"}
    e = {k: os.environ[v] for k, v in m.items() if os.environ.get(v)}
    if "port" in e:
        e["port"] = int(e["port"])
    return e


def devices():
    try:
        with open(DEV) as f:
            return json.load(f)
    except FileNotFoundError:
        return cfg().get("devices", [])  # first run: seed from config.json, UI edits move it to devices.json


def save_devices(lst):
    with open(DEV, "w") as f:
        json.dump(lst, f, indent=2)


def interval():
    try:
        with open(SET) as f:
            return json.load(f)["interval"]
    except Exception:
        return cfg().get("interval", 30)


def sql(q, a=()):
    with dblock:
        db = sqlite3.connect(DB)
        try:
            rows = db.execute(q, a).fetchall()
            db.commit()
            return rows
        finally:
            db.close()


def ssh_run(d):
    if "user" not in d:
        raise ValueError("No SSH user set: use SSH_USER or the ssh block in config.json.")
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(d["host"], port=d.get("port", 22), username=d["user"], password=d.get("password"),
              key_filename=d.get("key_file"), timeout=8, look_for_keys=False, allow_agent=False)
    try:
        return c.exec_command(CMD, timeout=20)[1].read().decode(errors="replace")
    finally:
        c.close()


def parse(out, d):
    s = dict(re.findall(r"@@(\S+)\n(.*?)(?=\n@@|\Z)", out, re.S))
    mem = {k: int(v) for k, v in re.findall(r"(\w+):\s+(\d+)", s["meminfo"])}
    avail = mem.get("MemAvailable", mem["MemFree"] + mem.get("Buffers", 0) + mem.get("Cached", 0))
    cpu = list(map(int, s["stat"].split("\n")[0].split()[1:]))
    idle, total = cpu[3] + cpu[4], sum(cpu)
    rx = tx = 0
    for line in s["net/dev"].split("\n")[2:]:
        name, _, vals = line.partition(":")
        if re.match(d.get("ifaces", r"eth\d+$"), name.strip()):  # per-device regex; avoids double-counting bridges
            v = vals.split()
            rx, tx = rx + int(v[0]), tx + int(v[8])
    now, p = time.time(), prev.get(d["name"])
    r = {"ts": now, "uptime": float(s["uptime"].split()[0]), "load": float(s["loadavg"].split()[0]),
         "mem": round(100 * (1 - avail / mem["MemTotal"]), 1), "cpu": None, "rx": None, "tx": None}
    if p and now > p["ts"] and total > p["total"]:
        dt = now - p["ts"]
        r["cpu"] = round(100 * (1 - (idle - p["idle"]) / (total - p["total"])), 1)
        r["rx"] = max(0, round(8 * (rx - p["rx"]) / dt))
        r["tx"] = max(0, round(8 * (tx - p["tx"]) / dt))
    prev[d["name"]] = {"ts": now, "idle": idle, "total": total, "rx": rx, "tx": tx}
    try:
        m = json.loads(s.get("mca", ""))
        r.update(model=m.get("model"), version=m.get("version"),
                 clients=sum(v.get("num_sta", 0) for v in m.get("vap_table", [])))
    except ValueError:
        pass
    return r


def poll(d):
    try:
        r = parse(ssh_run(d), d)
    except Exception as e:
        r = {"ts": time.time(), "error": str(e)[:200]}
    sql("INSERT INTO samples VALUES (?,?,?)", (d["name"], r["ts"], json.dumps(r)))


def loop():
    sql("CREATE TABLE IF NOT EXISTS samples (dev TEXT, ts REAL, data TEXT)")
    sql("CREATE INDEX IF NOT EXISTS i ON samples (dev, ts)")
    while True:
        t0 = time.time()
        try:
            c = cfg()
            # Shared SSH login: config.json "ssh" block, overridden by SSH_* env vars; a device's own keys win.
            shared = {**c.get("ssh", {}), **env_ssh()}
            devs = [{**shared, **d} for d in devices()]
            with ThreadPoolExecutor(8) as ex:
                list(ex.map(poll, devs))
            sql("DELETE FROM samples WHERE ts < ?", (t0 - 86400 * c.get("retention_days", 7),))
        except Exception as e:
            print("poll error:", e, flush=True)
        wake.wait(max(1, interval() - (time.time() - t0)))
        wake.clear()


@app.get("/")
def index():
    return send_file("index.html")


@app.get("/api/data")
def data():
    mins, out = int(request.args.get("minutes", 60)), []
    for d in devices():  # passwords never leave the server
        rows = sql("SELECT data FROM samples WHERE dev=? AND ts>? ORDER BY ts", (d["name"], time.time() - mins * 60))
        series = [json.loads(r[0]) for r in rows]
        step = max(1, len(series) // 300)  # downsample long ranges, always keeping the newest sample
        out.append({"name": d["name"], "host": d["host"], "series": series[::-1][::step][::-1]})
    return jsonify(interval=interval(), devices=out)


@app.post("/api/settings")
def settings():
    n = max(5, min(3600, int(request.json["interval"])))
    with open(SET, "w") as f:
        json.dump({"interval": n}, f)
    wake.set()
    return jsonify(interval=n)


HOST = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.:-]{0,251}$")
AUTH = os.environ.get("UI_PASSWORD")


@app.before_request
def guard():  # optional login: set UI_PASSWORD (and UI_USER, default "admin") to switch it on
    a = request.authorization
    want = f"{os.environ.get('UI_USER', 'admin')}:{AUTH}".encode()
    if AUTH and not (a and hmac.compare_digest(f"{a.username}:{a.password or ''}".encode(), want)):
        return Response("Login required", 401, {"WWW-Authenticate": 'Basic realm="UniFi stats"'})


def fields():
    j = request.get_json(silent=True) or {}
    name, host = str(j.get("name", "")).strip()[:60], str(j.get("host", "")).strip()
    if not name or not HOST.match(host):
        raise ValueError("Enter a name and a valid IP address or hostname.")
    return name, host


def bad(m, code=400):
    return jsonify(error=m), code


@app.post("/api/devices")
def add_device():
    try:
        name, host = fields()
    except ValueError as e:
        return bad(str(e))
    lst = devices()
    if any(d["name"] == name for d in lst):
        return bad("A device with that name already exists.")
    save_devices(lst + [{"name": name, "host": host}])
    wake.set()  # poll right away
    return jsonify(ok=True)


@app.put("/api/devices/<path:old>")
def edit_device(old):
    try:
        name, host = fields()
    except ValueError as e:
        return bad(str(e))
    lst = devices()
    cur = next((d for d in lst if d["name"] == old), None)
    if not cur:
        return bad("Device not found.", 404)
    if name != old and any(d["name"] == name for d in lst):
        return bad("A device with that name already exists.")
    cur.update(name=name, host=host)
    save_devices(lst)
    if name != old:
        sql("UPDATE samples SET dev=? WHERE dev=?", (name, old))  # history follows the rename
    prev.pop(old, None)
    wake.set()
    return jsonify(ok=True)


@app.delete("/api/devices/<path:name>")
def delete_device(name):
    save_devices([d for d in devices() if d["name"] != name])
    sql("DELETE FROM samples WHERE dev=?", (name,))
    prev.pop(name, None)
    return jsonify(ok=True)


@app.post("/api/order")
def reorder():
    order = (request.get_json(silent=True) or {}).get("order")
    if not isinstance(order, list):
        return bad("Expected a list of device names.")
    pos = {str(n): i for i, n in enumerate(order)}
    lst = devices()
    lst.sort(key=lambda d: pos.get(d["name"], len(pos)))  # stable; devices not listed stay at the end
    save_devices(lst)
    return jsonify(ok=True)


threading.Thread(target=loop, daemon=True).start()
