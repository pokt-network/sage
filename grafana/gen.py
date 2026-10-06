"""Generate the SAGE dashboards (Grafana JSON) from one source.

    python3 grafana/gen.py public > grafana/sage-quality-public.json
    python3 grafana/gen.py public path-quality-dashboard-public-pinned > grafana/sage-quality-public-path.json
    python3 grafana/gen.py public pqdppf2 > grafana/sage-quality-public-pqdppf2.json
    python3 grafana/gen.py operator > grafana/sage-operator.json

operator is everything, for whoever runs the gateway: reputation internals,
breaker and drain state, retries, hedges, probes. public is its subset for
users and suppliers: outcomes (success, latency, errors, concentration),
never mechanics, which would teach a supplier how to game the scoring.
Panels and table columns marked ops-only are left out of public.
No template variables: Grafana public dashboards do not support them.
"""
import json
import sys

S = 'job="sage", environment="mainnet-sage"'
DS = {"type": "prometheus", "uid": "prometheus"}
RI = "$__rate_interval"


def rec(name, matchers=""):
    """A recorded rate: Prometheus precomputes rate(sage_<name>[4m]) every
    minute (pnf-ops, kps/alerts/dashboard-recording-rules.yaml), so a panel
    reads a few hundred series instead of re-summing 10k-41k raw ones on every
    view — loads took 2.5-8s before. The rules cover mainnet-sage only and keep
    environment, service_id, operator, rpc_type, attempt, attribution,
    request_type and le; renaming any of those empties these panels. Windows
    other than $__rate_interval ([1h], [24h]) stay on the raw series."""
    m = 'environment="mainnet-sage"' + (", " + matchers if matchers else "")
    return f"sage:{name}:rate4m{{{m}}}"

PUBLIC = sys.argv[1:2] == ["public"]
if sys.argv[1:2] not in (["public"], ["operator"]) or len(sys.argv) > 3 or (len(sys.argv) == 3 and not PUBLIC):
    sys.exit("usage: gen.py public [uid] | gen.py operator")
# The public dashboard lives at its own uid and also replaced two PATH public
# dashboards, each kept at its own uid so its URLs and share link keep
# working; the uid picks which.
PUBLIC_UID = sys.argv[2] if len(sys.argv) == 3 else "sage-quality-public"

panels = []
_id = [0]
# ops is set around operator-only panels: a panel added while it is true is
# left out of the public dashboard.
ops = [False]
y = [0]


def add(panel):
    if not (PUBLIC and ops[0]):
        panels.append(panel)


def nid():
    _id[0] += 1
    return _id[0]


def row(title):
    add({"collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y[0]},
                   "id": nid(), "panels": [], "title": title, "type": "row"})
    y[0] += 1


def target(expr, ref="A", legend=None, table=False, instant=False):
    t = {"datasource": DS, "editorMode": "code", "expr": expr, "refId": ref}
    if table:
        t.update({"format": "table", "instant": True, "range": False})
    elif instant:
        t.update({"instant": True, "range": False})
    else:
        t["range"] = True
    if legend:
        t["legendFormat"] = legend
    return t


def thresholds(*steps):
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in steps]}


def stat(title, expr, x, w, h=4, unit="short", desc="", steps=((0, "green"),), mn=None, mx=None,
         graph="area", legend=None, decimals=None, color="thresholds"):
    d = {"color": {"mode": color}, "mappings": [], "thresholds": thresholds(*steps), "unit": unit}
    if mn is not None:
        d["min"] = mn
    if mx is not None:
        d["max"] = mx
    if decimals is not None:
        d["decimals"] = decimals
    add({
        "datasource": DS, "description": desc, "type": "stat", "title": title, "id": nid(),
        "gridPos": {"h": h, "w": w, "x": x, "y": y[0]},
        "fieldConfig": {"defaults": d, "overrides": []},
        "options": {"colorMode": "value", "graphMode": graph, "justifyMode": "auto", "orientation": "auto",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "showPercentChange": False, "textMode": "auto", "wideLayout": True},
        # A tile with no sparkline shows only the last value: one instant
        # query, not a range evaluated at every step to throw all but one away.
        "targets": [target(expr, legend=legend or "__auto", instant=graph == "none")],
    })


def ts(title, targets, x, w, h=8, unit="short", desc="", stack=True, fill=30, mn=None, mx=None, steps=None,
       sort_mean=True, placement="right"):
    d = {
        "color": {"mode": "palette-classic-by-name"},
        "custom": {"drawStyle": "line", "fillOpacity": fill, "lineInterpolation": "smooth", "lineWidth": 1,
                   "showPoints": "never", "spanNulls": False,
                   "stacking": {"group": "A", "mode": "normal" if stack else "none"},
                   "thresholdsStyle": {"mode": "line" if steps else "off"}},
        "mappings": [], "thresholds": thresholds(*(steps or ((0, "green"),))), "unit": unit,
    }
    if mn is not None:
        d["min"] = mn
    if mx is not None:
        d["max"] = mx
    legend = {"calcs": ["mean", "max"], "displayMode": "table", "placement": placement, "showLegend": True}
    if sort_mean:
        legend.update({"sortBy": "Mean", "sortDesc": True})
    add({
        "datasource": DS, "description": desc, "type": "timeseries", "title": title, "id": nid(),
        "gridPos": {"h": h, "w": w, "x": x, "y": y[0]},
        "fieldConfig": {"defaults": d, "overrides": []},
        "options": {"legend": legend, "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [target(e, ref=chr(65 + i), legend=l) for i, (e, l) in enumerate(targets)],
    })


def pie(title, expr, legend, x, w, h=8, desc=""):
    add({
        "datasource": DS, "description": desc, "type": "piechart", "title": title, "id": nid(),
        "gridPos": {"h": h, "w": w, "x": x, "y": y[0]},
        "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}, "mappings": []}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "right", "showLegend": True,
                               "values": ["percent", "value"]},
                    "pieType": "donut", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "sort": "desc", "tooltip": {"mode": "single", "sort": "none"}},
        "targets": [target(expr, legend=legend)],
    })


def col(name, unit=None, width=None, steps=None, cell=None, mn=None, mx=None, novalue=None, decimals=None):
    props = []
    if unit:
        props.append({"id": "unit", "value": unit})
    if novalue is not None:
        props.append({"id": "noValue", "value": novalue})
    if decimals is not None:
        props.append({"id": "decimals", "value": decimals})
    if mn is not None:
        props.append({"id": "min", "value": mn})
    if mx is not None:
        props.append({"id": "max", "value": mx})
    if steps:
        props.append({"id": "thresholds", "value": thresholds(*steps)})
    if cell:
        props.append({"id": "custom.cellOptions", "value": cell})
    if width:
        props.append({"id": "custom.width", "value": width})
    return {"matcher": {"id": "byName", "options": name}, "properties": props}


HIDE = {k: True for k in ["Time", "__name__", "container", "endpoint", "environment", "instance", "job",
                          "le", "namespace", "pod", "service"]}


def table(title, targets, order, rename, x, w, h, desc="", overrides=(), sort=None, extra_tx=(), page=False,
          hide=(), ops_refs=(), desc_public=""):
    # ops_refs are the targets (and their columns) only the operator view shows.
    if PUBLIC:
        cut = {rename.get("Value #" + r, r) for r in ops_refs}
        targets = [t for t in targets if t[0] not in ops_refs]
        overrides = [o for o in overrides if o["matcher"]["options"] not in cut]
        order = {k: v for k, v in order.items() if k.removeprefix("Value #") not in ops_refs}
        rename = {k: v for k, v in rename.items() if k.removeprefix("Value #") not in ops_refs}
        desc = desc_public or desc
    if PUBLIC:
        # Fixed widths on every column left a full-screen table ending two
        # thirds of the way across; only the label columns keep theirs, the
        # numbers share the rest.
        keep = {"Operator", "RPC Type", "Service"}
        overrides = [{**o, "properties": [pr for pr in o["properties"]
                                          if pr["id"] != "custom.width" or
                                          (o["matcher"]["options"] in keep and pr["value"] <= 200)]}
                     for o in overrides]
    ex = dict(HIDE)
    ex.update({k: True for k in hide})
    tx = [{"id": "merge", "options": {}},
          {"id": "organize", "options": {"excludeByName": ex, "indexByName": order, "renameByName": rename}}]
    tx.extend(extra_tx)
    opts = {"cellHeight": "sm", "enablePagination": page, "showHeader": True}
    if sort:
        opts["sortBy"] = [{"desc": True, "displayName": sort}]
    add({
        "datasource": DS, "description": desc, "type": "table", "title": title, "id": nid(),
        "gridPos": {"h": h, "w": w, "x": x, "y": y[0]},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"},
                                                "filterable": True, "inspect": False},
                                     "mappings": [], "thresholds": thresholds((0, "green"))},
                        "overrides": list(overrides)},
        "options": opts,
        "targets": [target(e, ref=r, table=True) for r, e in targets],
        "transformations": tx,
    })


GAUGE = {"mode": "basic", "type": "gauge", "valueDisplayMode": "color"}
BG = {"mode": "gradient", "type": "color-background"}
PCT_GOOD = ((0, "red"), (80, "yellow"), (95, "green"))

# Shorthands for the per-operator series (added in 9380153). OA is raw, for
# the [1h] windows; the rest are recorded rates, for $__rate_interval panels.
OA = f'sage_operator_attempts_total{{{S}}}'
FAULT = 'attribution=~"supplier|unknown"'
RA = rec("operator_attempts")
# First attempts only: the fair sample of an operator. Retries arrive with less
# budget after another host failed, and an operator reputation has demoted is
# sent mostly those, so a success rate over all attempts condemns it for being
# demoted.
RF = rec("operator_attempts", 'attempt="first"')
RFF = rec("operator_attempts", f'attempt="first", {FAULT}')
RAB = rec("operator_attempt_seconds_bucket")
RL = rec("relay_latency_seconds_bucket", 'request_type="client"')
RCL = rec("client_latency_seconds_bucket")
RWT = rec("websocket_supplier_tenure_seconds_bucket")


def rfa(attribution):
    """First attempts with one attribution, recorded."""
    return rec("operator_attempts", f'attempt="first", attribution="{attribution}"')


# ---------------------------------------------------------------------------
panels.append({
    "gridPos": {"h": 3, "w": 24, "x": 0, "y": 0}, "id": nid(), "type": "text", "title": "",
    "options": {"mode": "markdown", "content":
                ("# SAGE — public pinned view" if PUBLIC else "# SAGE — gateway operator view") +
                "\n**Scope:** `environment=mainnet-sage`, client traffic "
                "(`request_type=client`) unless a panel says otherwise. Operators are registrable domains. "
                "No filters: public dashboards do not support variables."},
})
y[0] = 3

def statusClass(series):
    """Counts by HTTP status class (2xx, 4xx, 5xx): a dozen codes side by side
    left each one unreadable in a stat tile."""
    return f'sum by (class) (label_replace({series}, "class", "${{1}}xx", "status", "([0-9]).*"))'


def relay_success(x):
    stat("Relay Success %",
         f'(1 - sum({RFF}) / sum({RF})) * 100',
         x, 4, unit="percent", mn=0, mx=100, steps=PCT_GOOD,
         desc="First client attempts whose outcome the heuristic did not blame on the supplier: a good answer, or a "
              "chain or client error the supplier answered honestly (block not found, execution reverted). First "
              "attempts only: retries arrive after another host failed, with less time left.")


row("Overview")
stat("Incoming RPS", f'sum(rate(sage_client_requests_total{{{S}}}[{RI}]))', 0, 4, unit="reqps",
     desc="Client requests to SAGE: one per request, whatever retries, hedges or batch items it fanned out into.")
stat("Outgoing Relays/s", f'sum(rate(sage_relay_total{{{S}, request_type="client"}}[{RI}]))', 4, 4,
     unit="reqps", steps=((0, "blue"),),
     desc="Upstream relay attempts for client traffic: every retry, hedge arm and batch item counts. Probes excluded.")
stat("Active WebSockets", f'sum(sage_websocket_connections{{{S}}})', 8, 4, steps=((0, "purple"),),
     desc="Live WebSocket bridges: a client connection plus its supplier connection.")
if PUBLIC:
    relay_success(12)
ops[0] = True
stat("Probe Success %",
     f'sum(rate(sage_relay_total{{{S}, request_type="probe", status=~"2.."}}[{RI}])) / '
     f'sum(rate(sage_relay_total{{{S}, request_type="probe"}}[{RI}])) * 100',
     12, 4, unit="percent", mn=0, mx=100, steps=((0, "red"), (90, "yellow"), (95, "green")),
     desc="Health-check relays answered 2xx. Independent of client traffic; only the probe leader sends them.")
ops[0] = False
stat("Relay Latency P50",
     f'histogram_quantile(0.50, sum by (le) ({RL})) * 1000',
     16, 4, unit="ms", steps=((0, "green"), (500, "yellow"), (1000, "red")),
     desc="Median upstream attempt latency, client traffic. Client-facing latency is in the Latency row.")
stat("Client 5xx %",
     f'sum(rate(sage_client_requests_total{{{S}, status=~"5..", origin!="chain"}}[{RI}])) / sum(rate(sage_client_requests_total{{{S}}}[{RI}])) * 100',
     20, 4, unit="percent", decimals=3, steps=((0, "green"), (0.5, "yellow"), (2, "red")),
     desc="Share of client requests SAGE answered with a 5xx: what clients actually saw, after retries and hedges. A node's own 5xx answer (CometBFT's HTTP 500 for an unknown tx) is the chain answering and is left out.")
y[0] += 4
stat("Requests in 24H", statusClass(f'increase(sage_client_requests_total{{{S}}}[24h])'), 0, 12,
     graph="none", decimals=0, legend="{{class}}", desc="Client requests by the status class SAGE answered, last 24h.")
stat("Requests in range", statusClass(f'increase(sage_client_requests_total{{{S}}}[$__range])'), 12, 12,
     graph="none", decimals=0, legend="{{class}}", desc="Client requests by status class over the dashboard time range.")
y[0] += 4
stat("Relays in 24H", statusClass(f'increase(sage_relay_total{{{S}, request_type="client"}}[24h])'), 0, 12,
     graph="none", decimals=0, legend="{{class}}", color="palette-classic-by-name",
     desc="Upstream client relay attempts by the HTTP status class the relay miner returned, last 24h. The exact "
          "codes are in the Relays by Status Code panel.")
stat("Relays in range", statusClass(f'increase(sage_relay_total{{{S}, request_type="client"}}[$__range])'), 12, 12,
     graph="none", decimals=0, legend="{{class}}", color="palette-classic-by-name",
     desc="Upstream client relay attempts by status class over the dashboard time range.")
y[0] += 4
if not PUBLIC:
    relay_success(0)
ops[0] = True
stat("Hedge Fire Rate %",
     f'sum(rate(sage_hedge_total{{{S}, result=~"primary_won|hedge_won|both_failed"}}[{RI}])) / '
     f'sum(rate(sage_hedge_total{{{S}, result=~"primary_before_delay|primary_won|hedge_won|both_failed"}}[{RI}])) * 100',
     4, 4, unit="percent", mn=0, mx=100, steps=((0, "green"), (15, "yellow"), (30, "red")),
     desc="Share of hedged relays where the primary had not answered by the hedge delay, so a hedge was sent. "
          "High means primaries are often slower than the delay. Reads 100% on images older than 2433a89, "
          "which had no primary_before_delay outcome.")
ops[0] = False
ops[0] = True
stat("Broken Domains",
     f'count(max by (service_id, domain) (sage_circuit_breaker_state{{{S}}}) == 1) or vector(0)', 8, 4,
     steps=((0, "green"), (1, "yellow"), (5, "red")),
     desc="Hosts currently circuit-broken for some service. 0 is healthy.")
ops[0] = False
ops[0] = True
stat("Drained Operators", f'count(max by (service_id, domain, rpc_type) (sage_drained_operators{{{S}}})) or vector(0)',
     12, 4, steps=((0, "green"), (1, "yellow")),
     desc="Operators an operator (or the auto-drain engine) has removed from a service's pool right now.")
ops[0] = False
ops[0] = True
stat("Retry Recovery %",
     f'sum(rate(sage_retry_resolution_total{{{S}, outcome="recovered"}}[{RI}])) / '
     f'sum(rate(sage_retry_resolution_total{{{S}}}[{RI}])) * 100',
     16, 4, unit="percent", mn=0, mx=100, steps=((0, "red"), (50, "yellow"), (80, "green")),
     desc="Retries a later attempt rescued. The rest were exhausted: the client got the last answer or an error.")
ops[0] = False
ops[0] = True
stat("Recovered Panics (1h)", f'sum(increase(sage_recovered_panics_total{{{S}}}[1h])) or vector(0)', 20, 4,
     steps=((0, "green"), (1, "red")),
     desc="Panics contained by the gateway. Non-zero means a bug was caught, not that nothing happened.")
ops[0] = False
if not PUBLIC:
    y[0] += 4

# ---------------------------------------------------------------------------
row("Supplier Quality (Operator / Service / RPC type) — client traffic")
by = "service_id, operator, rpc_type"
table(
    "HTTP Supplier Quality — excludes WebSocket",
    [
        ("RPS", f'sum by ({by}) ({RA})'),
        ("Success", f'(1 - (sum by ({by}) ({RFF}) or sum by ({by}) ({RF}) * 0) '
                    f'/ (sum by ({by}) ({RF}) > 0)) * 100'),
        ("SupplierErr", f'sum by ({by}) ({RFF})'),
        ("P50", f'histogram_quantile(0.50, sum by ({by}, le) ({RAB})) * 1000'),
        ("P95", f'histogram_quantile(0.95, sum by ({by}, le) ({RAB})) * 1000'),
        ("P99", f'histogram_quantile(0.99, sum by ({by}, le) ({RAB})) * 1000'),
        # Reputation rows for WebSocket keys belong in the WebSocket table
        # below: joined here they showed with RPS 0 (HTTP metrics), which
        # read as an operator carrying no WebSocket traffic at all.
        ("Eps", f'max by ({by}) (sage_session_endpoints{{{S}, rpc_type!="websocket"}})'),
        ("MeanScore", f'avg by ({by}) (sage_operator_reputation_mean{{{S}, rpc_type!="websocket"}})'),
        ("Low", f'max by ({by}) (sage_session_endpoints_low{{{S}, rpc_type!="websocket"}})'),
        ("URLs", f'max by ({by}) (sage_operator_reputation_keys{{{S}, rpc_type!="websocket"}})'),
        ("Drained", f'max by (service_id, operator, rpc_type) (label_replace(sage_drained_operators{{{S}, rpc_type!="websocket"}}, "operator", "$1", "domain", "(.*)"))'),
    ],
    {"service_id": 1, "operator": 0, "rpc_type": 2, "Value #RPS": 3, "Value #Success": 4, "Value #SupplierErr": 5,
     "Value #P50": 6, "Value #P95": 7, "Value #P99": 8, "Value #Eps": 9, "Value #MeanScore": 10,
     "Value #Low": 11, "Value #Drained": 12, "Value #URLs": 13},
    {"operator": "Operator", "service_id": "Service", "rpc_type": "RPC Type", "Value #RPS": "RPS",
     "Value #Success": "Success %", "Value #SupplierErr": "Supplier err/s", "Value #P50": "P50 (ms)",
     "Value #P95": "P95 (ms)", "Value #P99": "P99 (ms)", "Value #Eps": "Session eps",
     "Value #MeanScore": "Mean Score", "Value #Low": "Eps < 80", "Value #Drained": "Drained",
     "Value #URLs": "URLs (1h)"},
    0, 24, 18, sort="RPS", ops_refs=("MeanScore", "Low", "URLs", "Drained"),
    desc_public="Per operator (registrable domain), service and RPC type, client attempts only. RPS and latency count "
                "every attempt; Success % and Supplier err/s count first attempts only (the fair sample: retries arrive "
                "with less time left after another host failed). Success % counts a good answer and a chain or client "
                "error the supplier answered honestly; only supplier and unknown attributions count against it. "
                "Latency is per attempt. Session eps is the operator's registrations in the current session.",
    desc="Per operator (registrable domain), service and RPC type, client attempts only. RPS and latency count every "
         "attempt; Success % and Supplier err/s count first attempts only (the fair sample: an operator reputation "
         "has demoted gets mostly retries, which arrive with less time left, and would read worse than it is). An "
         "operator with almost no first attempts has no fair reading here: check its health checks or PATH. "
         "Success % counts a good "
         "answer and a chain or client error the supplier answered honestly; only supplier and unknown "
         "attributions count against it. Latency is per attempt. Session eps is the operator's "
         "registrations in the current session (PATH's Endpoints); Mean Score is the mean reputation of those "
         "that have a score, averaged across pods; Eps < 80 is how many of them sit below tier 1. All three "
         "count registrations, so operators compare whatever their host layout. Drained 1 = removed from the "
         "pool right now. URLs (1h) is how many distinct URLs the operator served from in the last hour — a "
         "layout indicator, not a quality score: many registrations behind one host is one failure away from "
         "losing them all.",
    overrides=[
        col("Success %", unit="percent", mn=0, mx=100, novalue="—", steps=PCT_GOOD, cell=GAUGE, width=120),
        col("RPS", unit="reqps", novalue="0", width=90),
        col("Supplier err/s", unit="reqps", novalue="0", steps=((0, "green"), (0.5, "yellow"), (5, "red")), cell=BG, width=147),
        col("P50 (ms)", unit="ms", novalue="—", steps=((0, "green"), (500, "yellow"), (1000, "red")), width=94),
        col("P95 (ms)", unit="ms", novalue="—", steps=((0, "green"), (1000, "yellow"), (3000, "red")), width=94),
        col("P99 (ms)", unit="ms", novalue="—", steps=((0, "green"), (2000, "yellow"), (5000, "red")), width=94),
        col("Session eps", decimals=0, novalue="—", width=114),
        col("Eps < 80", decimals=0, novalue="0", steps=((0, "green"), (1, "yellow"), (10, "red")), cell=BG, width=90),
        col("URLs (1h)", decimals=0, novalue="—", width=90),
        col("Mean Score", mn=0, mx=100, decimals=1, novalue="—", steps=((0, "red"), (50, "yellow"), (80, "green")),
            cell={"mode": "lcd", "type": "gauge", "valueDisplayMode": "color"}, width=120),
        col("Drained", novalue="—", steps=((0, "green"), (1, "red")), cell=BG, width=80),
        col("Operator", width=145), col("Service", width=110), col("RPC Type", width=100),
    ],
)
y[0] += 18
table(
    "Error Attribution by Operator — what's dragging Success %",
    [
        ("Supplier", f'sum by ({by}) ({rfa("supplier")})'),
        ("Unknown", f'sum by ({by}) ({rfa("unknown")})'),
        ("Chain", f'sum by ({by}) ({rfa("blockchain")})'),
        ("Client", f'sum by ({by}) ({rfa("client")})'),
        ("FaultPct", f'sum by ({by}) ({RFF}) / (sum by ({by}) ({RF}) > 0) * 100'),
    ],
    {"operator": 0, "service_id": 1, "rpc_type": 2, "Value #FaultPct": 3, "Value #Supplier": 4,
     "Value #Unknown": 5, "Value #Chain": 6, "Value #Client": 7},
    {"operator": "Operator", "service_id": "Service", "rpc_type": "RPC Type", "Value #FaultPct": "Supplier fault %",
     "Value #Supplier": "supplier /s", "Value #Unknown": "unknown /s", "Value #Chain": "chain /s",
     "Value #Client": "client /s"},
    0, 24, 12, sort="Supplier fault %",
    desc="First client attempts (the fair sample) by the side the heuristic blamed. supplier: the relay miner or node failed (5xx, 408, "
         "timeouts, bad answers). unknown: failed with no verdict either way. chain: the node answered honestly "
         "about chain state (block not found, pruned history) — never penalised. client: the request's own fault "
         "(invalid params, execution reverted) — never penalised. Only supplier and unknown count as fault.",
    overrides=[
        col("Supplier fault %", unit="percent", mn=0, mx=100, novalue="—",
            steps=((0, "green"), (5, "yellow"), (20, "red")), cell={"mode": "gradient", "type": "gauge"}, width=164),
        col("supplier /s", unit="reqps", novalue="0", steps=((0, "green"), (1, "yellow"), (5, "red")), cell=BG, width=110),
        col("unknown /s", unit="reqps", novalue="0", steps=((0, "green"), (0.1, "yellow"), (1, "red")), cell=BG, width=110),
        col("chain /s", unit="reqps", novalue="0", width=100),
        col("client /s", unit="reqps", novalue="0", width=100),
        col("Operator", width=150), col("Service", width=100), col("RPC Type", width=100),
    ],
)
y[0] += 12

# Status and failure reasons per operator: what an operator can act on,
# without the score or the thresholds behind it.
# HTTP registrations only: WebSocket keys score on a different footing, and
# mixed in they read an operator healthy over HTTP as demoted.
EPS = f'sum by (service_id, operator) (max by (service_id, operator, rpc_type) (sage_session_endpoints{{{S}, rpc_type!="websocket"}}))'
LOW = f'sum by (service_id, operator) (max by (service_id, operator, rpc_type) (sage_session_endpoints_low{{{S}, rpc_type!="websocket"}}))'
DRAINED = (f'max by (service_id, operator) (label_replace(sage_drained_operators{{{S}}}, '
           f'"operator", "$1", "domain", "(.*)"))')
STATUS_MAP = [{"type": "value", "options": {
    "1": {"text": "In rotation", "color": "green", "index": 0},
    "2": {"text": "Partly demoted", "color": "yellow", "index": 1},
    "3": {"text": "Demoted", "color": "orange", "index": 2},
    "4": {"text": "Drained", "color": "red", "index": 3}}}]
table(
    "Operator Status (HTTP)",
    [("Status", f'({DRAINED} > 0) * 0 + 4 or (({LOW} >= {EPS}) and ({EPS} > 0)) * 0 + 3 '
                f'or ({LOW} > 0) * 0 + 2 or ({EPS} > 0) * 0 + 1')],
    {"service_id": 0, "operator": 1, "Value": 2},
    {"service_id": "Service", "operator": "Operator", "Value": "Status"},
    0, 10, 12, page=True,
    desc="Where each operator's registrations in the current session stand with the gateway: in rotation (all "
         "selectable at full weight), partly demoted (some of them sent less traffic after failures), demoted (all of "
         "them), drained (removed from the service's pool for now). The Failure Reasons table beside it says why.",
    overrides=[{"matcher": {"id": "byName", "options": "Status"},
                "properties": [{"id": "mappings", "value": STATUS_MAP},
                               {"id": "custom.cellOptions", "value": {"type": "color-background"}}]},
               col("Service", width=140), col("Operator", width=170)],
)
table(
    "Failure Reasons by Operator (last 1h)",
    [("Failures", f'sum by (service_id, operator, rpc_type, reason) (increase(sage_operator_failures_total{{{S}}}[1h])) > 0')],
    {"service_id": 0, "operator": 1, "rpc_type": 2, "reason": 3, "Value": 4},
    {"service_id": "Service", "operator": "Operator", "rpc_type": "RPC Type", "reason": "Reason", "Value": "Failures (1h)"},
    10, 14, 12, page=True, sort="Failures (1h)",
    desc="What the gateway recorded against each operator in the last hour, client traffic and health checks: "
         "transport_timeout (accepted the connection, no answer in time), http_5xx / upstream_5xx (the backend or "
         "the relay miner failed), stale_response (answered with a chain head behind), html_response (an error "
         "page), ws_probe_dial_failed (the WebSocket upgrade failed), ws_endpoint_lost (a live WebSocket dropped), "
         "and the like. Only failures the gateway blames on the supplier's side are counted.",
    overrides=[col("Failures (1h)", decimals=0, steps=((0, "green"), (10, "yellow"), (100, "red")), cell=BG, width=120),
               col("Service", width=120), col("Operator", width=160), col("RPC Type", width=100)],
)
y[0] += 12
ops[0] = True
table(
    "Currently Broken Hosts (Circuit Breaker)",
    [("Broken", f'max by (service_id, domain) (sage_circuit_breaker_state{{{S}}}) == 1')],
    {"service_id": 0, "domain": 1, "Value": 2},
    {"service_id": "Service", "domain": "Host", "Value": "State"},
    0, 12, 8, page=True,
    desc="Hosts the circuit breaker is skipping right now. Recovers on its own when the break expires.",
)
table(
    "Top Broken Hosts (last 1h)",
    [("A", f'topk(15, sum by (service_id, domain) (increase(sage_circuit_breaks_total{{{S}}}[1h])) > 0)')],
    {"service_id": 0, "domain": 1, "Value": 2},
    {"service_id": "Service", "domain": "Host", "Value": "Breaks (1h)"},
    12, 12, 8, page=True, sort="Breaks (1h)",
    desc="Hosts broken most often in the last hour. One that keeps coming back between recoveries is flapping.",
    overrides=[col("Breaks (1h)", decimals=0, novalue="0", steps=((0, "yellow"), (10, "red")), cell=BG)],
)
ops[0] = False
y[0] += 8

# ---------------------------------------------------------------------------
row("Traffic & Relay Analysis")
ts("Client Requests by Service", [(f'sum by (service_id) (rate(sage_client_requests_total{{{S}}}[{RI}]))', "{{service_id}}")],
   0, 12, unit="reqps")
ts("Client Requests by RPC Type", [(f'sum by (rpc_type) (rate(sage_rpc_type_total{{{S}}}[{RI}]))', "{{rpc_type}}")],
   12, 12, unit="reqps", desc="By the RPC type SAGE settled on for the request (header or detection).")
y[0] += 8
ops[0] = True
ts("Hedge Fire Rate by Service",
   [(f'sum by (service_id) (rate(sage_hedge_total{{{S}, result=~"primary_won|hedge_won|both_failed"}}[{RI}])) / '
     f'sum by (service_id) (rate(sage_hedge_total{{{S}, result=~"primary_before_delay|primary_won|hedge_won|both_failed"}}[{RI}])) * 100', "{{service_id}}")],
   0, 24, unit="percent", stack=False, fill=10, mn=0, steps=((0, "green"), (15, "yellow"), (30, "red")),
   desc="Share of hedged relays where a hedge was sent, by service. One service climbing while the others stay flat points "
        "at that service's suppliers, not the gateway.")
ops[0] = False
y[0] += 8
ops[0] = True  # nearly all client: nothing for the public to read
pie("Relays by Type (client vs probe)", f'sum by (request_type) (rate(sage_relay_total{{{S}}}[{RI}]))',
    "{{request_type}}", 0, 8)
ops[0] = False
pie("Attempt Attribution", f'sum by (attribution) (rate(sage_heuristic_verdicts_total{{{S}}}[{RI}]))',
    "{{attribution}}", 0 if PUBLIC else 8, 12 if PUBLIC else 8, desc="Every client attempt by whose fault its outcome was; none = a good answer.")
pie("Relays by Status Code", f'sum by (status) (rate(sage_relay_total{{{S}, request_type="client"}}[{RI}]))',
    "{{status}}", 12 if PUBLIC else 16, 12 if PUBLIC else 8)
y[0] += 8

# ---------------------------------------------------------------------------
row("Latency Analysis")
lat = []
for q in ("0.50", "0.90", "0.95", "0.99"):
    lat.append((f'histogram_quantile({q}, sum by (le) ({RL})) * 1000',
                f'P{int(float(q) * 100)}'))
ops[0] = True
ts("Relay Attempt Latency Percentiles", lat, 0, 12, unit="ms", stack=False, fill=0, sort_mean=False,
   desc="Per upstream attempt. A retried or hedged request is several attempts, none of them its total.")
ops[0] = False
clat = []
for q in ("0.50", "0.90", "0.95", "0.99"):
    clat.append((f'histogram_quantile({q}, sum by (le) ({RCL})) * 1000',
                 f'P{int(float(q) * 100)}'))
ts("Client-Facing Latency Percentiles", clat, 0 if PUBLIC else 12, 12, unit="ms", stack=False, fill=0, sort_mean=False,
   desc="What the client waited, retries and hedges included: from the request reaching SAGE to the response written.")
if not PUBLIC:  # public: the 504 panel takes the other half of this row
    y[0] += 8
ops[0] = True
ts("Slow Attempts by Operator (> 2.5s)",
   # All attempts are the +Inf bucket: both sides of the subtraction then come
   # from one rule evaluation.
   [('topk(10, sum by (service_id, operator) (' + rec("operator_attempt_seconds_bucket", 'le="+Inf"') + ') - '
     'sum by (service_id, operator) (' + rec("operator_attempt_seconds_bucket", 'le="2.5"') + '))',
     "{{service_id}} {{operator}}")],
   0, 12, unit="reqps", stack=False, fill=10,
   desc="Attempts that took longer than 2.5s, by service and operator, top 10. A slow tail is what runs a request out "
        "of its deadline (a client 504) even when the median is fast: read it beside the 504 panel.")
ops[0] = False
ts("Client 504s by Service",
   [(f'topk(10, sum by (service_id) (rate(sage_client_requests_total{{{S}, status="504"}}[{RI}])))', "{{service_id}}")],
   12, 12, unit="reqps", stack=False, fill=10,
   desc="Requests that ran out of their deadline. The error log names the operators in flight for each one "
        "(\"attempts\" and the hedge deadline message).")
y[0] += 8

# ---------------------------------------------------------------------------
ops[0] = True
row("Health Checks, Selection & Retries")
ts("Probe Results by Status", [(f'sum by (status) (rate(sage_relay_total{{{S}, request_type="probe"}}[{RI}]))', "{{status}}")],
   0, 12, unit="reqps", desc="Health-check relays by the status the relay miner returned. Only the probe leader sends.")
ts("Selection Height Tier", [(f'sum by (tier) (rate(sage_qos_selection_tier_total{{{S}}}[{RI}]))', "tier {{tier}}")],
   12, 12, unit="reqps",
   desc="Endpoint selections by height tier: 1 within sync allowance of the chain head, 2 within twice it, 3 with "
        "the height filter abandoned (least-stale first). A rising tier 3 means a service's pool is behind.")
y[0] += 8
ts("Retry Reasons", [(f'sum by (reason) (rate(sage_retry_total{{{S}}}[{RI}]))', "{{reason}}")], 0, 12, unit="reqps")
ts("Retry Outcomes", [(f'sum by (outcome) (rate(sage_retry_resolution_total{{{S}}}[{RI}]))', "{{outcome}}")],
   12, 12, unit="reqps", desc="recovered: a later attempt succeeded. exhausted: the client got the last answer or an error.")
y[0] += 8
ts("Exhausted Retries by Cause", [(f'sum by (reason) (rate(sage_retry_resolution_total{{{S}, outcome="exhausted"}}[{RI}]))', "{{reason}}")],
   0, 12, unit="reqps", stack=False,
   desc="Retries that ran out, by the verdict that started them. blockchain_error here usually means the chain "
        "state asked for exists on no node in the pool.")
ts("Degraded Responses", [(f'sum by (tier) (rate(sage_degraded_total{{{S}}}[{RI}]))', "{{tier}}")],
   12, 12, unit="reqps",
   desc="Requests served in degraded mode: pool collapse (every endpoint below the floor) or an answer sent with X-Degraded.")
y[0] += 8

ops[0] = False
# ---------------------------------------------------------------------------
row("Client-Facing Errors by Service")
table(
    "Worst Services (client 5xx)",
    [
        ("Req", f'sum by (service_id) (rate(sage_client_requests_total{{{S}}}[{RI}]))'),
        ("Err", f'sum by (service_id) (rate(sage_client_requests_total{{{S}, status=~"5..", origin!="chain"}}[{RI}]))'),
        ("Pct", f'sum by (service_id) (rate(sage_client_requests_total{{{S}, status=~"5..", origin!="chain"}}[{RI}])) / '
                f'sum by (service_id) (rate(sage_client_requests_total{{{S}}}[{RI}])) * 100 > 0'),
    ],
    {"service_id": 0, "Value #Pct": 1, "Value #Err": 2, "Value #Req": 3},
    {"service_id": "Service", "Value #Pct": "5xx %", "Value #Err": "5xx /s", "Value #Req": "Requests /s"},
    0, 24, 8, sort="5xx %",
    desc="What clients saw, per service. A service high here with a clean supplier table usually lacks a supplier "
         "that can serve its requests at all (archival state, an RPC type nobody stakes).",
    overrides=[
        col("5xx %", unit="percent", mn=0, mx=100, novalue="0", steps=((0, "green"), (1, "yellow"), (5, "red")),
            cell={"mode": "gradient", "type": "gauge"}, width=160),
        col("5xx /s", unit="reqps", novalue="0", width=100),
        col("Requests /s", unit="reqps", novalue="0", width=343),
        col("Service", width=557),
    ],
    extra_tx=[{"id": "filterByValue", "options": {"filters": [{"config": {"id": "greater", "options": {"value": 0}},
                                                               "fieldName": "5xx /s"}], "match": "any",
                                                  "type": "include"}}],
)
y[0] += 8

# ---------------------------------------------------------------------------
row("WebSocket Monitoring")
ts("Active WebSocket Connections", [(f'sum by (service_id) (sage_websocket_connections{{{S}}})', "{{service_id}}")],
   0, 8, placement="bottom", sort_mean=False)
ts("WebSocket Closes by Initiator",
   [(f'sum by (initiator) (rate(sage_websocket_closes_total{{{S}}}[{RI}]))', "{{initiator}}")],
   8, 8, unit="cps", placement="bottom", sort_mean=False,
   desc="Bridges ended, by who ended them. gateway closes are SAGE's own decision (a deadline, a rebind that "
        "found nowhere to go); client and endpoint closes are the peers'.")
ts("WebSocket Frame Rate", [(f'sum by (direction) (rate(sage_websocket_frames_total{{{S}}}[{RI}]))', "{{direction}}")],
   16, 8, unit="reqps", placement="bottom", sort_mean=False)
y[0] += 8
ops[0] = True
ts("Rebinds, Stalls & Unresponsive Peers",
   [(f'sum by (result) (rate(sage_websocket_rebinds_total{{{S}}}[{RI}]))', "rebind:{{result}}"),
    (f'sum(rate(sage_websocket_stalls_total{{{S}}}[{RI}]))', "stall"),
    (f'sum by (side) (rate(sage_websocket_unresponsive_total{{{S}}}[{RI}]))', "unresponsive:{{side}}")],
   0, 8, unit="cps", stack=False, placement="bottom", sort_mean=False,
   desc="rebind:ok is healthy: a supplier was replaced under a live client (session rollover, loss, stall) and "
        "subscriptions replayed. rebind:failed means the client was told to reconnect.")
ops[0] = False
tenure = []
for q in ("0.50", "0.90", "0.99"):
    tenure.append((f'histogram_quantile({q}, sum by (le) ({RWT}))',
                   f'p{int(float(q) * 100)}'))
ts("Supplier Tenure per Connection (p50/p90/p99)", tenure, 0 if PUBLIC else 8, 12 if PUBLIC else 8, unit="s", stack=False, placement="bottom",
   sort_mean=False,
   desc="How long one supplier served one client connection, bind to rebind or close. A connection that "
        "rebinds is several tenures, so this is a lower bound on connection lifetime.")
ts("Subscription Notifications by Grade",
   [(f'sum by (grade) (rate(sage_websocket_supplier_notifications_total{{{S}}}[{RI}]))', "{{grade}}")],
   12 if PUBLIC else 16, 12 if PUBLIC else 8, unit="reqps", stack=False, placement="bottom", sort_mean=False,
   desc="ok: for a subscription open on the connection. duplicate: a byte-for-byte repeat of one of the last 8. "
        "unsolicited: for a subscription never opened with that supplier. Both are relays nobody asked for.")
y[0] += 8
wsby = "service_id, operator"
table(
    "WebSocket Supplier Quality by Operator/Service",
    [
        ("Conns", f'sum by ({wsby}) (sage_websocket_supplier_connections{{{S}}})'),
        ("Down", f'sum by ({wsby}) (rate(sage_websocket_supplier_frames_total{{{S}, direction="endpoint_to_client"}}[{RI}])) '
                 f'and on ({wsby}) (sum by ({wsby}) (sage_websocket_supplier_connections{{{S}}}) > 0)'),
        ("Up", f'sum by ({wsby}) (rate(sage_websocket_supplier_frames_total{{{S}, direction="client_to_endpoint"}}[{RI}])) '
               f'and on ({wsby}) (sum by ({wsby}) (sage_websocket_supplier_connections{{{S}}}) > 0)'),
        ("Bad", f'sum by ({wsby}) (rate(sage_websocket_supplier_notifications_total{{{S}, grade=~"duplicate|unsolicited"}}[{RI}])) / '
                f'sum by ({wsby}) (rate(sage_websocket_supplier_notifications_total{{{S}}}[{RI}])) * 100'),
        ("Tenure", f'histogram_quantile(0.50, sum by ({wsby}, le) (rate(sage_websocket_supplier_tenure_seconds_bucket{{{S}}}[1h]))) >= 0'),
        # WebSocket reputation beside WebSocket volume: the same columns the
        # HTTP table carries, for the websocket face only.
        ("MeanScore", f'avg by ({wsby}) (sage_operator_reputation_mean{{{S}, rpc_type="websocket"}})'),
        ("Eps", f'max by ({wsby}) (sage_session_endpoints{{{S}, rpc_type="websocket"}})'),
        ("Low", f'max by ({wsby}) (sage_session_endpoints_low{{{S}, rpc_type="websocket"}})'),
        ("URLs", f'max by ({wsby}) (sage_operator_reputation_keys{{{S}, rpc_type="websocket"}})'),
        ("Drained", f'max by ({wsby}) (label_replace(sage_drained_operators{{{S}, rpc_type=~"websocket|"}}, "operator", "$1", "domain", "(.*)"))'),
    ],
    {"operator": 0, "service_id": 1, "Value #Conns": 2, "Value #Down": 3, "Value #Up": 4, "Value #Bad": 5,
     "Value #Tenure": 6, "Value #MeanScore": 7, "Value #Eps": 8, "Value #Low": 9, "Value #URLs": 10,
     "Value #Drained": 11},
    {"operator": "Operator", "service_id": "Service", "Value #Conns": "WS conns",
     "Value #Down": "Frames/s to client", "Value #Up": "Frames/s to supplier", "Value #Bad": "Dup+unsolicited %",
     "Value #Tenure": "Median tenure (1h)", "Value #MeanScore": "WS Mean Score", "Value #Eps": "WS session eps",
     "Value #Low": "WS eps < 80", "Value #URLs": "WS URLs (1h)", "Value #Drained": "Drained"},
    0, 24, 10, sort="Frames/s to client", ops_refs=("MeanScore", "Low", "URLs", "Drained"),
    desc_public="Per operator and service. Every frame in either direction is a relay the supplier can claim, and "
                "the push rate on a subscription is chosen by the supplier being paid, so Frames /conn is worth "
                "comparing across operators on the same service. Dup+unsolicited % is notifications nobody asked for. "
                "Frame columns show only while the operator holds a live connection. WS session eps is the operator's "
                "registrations in the session.",
    extra_tx=[{"id": "calculateField", "options": {"alias": "Frames /conn", "mode": "binary", "replaceFields": False,
                                                   "binary": {"left": "Frames/s to client", "operator": "/",
                                                              "right": "WS conns"}}}],
    desc="Per operator and service. Every frame in either direction is a relay the supplier can claim, and the "
         "push rate on a subscription is chosen by the supplier being paid, so Frames /conn is worth comparing "
         "across operators on the same service: from inside the gateway a busy feed and an inflated one look "
         "alike. Dup+unsolicited % is notifications nobody asked for. Frame columns show only while the operator "
         "holds a live connection, so a rate window cannot outlive the connection that produced it. The WS "
         "columns are the operator's WebSocket reputation (its websocket keys only, as the HTTP table above "
         "carries the rest): mean score, registrations in the session, how many sit below tier 1, distinct URLs "
         "in the last hour, and whether it is drained.",
    overrides=[
        col("WS conns", decimals=0, novalue="0", steps=((0, "green"), (10, "blue")), cell=BG, width=100),
        col("Frames/s to client", unit="short", decimals=1, novalue="0", width=176),
        col("Frames/s to supplier", unit="short", decimals=2, novalue="0", width=116),
        col("Dup+unsolicited %", unit="percent", decimals=2, novalue="—",
            steps=((0, "green"), (1, "yellow"), (5, "red")), cell=BG, width=118),
        col("Median tenure (1h)", unit="s", novalue="—", width=160),
        col("Frames /conn", unit="short", decimals=1, novalue="0", steps=((0, "green"), (50, "yellow"), (200, "red")),
            cell=BG, width=125),
        col("WS Mean Score", mn=0, mx=100, decimals=1, novalue="—", steps=((0, "red"), (50, "yellow"), (80, "green")),
            cell={"mode": "lcd", "type": "gauge", "valueDisplayMode": "color"}, width=130),
        col("WS session eps", decimals=0, novalue="—", width=109),
        col("WS eps < 80", decimals=0, novalue="0", steps=((0, "green"), (1, "yellow"), (10, "red")), cell=BG, width=100),
        col("WS URLs (1h)", decimals=0, novalue="—", width=100),
        col("Drained", novalue="—", steps=((0, "green"), (1, "red")), cell=BG, width=80),
        col("Operator", width=150), col("Service", width=100),
    ],
)
y[0] += 10
table(
    "Supplier Concentration — top operator share per service (HTTP + WS)",
    [
        ("HTTP", f'( max by (service_id) (sum by (service_id, operator) (rate({OA}[1h]))) / on (service_id) '
                 f'sum by (service_id) (rate({OA}[1h])) ) and on (service_id) (sum by (service_id) (rate({OA}[1h])) > 0)'),
        ("WS", f'( max by (service_id) (sum by (service_id, operator) (avg_over_time(sage_websocket_supplier_connections{{{S}}}[1h]))) '
               f'/ on (service_id) sum by (service_id) (avg_over_time(sage_websocket_supplier_connections{{{S}}}[1h])) ) '
               f'and on (service_id) (sum by (service_id) (avg_over_time(sage_websocket_supplier_connections{{{S}}}[1h])) > 0)'),
    ],
    {"service_id": 0, "Value #HTTP": 1, "Value #WS": 2},
    {"service_id": "Service", "Value #HTTP": "Top HTTP operator share", "Value #WS": "Top WS operator share"},
    0, 24, 8, sort="Top HTTP operator share",
    desc="Blast radius: the largest operator's share of each service over the last hour. High share means one "
         "operator failing takes most of the service with it. Share only; no operator named.",
    overrides=[
        col("Top HTTP operator share", unit="percentunit", mn=0, mx=1, novalue="—",
            steps=((0, "green"), (0.5, "yellow"), (0.7, "orange"), (0.85, "red")), cell={"type": "color-background"}),
        col("Top WS operator share", unit="percentunit", mn=0, mx=1, novalue="—",
            steps=((0, "green"), (0.5, "yellow"), (0.7, "orange"), (0.85, "red")), cell={"type": "color-background"}),
        col("Service", width=140),
    ],
)
y[0] += 8

# ---------------------------------------------------------------------------
ops[0] = True
row("Gateway Internals — probes, auto-drain, health")
ts("WebSocket Recovery Probes", [(f'sum by (result) (rate(sage_websocket_probes_total{{{S}}}[{RI}]))', "{{result}}")],
   0, 8, unit="reqps", placement="bottom", sort_mean=False,
   desc="Probes of demoted WebSocket keys. dial_failed backs off per URL up to 32 min; ok and other_dialect "
        "(alive, speaks another API on that socket) earn a key its way back.")
ts("Auto-Drain Decisions", [(f'sum by (outcome) (rate(sage_auto_drain_total{{{S}}}[{RI}]))', "{{outcome}}")],
   8, 8, unit="cps", placement="bottom", sort_mean=False,
   desc="The auto-drain engine's decisions. shadow = would have drained (engine in shadow mode).")
ts("Probe Leader & Session Layer",
   [(f'sum(sage_health_check_is_leader{{{S}}})', "probe leaders (want 1)"),
    (f'min(sage_session_layer_ready{{{S}}})', "session layer ready (min)")],
   16, 8, stack=False, placement="bottom", sort_mean=False,
   desc="Exactly one pod should lead probing. 0 = nobody probing; >1 = split lease. Session layer 0 = a pod "
        "cannot read sessions from the full node.")
y[0] += 8

ops[0] = False
dash = {
    "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                              "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts",
                              "type": "dashboard"}]},
    "description": ("Supplier quality view for SAGE, externally shareable: outcomes only. "
                    if PUBLIC else "Everything the gateway operator watches: reputation, breaker, drains, retries, "
                    "hedges, probes. ") + "Scoped to environment=mainnet-sage and client traffic. No template "
                   "variables: Grafana public dashboards do not support them.",
    "editable": True, "fiscalYearStartMonth": 0, "graphTooltip": 1, "links": [], "panels": panels,
    "preload": False, "refresh": "5m", "schemaVersion": 42,
    "tags": ["gateway", "metrics", "sage", "pinned", "quality"] if PUBLIC else ["gateway", "metrics", "sage", "operator"],
    "templating": {"list": []}, "time": {"from": "now-1h", "to": "now"},
    "timepicker": {"refresh_intervals": ["30s", "1m", "5m", "15m", "30m", "1h"]},
    "timezone": "browser",
    "title": "SAGE Quality Dashboard (Public — Pinned)" if PUBLIC else "SAGE Gateway Operator",
    # The public dashboard took over the old PATH one at its uid, so its URLs
    # and its public share link keep working.
    "uid": PUBLIC_UID if PUBLIC else "sage-operator",
    "version": 1,
}


def generic(o):
    """The public dashboard names no product: whoever reads it is looking at
    "the gateway". Text only: titles, descriptions and the header; metric
    names and selectors inside queries are left alone."""
    if isinstance(o, dict):
        return {k: (generic_text(v) if k in ("title", "description", "content") and isinstance(v, str) else generic(v))
                for k, v in o.items()}
    if isinstance(o, list):
        return [generic(v) for v in o]
    return o


def generic_text(t):
    for a, b in (("SAGE Quality Dashboard", "Gateway Quality Dashboard"), ("# SAGE —", "# Gateway —"),
                 ("`environment=mainnet-sage`", "mainnet"), ("environment=mainnet-sage", "mainnet"),
                 ("SAGE's", "the gateway's"), ("SAGE", "the gateway")):
        t = t.replace(a, b)
    return t


if PUBLIC:
    dash = generic(dash)
    dash["tags"] = [t for t in dash["tags"] if t != "sage"]
json.dump(dash, sys.stdout, indent=2)
