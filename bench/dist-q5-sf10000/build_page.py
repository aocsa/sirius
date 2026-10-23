#!/usr/bin/env python3
"""Build the Q5 distributed profile page from prof.json (extract.py output).

usage: build_page.py prof.json out.html [LABEL,LABEL,...]
prof.json lists the ranks in the order extract.py was given their directories; LABELs name
them on the page (default "rank 0,rank 1,..."). The charts, tables and pipeline timings come
from prof.json; the prose in template.html describes the 2026-10-23 run and needs editing for
another run. The query's first iteration is drawn as cold and its third as warm.
"""
import json
import sys

R = json.load(open(sys.argv[1]))
LABELS = (
    sys.argv[3].split(",")
    if len(sys.argv) > 3
    else [f"rank {i}" for i in range(len(R))]
)
RANKS = list(enumerate(LABELS))
COLD, WARM = 0, 2


def P(r, qi, pid):
    return next(p for p in R[r]["queries"][qi]["pipelines"] if p["id"] == pid)


def bounds(r, qi):
    q = R[r]["queries"][qi]
    return [
        P(r, qi, "P0")["start"],
        P(r, qi, "P22")["start"],
        P(r, qi, "P26")["start"],
        P(r, qi, "P30")["start"],
        P(r, qi, "P33")["start"],
        P(r, qi, "P38")["start"],
        q["exec_s"],
    ]


SEGS = [
    ("dims", "var(--dim)", "small tables"),
    ("orders", "var(--orders)", "orders scan + shuffle"),
    ("j3", "var(--join)", "orders ⋈ customer"),
    ("li", "var(--scan)", "lineitem scan + shuffle"),
    ("joins", "var(--join)", "joins, partial GROUP BY"),
    ("tail", "var(--tail)", "merge, sort, gather"),
]


def timeline(qi, tmax, ticks, unit_fmt, label_min_px=46, cold=False):
    x0, width = 96, 800
    sc = width / tmax
    rows = []
    for i, (r, name) in enumerate(RANKS):
        y = 34 + i * 44
        b = bounds(r, qi)
        rows.append(
            f'<text x="{x0 - 10}" y="{y + 16}" text-anchor="end" fill="currentColor">{name}</text>'
        )
        for k, (key, color, _) in enumerate(SEGS):
            a, z = b[k], b[k + 1]
            w = max((z - a) * sc, 0.8)
            rows.append(
                f'<rect x="{x0 + a * sc:.1f}" y="{y}" width="{w:.1f}" height="24" fill="{color}"/>'
            )
            if w >= label_min_px and key in ("orders", "li", "joins"):
                txt = f"{z - a:.2f} s" if not cold else f"{z - a:.1f} s"
                rows.append(
                    f'<text x="{x0 + a * sc + 6:.1f}" y="{y + 16}" fill="var(--on-bar)" '
                    f'font-size="11" font-weight="600">{txt}</text>'
                )
    grid = []
    for t in ticks:
        x = x0 + t * sc
        grid.append(
            f'<line x1="{x:.1f}" y1="24" x2="{x:.1f}" y2="{34 + 4 * 44}" stroke="var(--rule)"/>'
        )
        grid.append(
            f'<text x="{x:.1f}" y="{34 + 4 * 44 + 16}" text-anchor="middle" fill="var(--muted)" '
            f'font-size="11">{unit_fmt(t)}</text>'
        )
    legend = []
    lx, ly = x0, 34 + 4 * 44 + 40
    for key, color, label in SEGS:
        if cold and key == "joins":
            label = "wait for last batches, joins"
        if lx + 18 + len(label) * 6.3 > 900:
            lx, ly = x0, ly + 20
        legend.append(
            f'<rect x="{lx}" y="{ly - 10}" width="12" height="12" fill="{color}"/>'
            f'<text x="{lx + 18}" y="{ly}" font-size="11" fill="currentColor">{label}</text>'
        )
        lx += 18 + len(label) * 6.3 + 18
    h = ly + 14
    return (
        f'<svg viewBox="0 0 910 {h}" role="img" aria-label="Per-rank timeline">'
        f'<g font-family="IBM Plex Sans, system-ui, sans-serif" font-size="12">'
        + "".join(grid)
        + "".join(rows)
        + "".join(legend)
        + "</g></svg>"
    )


def phase_table(qi, cold):
    rows = []
    for r, name in RANKS:
        b = bounds(r, qi)
        li, od = P(r, qi, "P30"), P(r, qi, "P22")
        rows.append(
            f"<tr><td>{name}</td>"
            f'<td class="num">{b[1] - b[0]:.2f} s</td>'
            f'<td class="num">{od["end"] - od["start"]:.2f} s</td>'
            f'<td class="num">{li["end"] - li["start"]:.2f} s</td>'
            f'<td class="num">{li["compute_s"]:.1f} s</td>'
            f'<td class="num">{b[5] - b[4]:.2f} s</td>'
            f'<td class="num">{R[r]["queries"][qi]["exec_s"]:.2f} s</td></tr>'
        )
    head = (
        "<tr><th>Rank</th><th>Small tables</th><th>Orders scan</th><th>Lineitem scan</th>"
        "<th>Lineitem task time, summed</th><th>"
        + ("Wait + joins" if cold else "Joins")
        + "</th><th>Executing</th></tr>"
    )
    return '<div class="tablebox"><table>' + head + "".join(rows) + "</table></div>"


def pipe_row(pid, ops, note=""):
    r = RANKS[0][0]
    p = P(r, WARM, pid)
    timing = (
        (
            f'<span class="t">{p["start"]:.2f}–{p["end"]:.2f} s · {p["tasks"]} task'
            f'{"s" if p["tasks"] != 1 else ""}</span>'
        )
        if p["tasks"]
        else '<span class="t">no tasks on rank 0</span>'
    )
    chips = '<span class="arrow">→</span>'.join(ops)
    n = f'<span class="note">{note}</span>' if note else ""
    return f'<div class="pipe"><span class="pid">{pid}</span>{chips}{timing}{n}</div>'


def op(name, kind=""):
    return f'<span class="op {kind}">{name}</span>'


warm = timeline(
    WARM,
    3.1,
    [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
    lambda t: f"{t:g} s" if t == 0 else f"{t:g}",
)
cold = timeline(
    COLD,
    110,
    [0, 20, 40, 60, 80, 100],
    lambda t: f"{t:g} s" if t == 0 else f"{t:g}",
    cold=True,
)

r0 = RANKS[0][0]
wb = bounds(r0, WARM)
wtot = R[r0]["queries"][WARM]["exec_s"]
share = {k: (wb[i + 1] - wb[i]) / wtot * 100 for i, (k, _, _) in enumerate(SEGS)}

stage_dims = "".join(
    [
        pipe_row(
            "P0",
            [op("GPU_SCAN supplier", "src"), op("EXCHANGE_SINK broadcast", "sink")],
        ),
        pipe_row("P1", [op("EXCHANGE_SOURCE ← 4 ranks")]),
        pipe_row("P2", [op("PARTITION")]),
        pipe_row("P3", [op("CONCAT")], "build side of J5"),
        pipe_row(
            "P4", [op("GPU_SCAN region", "src"), op("EXCHANGE_SINK broadcast", "sink")]
        ),
        pipe_row("P5", [op("EXCHANGE_SOURCE")]),
        pipe_row("P6", [op("PARTITION")]),
        pipe_row("P7", [op("CONCAT")]),
        pipe_row("P8", [op("GPU_SCAN nation", "src"), op("DYNAMIC_FILTER")]),
        pipe_row("P9", [op("PARTITION")]),
        pipe_row("P10", [op("CONCAT")]),
        pipe_row("P11", [op("HASH_JOIN J1"), op("EXCHANGE_SINK broadcast", "sink")]),
        pipe_row("P12", [op("EXCHANGE_SOURCE")]),
        pipe_row("P13", [op("PARTITION")]),
        pipe_row("P14", [op("CONCAT")]),
        pipe_row("P15", [op("GPU_SCAN customer", "src"), op("DYNAMIC_FILTER")]),
        pipe_row("P16", [op("PARTITION")]),
        pipe_row("P17", [op("CONCAT")]),
        pipe_row("P18", [op("HASH_JOIN J2"), op("EXCHANGE_SINK hash custkey", "sink")]),
    ]
)
stage_orders = "".join(
    [
        pipe_row("P19", [op("EXCHANGE_SOURCE ← 4 ranks")]),
        pipe_row("P20", [op("PARTITION")]),
        pipe_row("P21", [op("CONCAT")], "build side of J3"),
        pipe_row(
            "P22",
            [
                op("GPU_SCAN orders", "src hot"),
                op("DYNAMIC_FILTER"),
                op("EXCHANGE_SINK hash custkey", "sink"),
            ],
        ),
        pipe_row("P23", [op("EXCHANGE_SOURCE ← 4 ranks")]),
        pipe_row("P24", [op("PARTITION")]),
        pipe_row("P25", [op("CONCAT")]),
        pipe_row(
            "P26", [op("HASH_JOIN J3"), op("EXCHANGE_SINK hash orderkey", "sink")]
        ),
    ]
)
stage_li = "".join(
    [
        pipe_row("P27", [op("EXCHANGE_SOURCE ← 4 ranks")]),
        pipe_row("P28", [op("PARTITION")]),
        pipe_row("P29", [op("CONCAT")], "build side of J4"),
        pipe_row(
            "P30",
            [
                op("GPU_SCAN lineitem", "src hot"),
                op("DYNAMIC_FILTER", "hot"),
                op("EXCHANGE_SINK hash orderkey", "sink hot"),
            ],
            "two thirds of the query",
        ),
        pipe_row("P31", [op("EXCHANGE_SOURCE ← 4 ranks")]),
        pipe_row("P32", [op("PARTITION")]),
        pipe_row("P33", [op("CONCAT")]),
    ]
)
stage_tail = "".join(
    [
        pipe_row("P34", [op("HASH_JOIN J4")]),
        pipe_row("P35", [op("PARTITION")]),
        pipe_row("P36", [op("CONCAT")]),
        pipe_row("P37", [op("HASH_JOIN J5"), op("PROJECTION"), op("HASH_GROUP_BY")]),
        pipe_row("P38", [op("EXCHANGE_SINK hash n_name", "sink")]),
        pipe_row("P39", [op("EXCHANGE_SOURCE ← 4 ranks")]),
        pipe_row("P40", [op("PARTITION")]),
        pipe_row("P41", [op("MERGE_GROUP_BY"), op("ORDER_BY")]),
        pipe_row("P42", [op("SORT_SAMPLE"), op("EXCHANGE_SINK range", "sink")]),
        pipe_row("P43", [op("EXCHANGE_SOURCE"), op("SORT_PARTITION")]),
        pipe_row("P44", [op("MERGE_SORT")]),
        pipe_row("P45", [op("EXCHANGE_SINK gather", "sink")]),
        pipe_row(
            "P46",
            [op("EXCHANGE_SOURCE"), op("RESULT_COLLECTOR", "sink")],
            "rank 0 only",
        ),
    ]
)

page = open(__file__.replace("build_page.py", "template.html")).read()
for k, v in {
    "{{WARM_SVG}}": warm,
    "{{COLD_SVG}}": cold,
    "{{WARM_TABLE}}": phase_table(WARM, False),
    "{{COLD_TABLE}}": phase_table(COLD, True),
    "{{STAGE_DIMS}}": stage_dims,
    "{{STAGE_ORDERS}}": stage_orders,
    "{{STAGE_LI}}": stage_li,
    "{{STAGE_TAIL}}": stage_tail,
    "{{LI_SHARE}}": f"{share['li']:.0f}",
    "{{ORD_SHARE}}": f"{(wb[3] - wb[1]) / wtot * 100:.0f}",
    "{{WARM_EXEC}}": f"{wtot:.2f}",
}.items():
    page = page.replace(k, v)
assert "{{" not in page, page[page.index("{{") : page.index("{{") + 40]
open(sys.argv[2], "w").write(page)
