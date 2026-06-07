import os
import re
import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Preformatted,
    Table,
    TableStyle,
    Image as RLImage,
    KeepTogether,
)


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "TEMPORALSTORE_ONE_CLUSTER_TEMPORAL_FEATURES.md"
OUT_DIR = ROOT / "outputs"
DIAGRAM_DIR = OUT_DIR / "temporalstore_diagrams"
PDF = OUT_DIR / "TEMPORALSTORE_ONE_CLUSTER_TEMPORAL_FEATURES.pdf"


def load_font(size=22, bold=False):
    candidates = [
        r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\segoeuib.ttf" if bold else r"C:\Windows\Fonts\segoeui.ttf",
    ]
    for c in candidates:
        if os.path.exists(c):
            return ImageFont.truetype(c, size)
    return ImageFont.load_default()


FONT = load_font(21)
FONT_B = load_font(22, True)
FONT_S = load_font(18)


def wrap(draw, text, font, width):
    words = text.replace("<br/>", "\n").split()
    lines = []
    line = ""
    for word in words:
        test = (line + " " + word).strip()
        if draw.textbbox((0, 0), test, font=font)[2] <= width:
            line = test
        else:
            if line:
                lines.append(line)
            line = word
    if line:
        lines.append(line)
    return lines


def box(draw, xy, text, fill="#eef6ff", outline="#2f80ed", font=None):
    font = font or FONT
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle(xy, radius=16, fill=fill, outline=outline, width=3)
    lines = []
    for part in text.split("\n"):
        lines.extend(wrap(draw, part, font, x2 - x1 - 24))
    line_h = font.size + 5
    total_h = line_h * len(lines)
    y = y1 + (y2 - y1 - total_h) / 2
    for line in lines:
        tw = draw.textbbox((0, 0), line, font=font)[2]
        draw.text((x1 + (x2 - x1 - tw) / 2, y), line, fill="#17202a", font=font)
        y += line_h


def arrow(draw, start, end, color="#475569", width=3):
    draw.line([start, end], fill=color, width=width)
    x1, y1 = start
    x2, y2 = end
    dx, dy = x2 - x1, y2 - y1
    length = max((dx * dx + dy * dy) ** 0.5, 1)
    ux, uy = dx / length, dy / length
    px, py = -uy, ux
    size = 13
    p1 = (x2, y2)
    p2 = (x2 - ux * size + px * size * 0.55, y2 - uy * size + py * size * 0.55)
    p3 = (x2 - ux * size - px * size * 0.55, y2 - uy * size - py * size * 0.55)
    draw.polygon([p1, p2, p3], fill=color)


def label(draw, xy, text):
    draw.text(xy, text, font=FONT_S, fill="#334155")


def canvas(w=1600, h=950):
    img = Image.new("RGB", (w, h), "white")
    draw = ImageDraw.Draw(img)
    return img, draw


def save(img, name):
    DIAGRAM_DIR.mkdir(parents=True, exist_ok=True)
    path = DIAGRAM_DIR / name
    img.save(path)
    return path


def diagram_overall():
    img, d = canvas(1700, 1050)
    box(d, (650, 40, 1050, 130), "Applications\nRisk / Fraud / Ads / Feature Serving", "#fff7ed", "#f97316", FONT_B)
    box(d, (330, 190, 650, 290), "Direct SDK\nC++ / Go / Java / Python / Rust", "#f0fdf4", "#16a34a")
    box(d, (1050, 190, 1370, 290), "Proxy\nRedis path / Routing", "#f0fdf4", "#16a34a")
    box(d, (680, 330, 1020, 430), "Metaserver\nMetadata / Routing / Placement", "#eef6ff", "#2563eb", FONT_B)
    for sx in [500, 850, 1200]:
        arrow(d, (850, 130), (sx, 190))
    arrow(d, (490, 290), (720, 330))
    arrow(d, (1210, 290), (980, 330))
    xs = [230, 690, 1150]
    names = ["Data Node 1\nPrimary Partitions", "Data Node 2\nSecondary Partitions", "Data Node 3\nSecondary Partitions"]
    for x, name in zip(xs, names):
        box(d, (x, 520, x + 330, 620), name, "#eef6ff", "#2563eb", FONT_B)
        box(d, (x, 700, x + 330, 790), "Memory\nObject State + Index", "#fefce8", "#ca8a04")
        box(d, (x, 825, x + 330, 915), "BlockCache / MtCache\nDRAM + SSD", "#f5f3ff", "#7c3aed")
        arrow(d, (850, 430), (x + 165, 520))
        arrow(d, (x + 165, 620), (x + 165, 700))
        arrow(d, (x + 165, 790), (x + 165, 825))
    box(d, (620, 950, 1080, 1030), "Shared Durable Store\nEFS / S3-compatible / Future Object Store", "#ecfeff", "#0891b2", FONT_B)
    for x in xs:
        arrow(d, (x + 165, 915), (850, 950))
    return save(img, "01_overall_architecture.png")


def diagram_write():
    img, d = canvas(1700, 900)
    steps = [
        ("Application", "write event"),
        ("SDK / Proxy", "route"),
        ("Metaserver", "partition"),
        ("Primary Node", "mutate"),
        ("Memory Object\n+ Index", "update"),
        ("Oplog", "append"),
        ("Shared Store", "dump"),
        ("Secondary", "replay"),
    ]
    y = 120
    x0 = 50
    w = 180
    gap = 25
    centers = []
    for i, (name, _) in enumerate(steps):
        x = x0 + i * (w + gap)
        box(d, (x, y, x + w, y + 90), name, "#eef6ff", "#2563eb", FONT_B if i in [0, 3, 7] else FONT)
        centers.append((x + w / 2, y + 90))
        draw_x = x + w / 2
        d.line([(draw_x, y + 90), (draw_x, 760)], fill="#cbd5e1", width=2)
    arrow_y = 300
    for i in range(len(steps) - 1):
        x1 = x0 + i * (w + gap) + w
        x2 = x0 + (i + 1) * (w + gap)
        arrow(d, (x1, arrow_y), (x2, arrow_y))
        label(d, (x1 + 10, arrow_y - 35), steps[i][1])
        arrow_y += 55 if i % 2 == 0 else -55
    box(d, (520, 650, 1180, 760), "Replica becomes queryable after loading dumped state and replaying oplog\nReplica reads should be gated by lag and consistency policy.", "#fff7ed", "#f97316", FONT_B)
    return save(img, "02_write_replication.png")


def diagram_read():
    img, d = canvas(1500, 1150)
    box(d, (520, 40, 980, 120), "Read Query\ncount / sum / distinct / sequence / KV", "#fff7ed", "#f97316", FONT_B)
    box(d, (560, 180, 940, 260), "SDK or Proxy Route Lookup", "#f0fdf4", "#16a34a")
    box(d, (570, 320, 930, 400), "Data Node", "#eef6ff", "#2563eb", FONT_B)
    box(d, (520, 460, 980, 540), "In-Memory Index\nkey to object/page metadata", "#fefce8", "#ca8a04", FONT_B)
    box(d, (520, 610, 980, 700), "Memory Hit?\nHot object or page", "#ffffff", "#64748b", FONT_B)
    box(d, (170, 780, 520, 870), "Read from Memory", "#fefce8", "#ca8a04")
    box(d, (610, 780, 960, 870), "BlockCache Hit?\nDRAM or SSD", "#f5f3ff", "#7c3aed", FONT_B)
    box(d, (1040, 780, 1390, 870), "Read from Shared Store\nEFS / S3-compatible", "#ecfeff", "#0891b2")
    box(d, (610, 940, 960, 1020), "Decode Page / Object", "#eef6ff", "#2563eb")
    box(d, (1010, 940, 1360, 1020), "Fill BlockCache", "#f5f3ff", "#7c3aed")
    box(d, (430, 1050, 1070, 1130), "Model-Specific Compute\nwindow / filter / count / distinct / sequence", "#fff7ed", "#f97316", FONT_B)
    for a, b in [((750,120),(750,180)), ((750,260),(750,320)), ((750,400),(750,460)), ((750,540),(750,610))]:
        arrow(d, a, b)
    arrow(d, (520, 655), (520, 825)); label(d, (455, 700), "Yes")
    arrow(d, (980, 655), (610, 825)); label(d, (800, 700), "No")
    arrow(d, (960, 825), (1040, 825)); label(d, (975, 790), "Miss")
    arrow(d, (785, 870), (785, 940)); label(d, (720, 890), "Hit")
    arrow(d, (1215, 870), (1185, 940))
    arrow(d, (1010, 980), (960, 980))
    arrow(d, (345, 870), (600, 1050))
    arrow(d, (785, 1020), (750, 1050))
    return save(img, "03_read_blockcache.png")


def diagram_before_after():
    img, d = canvas(1700, 900)
    box(d, (80, 40, 760, 120), "Traditional Per-Feature Stack", "#fee2e2", "#ef4444", FONT_B)
    left = [
        "Raw Events",
        "Kafka / Queue",
        "Flink Job\nper feature family",
        "Redis / KV\ncounters, buckets, blobs",
        "Custom Serving Logic\nTTL, windows, filters, merge",
        "Risk / Ads / Model",
    ]
    y = 170
    for i, text in enumerate(left):
        box(d, (210, y, 630, y + 75), text, "#fff7ed" if i == 0 else "#ffffff", "#ef4444")
        if i:
            arrow(d, (420, y - 30), (420, y))
        y += 105
    box(d, (30, 445, 180, 535), "Offline Batch\nrepair/backfill", "#fefce8", "#ca8a04")
    arrow(d, (180, 490), (210, 490))

    box(d, (940, 40, 1620, 120), "TemporalStore Path", "#dcfce7", "#16a34a", FONT_B)
    right = [
        "Raw or Bucketed Events",
        "TemporalStore\nTemporal Models",
        "Direct Online Query\ncount, sum, distinct, sequence, top-K",
        "Risk / Ads / Model",
    ]
    y = 210
    for i, text in enumerate(right):
        box(d, (1050, y, 1510, y + 90), text, "#f0fdf4" if i == 1 else "#ffffff", "#16a34a", FONT_B if i == 1 else FONT)
        if i:
            arrow(d, (1280, y - 45), (1280, y))
        y += 140
    return save(img, "04_one_store_vs_many.png")


def diagram_models():
    img, d = canvas(1600, 900)
    box(d, (560, 40, 1040, 120), "Events\nlogin / payment / impression / click / signup", "#fff7ed", "#f97316", FONT_B)
    box(d, (620, 210, 980, 300), "TemporalStore", "#eef6ff", "#2563eb", FONT_B)
    arrow(d, (800, 120), (800, 210))
    models = [
        ((80, 430, 360, 530), "TemporalCounter\nrolling count or sum", "Risk / Fraud"),
        ((390, 430, 670, 530), "TemporalAggregate\nfiltered window aggregation", "Risk / Fraud"),
        ((700, 430, 980, 530), "TemporalDistinct\nunique count", "Risk / Fraud"),
        ((1010, 430, 1290, 530), "SequenceModel\nrecent behavior history", "Recommendation / Ranking"),
        ((1320, 430, 1580, 530), "Hash / KV\nlatest profile", "Online Feature Serving"),
    ]
    for xy, model, out in models:
        box(d, xy, model, "#f5f3ff", "#7c3aed")
        cx = (xy[0] + xy[2]) / 2
        arrow(d, (800, 300), (cx, xy[1]))
        box(d, (xy[0], 680, xy[2], 770), out, "#f0fdf4", "#16a34a")
        arrow(d, (cx, xy[3]), (cx, 680))
    return save(img, "05_model_map.png")


DIAGRAM_BUILDERS = [
    diagram_overall,
    diagram_write,
    diagram_read,
    diagram_before_after,
    diagram_models,
]


def escape(s):
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def render_pdf():
    OUT_DIR.mkdir(exist_ok=True)
    diagram_paths = DIAGRAM_BUILDERS.copy()
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="Body2", parent=styles["BodyText"], fontName="Helvetica", fontSize=9.5, leading=13))
    styles.add(ParagraphStyle(name="H1x", parent=styles["Heading1"], fontSize=22, leading=27, spaceAfter=12))
    styles.add(ParagraphStyle(name="H2x", parent=styles["Heading2"], fontSize=15, leading=19, spaceBefore=14, spaceAfter=8))
    styles.add(ParagraphStyle(name="H3x", parent=styles["Heading3"], fontSize=12, leading=15, spaceBefore=10, spaceAfter=6))
    styles.add(ParagraphStyle(name="Bullet2", parent=styles["Body2"], leftIndent=14, firstLineIndent=-8))

    story = []
    lines = DOC.read_text(encoding="utf-8").splitlines()
    i = 0
    diagram_index = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            story.append(Spacer(1, 5))
            i += 1
            continue
        if line.startswith("```"):
            lang = line[3:].strip()
            block = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                block.append(lines[i])
                i += 1
            i += 1
            if lang == "mermaid":
                if diagram_index < len(diagram_paths):
                    img_path = diagram_paths[diagram_index]()
                    diagram_index += 1
                    story.append(KeepTogether([RLImage(str(img_path), width=7.1 * inch, height=4.3 * inch), Spacer(1, 8)]))
                else:
                    story.append(Preformatted("\n".join(block), styles["Code"]))
            else:
                story.append(Preformatted("\n".join(block), styles["Code"]))
            continue
        if line.startswith("# "):
            story.append(Paragraph(escape(line[2:]), styles["H1x"]))
        elif line.startswith("## "):
            story.append(Paragraph(escape(line[3:]), styles["H2x"]))
        elif line.startswith("### "):
            story.append(Paragraph(escape(line[4:]), styles["H3x"]))
        elif line.startswith("- "):
            story.append(Paragraph("• " + escape(line[2:]), styles["Bullet2"]))
        elif line.startswith("|"):
            table_lines = []
            while i < len(lines) and lines[i].startswith("|"):
                table_lines.append(lines[i])
                i += 1
            rows = []
            for t in table_lines:
                cells = [c.strip() for c in t.strip("|").split("|")]
                if all(set(c) <= set("-: ") for c in cells):
                    continue
                rows.append([Paragraph(escape(c), styles["Body2"]) for c in cells])
            if rows:
                tbl = Table(rows, repeatRows=1, colWidths=[2.0 * inch, 2.3 * inch, 2.1 * inch])
                tbl.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eef2f7")),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ]))
                story.append(tbl)
            continue
        else:
            story.append(Paragraph(escape(line), styles["Body2"]))
        i += 1

    doc = SimpleDocTemplate(
        str(PDF),
        pagesize=LETTER,
        rightMargin=0.55 * inch,
        leftMargin=0.55 * inch,
        topMargin=0.55 * inch,
        bottomMargin=0.55 * inch,
    )
    doc.build(story)
    print(PDF)


if __name__ == "__main__":
    render_pdf()
