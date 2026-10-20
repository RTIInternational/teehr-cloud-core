"""Small helpers for generating editable draw.io (.drawio) files from Python.

Each Page becomes one tab in the .drawio file. Shapes are placed with absolute
coordinates (x, y, width, height) and edges connect shapes by id, so the
output stays fully editable in draw.io.
"""
from xml.sax.saxutils import quoteattr

# Shape styles (colors follow the draw.io default palette)
BASE = "rounded=1;whiteSpace=wrap;html=1;arcSize=8;fontSize=12;"
CORE = BASE + "fillColor=#dae8fc;strokeColor=#6c8ebf;"
CLIENT = BASE + "fillColor=#d5e8d4;strokeColor=#82b366;"
PY = BASE + "fillColor=#ffe6cc;strokeColor=#d79b00;"
INFRA = BASE + "fillColor=#f5f5f5;strokeColor=#666666;fontColor=#333333;"
EXT = BASE + "fillColor=#ffffff;strokeColor=#363636;"
STORE = "shape=cylinder3;whiteSpace=wrap;html=1;boundedLbl=1;backgroundOutline=1;size=10;fontSize=12;fillColor=#dae8fc;strokeColor=#6c8ebf;"
GROUP = "rounded=0;whiteSpace=wrap;html=1;fillColor=none;dashed=1;strokeWidth=2;dashPattern=8 8;strokeColor=#363636;"
GROUP_LABEL = "text;html=1;align=center;verticalAlign=middle;whiteSpace=wrap;rounded=0;fillColor=default;fontSize=14;fontStyle=1;fontColor=#363636;"
SUB = "rounded=0;whiteSpace=wrap;html=1;fillColor=none;dashed=1;strokeWidth=1;dashPattern=4 4;strokeColor=#888888;"
SUB_LABEL = "text;html=1;align=left;verticalAlign=middle;whiteSpace=wrap;rounded=0;fillColor=default;fontSize=12;fontStyle=3;fontColor=#555555;spacingLeft=4;"
NOTE = "text;html=1;align=center;verticalAlign=middle;whiteSpace=wrap;fontSize=11;fontStyle=2;fontColor=#555555;"
TEXT = "text;html=1;align=left;verticalAlign=middle;whiteSpace=wrap;fontSize=11;"
EDGE = "edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;endArrow=block;endFill=1;strokeColor=#363636;fontSize=11;labelBackgroundColor=default;"
DASHED = "dashed=1;"
# Arrow that crosses others: draw it last so the crossing gets a jump arc
DIRECT = "jumpStyle=arc;jumpSize=10;"

LEGEND_ITEMS = [
    (CORE, "Shared core (teehr-cloud-core)"),
    (CLIENT, "Client deployment (CIROH, USBR)"),
    (PY, "TEEHR-Python"),
    (INFRA, "Infrastructure"),
    (EXT, "External"),
]


def title_body(title, body=""):
    """Bold title with a smaller body line underneath (HTML label)."""
    if not body:
        return f"<b>{title}</b>"
    return f'<b>{title}</b><br><font style="font-size:11px">{body}</font>'


class Page:
    """One draw.io page (tab)."""

    def __init__(self, name, width=1600, height=1000):
        self.name = name
        self.width = width
        self.height = height
        self.cells = []

    def box(self, id, value, style, x, y, w, h):
        self.cells.append(
            f'<mxCell id="{id}" value={quoteattr(value)} style="{style}" vertex="1" parent="1">'
            f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>'
        )

    def edge(self, id, source, target, label="", style="", points=()):
        """Arrow between two shapes; `points` are optional (x, y) waypoints."""
        waypoints = ""
        if points:
            waypoints = '<Array as="points">' + "".join(f'<mxPoint x="{x}" y="{y}"/>' for x, y in points) + "</Array>"
        self.cells.append(
            f'<mxCell id="{id}" value={quoteattr(label)} style="{EDGE}{style}" edge="1" parent="1" '
            f'source="{source}" target="{target}"><mxGeometry relative="1" as="geometry">{waypoints}</mxGeometry></mxCell>'
        )

    def line(self, id, x1, y1, x2, y2, style=""):
        """Free-standing arrow between two points (e.g., a legend sample)."""
        self.cells.append(
            f'<mxCell id="{id}" value="" style="{EDGE}{style}" edge="1" parent="1"><mxGeometry relative="1" as="geometry">'
            f'<mxPoint x="{x1}" y="{y1}" as="sourcePoint"/><mxPoint x="{x2}" y="{y2}" as="targetPoint"/></mxGeometry></mxCell>'
        )

    def group(self, id, label, x, y, w, h, sub=False):
        """Dashed outline with its label sitting on the top border."""
        self.box(id, "", SUB if sub else GROUP, x, y, w, h)
        if sub:
            self.box(f"{id}-label", label, SUB_LABEL, x + 10, y - 10, 12 + 7 * len(label), 20)
        else:
            self.box(f"{id}-label", label, GROUP_LABEL, x + 20, y - 14, 20 + 8 * len(label), 30)

    def legend(self, x, y, items=LEGEND_ITEMS, width=200, lines=()):
        """Legend of box swatches, plus optional arrow samples as (edge style, text) pairs."""
        self.box("legend", "", "rounded=0;whiteSpace=wrap;html=1;fillColor=none;strokeColor=#bbbbbb;",
                 x, y, width, 40 + 28 * (len(items) + len(lines)))
        self.box("legend-title", "<b>Legend</b>", "text;html=1;align=left;verticalAlign=middle;fontSize=12;",
                 x + 10, y + 5, 100, 20)
        for i, (style, text) in enumerate(items):
            sy = y + 32 + i * 28
            self.box(f"legend-sw{i}", "", style, x + 12, sy, 28, 18)
            self.box(f"legend-tx{i}", text, TEXT, x + 48, sy - 3, width - 52, 24)
        for i, (style, text) in enumerate(lines):
            sy = y + 32 + (len(items) + i) * 28
            self.line(f"legend-ln{i}", x + 10, sy + 9, x + 42, sy + 9, style)
            self.box(f"legend-lt{i}", text, TEXT, x + 48, sy - 3, width - 52, 24)

    def xml(self, index):
        return (
            f'<diagram id="page-{index}" name={quoteattr(self.name)}>'
            f'<mxGraphModel dx="{self.width}" dy="{self.height}" grid="1" gridSize="10" guides="1" tooltips="1" '
            f'connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="{self.width}" '
            f'pageHeight="{self.height}" math="0" shadow="0">'
            '<root><mxCell id="0"/><mxCell id="1" parent="0"/>'
            + "".join(self.cells)
            + "</root></mxGraphModel></diagram>"
        )


def write_drawio(path, pages):
    """Write pages to a single multi-page .drawio file."""
    body = "".join(p.xml(i) for i, p in enumerate(pages))
    with open(path, "w", encoding="utf-8") as f:
        f.write(f'<mxfile host="Electron" type="device">{body}</mxfile>')
