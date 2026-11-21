"""A small .xlsx writer: one workbook, several sheets, numbers kept as numbers, the header row bold and frozen, the
columns sized to their text. The review is sent as one file that Excel, WPS, Numbers and Google Sheets open as they
are, without a spreadsheet library on the server (the standard library's zipfile and the Office Open XML format)."""
from __future__ import annotations

import io
import math
import re
import zipfile
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Sequence
from xml.sax.saxutils import escape

MAX_ROWS = 1_048_576
MAX_TEXT = 32_767
ILLEGAL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]")   # not allowed in XML 1.0
NAME_BAD = re.compile(r"[\[\]:*?/\\]")
MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


@dataclass
class Sheet:
    name: str
    rows: list[list[Any]]
    header: bool = True                         # the first row is a header: bold, frozen while scrolling
    bold: set[int] = field(default_factory=set)  # other rows in bold (0-based): section titles


def column(n: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA."""
    out = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def _number(value: Any) -> str | None:
    """The value as a cell number, or None when it is not one (text, a bool, NaN)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return str(int(value)) if value.is_integer() and abs(value) < 1e15 else repr(value)
    if isinstance(value, Decimal):
        return format(value, "f") if value.is_finite() else None
    return None


def _cell(ref: str, value: Any, style: int) -> str:
    s = f' s="{style}"' if style else ""
    if value is None or value == "":
        return ""
    number = _number(value)
    if number is not None:
        return f'<c r="{ref}"{s}><v>{number}</v></c>'
    if isinstance(value, bool):
        value = "是" if value else "否"
    text = ILLEGAL.sub("", str(value))[:MAX_TEXT]
    return f'<c r="{ref}"{s} t="inlineStr"><is><t xml:space="preserve">{escape(text)}</t></is></c>'


def _width(value: Any) -> int:
    """A column's width for the text: wide (CJK) characters count twice."""
    text = str(value) if value is not None else ""
    return sum(2 if ord(ch) > 0x2E80 else 1 for ch in text)


def _sheet_xml(sheet: Sheet) -> str:
    rows = sheet.rows[:MAX_ROWS]
    widths: dict[int, int] = {}
    body = []
    for r, row in enumerate(rows):
        style = 1 if (r == 0 and sheet.header) or r in sheet.bold else 0
        cells = []
        for c, value in enumerate(row):
            cells.append(_cell(f"{column(c)}{r + 1}", value, style))
            if r < 2000:  # widths from the first rows are enough
                widths[c] = max(widths.get(c, 0), _width(value))
        body.append(f'<row r="{r + 1}">{"".join(cells)}</row>')
    cols = "".join(f'<col min="{c + 1}" max="{c + 1}" width="{min(60, max(8, w + 2))}" customWidth="1"/>'
                   for c, w in sorted(widths.items()))
    view = ('<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" '
            'state="frozen"/></sheetView></sheetViews>' if sheet.header and len(rows) > 1 else "")
    return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<worksheet xmlns="{MAIN}">{view}'
            + (f"<cols>{cols}</cols>" if cols else "") + f'<sheetData>{"".join(body)}</sheetData></worksheet>')


def sheet_names(names: Sequence[str]) -> list[str]:
    """Names Excel accepts: at most 31 characters, none of []:*?/\\, each once."""
    out: list[str] = []
    for name in names:
        base = NAME_BAD.sub("_", name).strip("'")[:31] or "Sheet"
        candidate, n = base, 2
        while candidate.lower() in {x.lower() for x in out}:
            suffix = f"({n})"
            candidate, n = base[:31 - len(suffix)] + suffix, n + 1
        out.append(candidate)
    return out


STYLES = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<styleSheet xmlns="{MAIN}">'
          '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
          '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>'
          '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
          '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
          '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
          '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>'
          '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>')


def workbook(sheets: Sequence[Sheet]) -> bytes:
    """The .xlsx file of the sheets, in their order."""
    if not sheets:
        raise ValueError("a workbook needs a sheet")
    names = sheet_names([s.name for s in sheets])
    n = len(sheets)
    head = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    types = (head + '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
             '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
             '<Default Extension="xml" ContentType="application/xml"/>'
             '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
             '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
             + "".join(f'<Override PartName="/xl/worksheets/sheet{i + 1}.xml" '
                       'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' for i in range(n))
             + "</Types>")
    root_rels = (head + '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                 f'<Relationship Id="rId1" Type="{REL}/officeDocument" Target="xl/workbook.xml"/></Relationships>')
    book = (head + f'<workbook xmlns="{MAIN}" xmlns:r="{REL}"><sheets>'
            + "".join(f'<sheet name="{escape(name, {chr(34): "&quot;"})}" sheetId="{i + 1}" r:id="rId{i + 1}"/>'
                      for i, name in enumerate(names))
            + "</sheets></workbook>")
    book_rels = (head + '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                 + "".join(f'<Relationship Id="rId{i + 1}" Type="{REL}/worksheet" Target="worksheets/sheet{i + 1}.xml"/>'
                           for i in range(n))
                 + f'<Relationship Id="rId{n + 1}" Type="{REL}/styles" Target="styles.xml"/></Relationships>')
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", types)
        z.writestr("_rels/.rels", root_rels)
        z.writestr("xl/workbook.xml", book)
        z.writestr("xl/_rels/workbook.xml.rels", book_rels)
        z.writestr("xl/styles.xml", STYLES)
        for i, sheet in enumerate(sheets):
            z.writestr(f"xl/worksheets/sheet{i + 1}.xml", _sheet_xml(sheet))
    return out.getvalue()
