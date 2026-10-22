#!/usr/bin/env python3
"""Export print SVGs from already validated Archify HTML, without executing JS.

Preserves rendered SVG geometry and embeds the delivered font/style assets.
Does not replace Archify validation/delivery or claim viewer visual acceptance.
"""
from pathlib import Path
import hashlib
import json
import re
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent


def main():
    exports = []
    for path in sorted((ROOT / "figures").glob("0[1-5]-*.html")):
        if path.stem.endswith("visual-check"):
            continue
        html = path.read_text()
        styles = re.findall(r"<style[^>]*>([\s\S]*?)</style>", html)
        matches = re.findall(r"<svg\b[\s\S]*?</svg>", html)
        if len(matches) != 1 or not styles:
            raise ValueError(f"Expected one delivered SVG and embedded styles: {path.name}")
        svg = matches[0]
        svg = svg.replace("<svg ", '<svg xmlns="http://www.w3.org/2000/svg" data-theme="light" ', 1)
        # Static print figure: no viewer controls or executable scripts.
        css = "\n".join(styles)
        if "]]>" in css:
            raise ValueError("Unexpected CDATA terminator")
        boundary = svg.index(">") + 1
        svg = svg[:boundary] + '<style><![CDATA[' + css + ']]></style><rect width="100%" height="100%" fill="white"/>' + svg[boundary:]
        svg = '<?xml version="1.0" encoding="UTF-8"?>\n' + svg + '\n'
        svg = '\n'.join(line.rstrip() for line in svg.splitlines()) + '\n'
        parsed = ET.fromstring(svg)
        if parsed.findall(".//{http://www.w3.org/2000/svg}script"):
            raise ValueError("Unexpected script in exported figure")
        destination = path.with_suffix(".svg")
        destination.write_text(svg)
        exports.append({"source": path.name, "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "svg": destination.name, "svg_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                        "viewBox": parsed.attrib["viewBox"]})
    if len(exports) != 5:
        raise ValueError(f"Expected five figures, got {len(exports)}")
    (ROOT / "figures" / "exports.json").write_text(json.dumps(exports, indent=2) + "\n")
    print("Exported five standalone light-theme SVG figures; XML parsed successfully.")


if __name__ == "__main__":
    main()
