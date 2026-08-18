#!/usr/bin/env python3
"""Write the 406 mark as a small brand kit under docs/source/_static/brand/."""

from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from co_circle import PAPER, RED, VARIANTS, render_variant, rgb

ROOT = Path(__file__).resolve().parent
DEST = ROOT.parents[1] / "docs" / "source" / "_static" / "brand"
INTER = "/usr/share/fonts/opentype/inter/Inter-ExtraBold.otf"
KIT_ID = 6  # gallery 406 — Large marks


def _mark() -> dict:
    return next(v for v in VARIANTS if v["id"] == KIT_ID)


def _lockup(mark: Image.Image, ink: str, dest: Path) -> None:
    w, h = 1400, 420
    canvas = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    tile = mark.resize((360, 360), Image.Resampling.LANCZOS)
    canvas.paste(tile, (48, (h - 360) // 2), tile)
    dr = ImageDraw.Draw(canvas)
    font = ImageFont.truetype(INTER, 118)
    dr.text((430, h // 2), "coredis", font=font, fill=(*rgb(ink), 255), anchor="lm")
    canvas.save(dest, "PNG")


def main() -> None:
    DEST.mkdir(parents=True, exist_ok=True)
    v = _mark()
    mark = render_variant(v, transparent=True)
    mark.save(DEST / "mark-light.png", "PNG")
    mark.save(DEST / "mark-dark.png", "PNG")
    for n in (32, 64, 128, 256):
        icon = mark.resize((n, n), Image.Resampling.LANCZOS)
        icon.save(DEST / f"icon-{n}.png", "PNG")
        icon.save(DEST / f"icon-{n}-dark.png", "PNG")
    _lockup(mark, RED, DEST / "lockup-light.png")
    _lockup(mark, PAPER, DEST / "lockup-dark.png")
    for path in sorted(DEST.glob("*.png")):
        sys.stdout.write(f"wrote {path.relative_to(DEST)} ({path.stat().st_size} bytes)\n")


if __name__ == "__main__":
    main()
