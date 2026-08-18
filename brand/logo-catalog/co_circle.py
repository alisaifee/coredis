#!/usr/bin/env python3
"""Co-circle mark: C + nested o, Redis type marks completing the circle."""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import TypedDict

from PIL import Image, ImageDraw, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "co-circle"
S = 1024
CX = 512
CY = 512
RED = "#DC2626"  # crimson that sits next to violet
PAPER = "#FFF8F5"
PURPLE = "#6D28D9"


def rgb(h: str) -> tuple[int, int, int]:
    h = h.removeprefix("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


class Variant(TypedDict):
    id: int
    name: str
    note: str
    bg: str
    c_color: str
    o_color: str
    c_width: int
    radius: int
    o_radius: int
    o_width: int
    o_shift: int
    o_fill: bool
    c_start: int
    c_end: int
    glyphs: tuple[str, ...]
    glyph_scale: int
    glyph_scales: tuple[int, ...]
    glyph_inset: float
    type_color: str
    glyph_colors: tuple[str, ...]
    arc_start: float
    arc_end: float


def _R(s: int) -> float:
    """Shared circumradius so the four type marks match in flat top view."""
    return max(6.0, s * 0.46)


def _star(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    outer = _R(s)
    inner = outer * 0.40
    pts: list[tuple[float, float]] = []
    for i in range(10):
        r = outer if i % 2 == 0 else inner
        a = math.radians(-90 + i * 36)
        pts.append((x + r * math.cos(a), y + r * math.sin(a)))
    dr.polygon(pts, fill=rgb(color))


def _circle(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = int(round(_R(s)))
    dr.ellipse((x - r, y - r, x + r, y + r), fill=rgb(color))


def _triangle(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = _R(s)
    # Equilateral, point up, same circumcircle as the other three.
    pts = []
    for deg in (-90, 30, 150):
        a = math.radians(deg)
        pts.append((x + r * math.cos(a), y + r * math.sin(a)))
    dr.polygon(pts, fill=rgb(color))


def _diamond(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = _R(s)
    dr.polygon([(x, y - r), (x + r, y), (x, y + r), (x - r, y)], fill=rgb(color))


def _ngon(
    dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str, n: int, rot: float
) -> None:
    x, y = xy
    r = _R(s)
    pts = []
    for i in range(n):
        a = math.radians(rot + i * 360 / n)
        pts.append((x + r * math.cos(a), y + r * math.sin(a)))
    dr.polygon(pts, fill=rgb(color))


def _pentagon(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    _ngon(dr, xy, s, color, 5, -90)


def _hexagon(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    _ngon(dr, xy, s, color, 6, -90)


def _hex_flat(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    _ngon(dr, xy, s, color, 6, 0)


def _octagon(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    _ngon(dr, xy, s, color, 8, -90 + 22.5)


def _square(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    _ngon(dr, xy, s, color, 4, -45)


def _plus(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = _R(s)
    t = r * 0.32
    dr.polygon([(x - t, y - r), (x + t, y - r), (x + t, y + r), (x - t, y + r)], fill=rgb(color))
    dr.polygon([(x - r, y - t), (x + r, y - t), (x + r, y + t), (x - r, y + t)], fill=rgb(color))


def _chevron(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = _R(s)
    dr.polygon(
        [
            (x + r, y),
            (x - r * 0.55, y - r),
            (x - r * 0.55, y + r),
        ],
        fill=rgb(color),
    )


def _capsule(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = _R(s)
    w = r * 0.62
    dr.rounded_rectangle((x - w, y - r, x + w, y + r), radius=w, fill=rgb(color))


def _ring(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = int(round(_R(s)))
    w = max(3, int(round(r * 0.38)))
    dr.ellipse((x - r, y - r, x + r, y + r), outline=rgb(color), width=w)


def _bars(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = _R(s)
    h = r * 0.20
    step = r * 0.70
    w = r * 0.88
    for i in (-1, 0, 1):
        cy = y + i * step
        dr.rounded_rectangle((x - w, cy - h, x + w, cy + h), radius=h, fill=rgb(color))


_INTER = "/usr/share/fonts/opentype/inter/Inter-ExtraBold.otf"
PY_BLUE = "#3776AB"
PY_YELLOW = "#FFD43B"


def _paste_rgba(dr: ImageDraw.ImageDraw, sprite: Image.Image, xy: tuple[int, int]) -> None:
    x, y = xy
    host = dr._image
    px = int(x - sprite.width / 2)
    py = int(y - sprite.height / 2)
    if sprite.mode != "RGBA":
        sprite = sprite.convert("RGBA")
    host.paste(sprite, (px, py), sprite)


def _fit_ink(im: Image.Image, size: int) -> Image.Image:
    bbox = im.getbbox()
    if bbox is None:
        return Image.new("RGBA", (size, size), (0, 0, 0, 0))
    cropped = im.crop(bbox)
    side = max(cropped.size)
    square = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    square.paste(cropped, ((side - cropped.width) // 2, (side - cropped.height) // 2), cropped)
    return square.resize((size, size), Image.Resampling.LANCZOS)


def _char_sprite(
    ch: str,
    font_path: str,
    s: int,
    color: str,
    *,
    rotate: float = 0,
    thicken: int = 0,
    embed: bool = False,
    size_mul: float = 1.0,
) -> Image.Image:
    target = max(8, int(round(2 * _R(s) * size_mul)))
    if embed:
        font = ImageFont.truetype(font_path, 109)
        raw = Image.new("RGBA", (200, 200), (0, 0, 0, 0))
        ImageDraw.Draw(raw).text((20, 20), ch, font=font, embedded_color=True)
        if rotate:
            raw = raw.rotate(rotate, resample=Image.Resampling.BICUBIC, expand=True)
        return _fit_ink(raw, target)
    px = max(24, int(s * 1.35))
    canvas = px * 3
    raw = Image.new("RGBA", (canvas, canvas), (0, 0, 0, 0))
    ImageDraw.Draw(raw).text(
        (canvas // 5, canvas // 5),
        ch,
        font=ImageFont.truetype(font_path, px),
        fill=(255, 255, 255, 255),
    )
    if thicken:
        alpha = raw.split()[-1].filter(ImageFilter.MaxFilter(thicken * 2 + 1))
        raw = Image.new("RGBA", raw.size, (0, 0, 0, 0))
        raw.putalpha(alpha)
    if rotate:
        raw = raw.rotate(rotate, resample=Image.Resampling.BICUBIC, expand=True)
    fitted = _fit_ink(raw, target)
    tint = Image.new("RGBA", fitted.size, (*rgb(color), 255))
    tint.putalpha(fitted.split()[-1])
    return tint


def _letter_s(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    _paste_rgba(dr, _char_sprite("S", _INTER, s, color), xy)


def _hex_s(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    _hexagon(dr, xy, s, color)
    _paste_rgba(dr, _char_sprite("S", _INTER, int(s * 0.62), PAPER), xy)


def _wrap_body(
    dr: ImageDraw.ImageDraw,
    x: float,
    y: float,
    r: float,
    color: str,
    eye: str,
    flip: bool,
    *,
    t_mul: float = 0.74,
    arm: float = 0.55,
    rad_k: float = 0.50,
    eyes: bool = True,
) -> None:
    """One folded body of the pair: fat L that reads as a shape, not a pipe."""
    t = r * t_mul
    rad = max(2, int(round(t * rad_k)))
    drop = r * arm * 0.55
    if flip:
        dr.rounded_rectangle((x - r * arm, y - r, x + r, y - r + t), radius=rad, fill=rgb(color))
        dr.rounded_rectangle(
            (x - r * arm, y - r, x - r * arm + t, y + drop), radius=rad, fill=rgb(color)
        )
        ex, ey = x + r - t * 0.48, y - r + t * 0.48
    else:
        dr.rounded_rectangle((x - r, y + r - t, x + r * arm, y + r), radius=rad, fill=rgb(color))
        dr.rounded_rectangle(
            (x + r * arm - t, y - drop, x + r * arm, y + r), radius=rad, fill=rgb(color)
        )
        ex, ey = x - r + t * 0.48, y + r - t * 0.48
    if eyes:
        er = max(1.6, t * 0.16)
        dr.ellipse((ex - er, ey - er, ex + er, ey + er), fill=rgb(eye))


def _twins(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    r = _R(s)
    _wrap_body(dr, x, y, r, color, PAPER, False)
    _wrap_body(dr, x, y, r, color, PAPER, True)


def _twins_co(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    del color
    x, y = xy
    r = _R(s)
    _wrap_body(dr, x, y, r, RED, PAPER, False)
    _wrap_body(dr, x, y, r, PURPLE, PAPER, True)


def _twins_py(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    del color
    x, y = xy
    r = _R(s)
    _wrap_body(dr, x, y, r, PY_BLUE, PAPER, False)
    _wrap_body(dr, x, y, r, PY_YELLOW, PAPER, True)


def _wrap(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    x, y = xy
    _wrap_body(dr, x, y, _R(s), color, PAPER, False)


def _stamp_pair(
    dr: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    s: int,
    a: str,
    b: str,
    *,
    t_mul: float,
    arm: float,
    rad_k: float,
    eyes: bool,
    rot: float,
    size_mul: float,
) -> None:
    target = max(8, int(round(2 * _R(s) * size_mul)))
    pad = max(target * 2, int(s * 3))
    im = Image.new("RGBA", (pad, pad), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    cx = cy = pad / 2
    rr = _R(s)
    _wrap_body(d, cx, cy, rr, a, PAPER, False, t_mul=t_mul, arm=arm, rad_k=rad_k, eyes=eyes)
    _wrap_body(d, cx, cy, rr, b, PAPER, True, t_mul=t_mul, arm=arm, rad_k=rad_k, eyes=eyes)
    if rot:
        im = im.rotate(rot, resample=Image.Resampling.BICUBIC, expand=True)
    _paste_rgba(dr, _fit_ink(im, target), xy)


def _pair_draw(
    *,
    a: str = RED,
    b: str = PURPLE,
    t_mul: float = 0.74,
    arm: float = 0.55,
    rad_k: float = 0.50,
    eyes: bool = True,
    rot: float = 0.0,
    size_mul: float = 1.10,
):
    def draw(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
        del color
        _stamp_pair(
            dr,
            xy,
            s,
            a,
            b,
            t_mul=t_mul,
            arm=arm,
            rad_k=rad_k,
            eyes=eyes,
            rot=rot,
            size_mul=size_mul,
        )

    return draw


TILT = -28.0


def _turn(pts: list[tuple[float, float]], rot: float) -> list[tuple[float, float]]:
    a = math.radians(rot)
    c, sn = math.cos(a), math.sin(a)
    return [(px * c - py * sn, px * sn + py * c) for px, py in pts]


def _ess_bars(
    r: float, *, t_mul: float, arm: float
) -> tuple[list[tuple[float, float, float, float]], list[tuple[float, float, float, float]]]:
    """Two thick L-bars that read as an S. Sharp corners, same weight as the square."""
    t = r * t_mul
    drop = r * arm * 0.55
    # bottom (red): head SW, fold up the inner right
    bot = [(-r, r - t, r * arm, r), (r * arm - t, -drop, r * arm, r)]
    # top (purple): head NE, fold down the inner left
    top = [(-r * arm, -r, r, -r + t), (-r * arm, -r, -r * arm + t, drop)]
    return top, bot


def _ess_draw(
    dr: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    s: int,
    *,
    rot: float = TILT,
    t_mul: float = 0.74,
    span: float = 0.55,
    a: str = PURPLE,
    b: str = RED,
) -> None:
    x, y = xy
    r = _R(s) * 1.12
    top, bot = _ess_bars(r, t_mul=t_mul, arm=span)
    sample = []
    for x0, y0, x1, y1 in top + bot:
        sample.extend(_turn([(x0, y0), (x1, y0), (x1, y1), (x0, y1)], rot))
    far = max(math.hypot(px, py) for px, py in sample)
    k = r / far

    def draw_bar(box: tuple[float, float, float, float], color: str) -> None:
        x0, y0, x1, y1 = box
        pts = _turn([(x0, y0), (x1, y0), (x1, y1), (x0, y1)], rot)
        dr.polygon([(x + px * k, y + py * k) for px, py in pts], fill=rgb(color))

    for box in bot:
        draw_bar(box, b)
    for box in top:
        draw_bar(box, a)


def _ess_fn(
    *,
    rot: float = TILT,
    t_mul: float = 0.74,
    span: float = 0.55,
    a: str = PURPLE,
    b: str = RED,
):
    def draw(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
        del color
        _ess_draw(dr, xy, s, rot=rot, t_mul=t_mul, span=span, a=a, b=b)

    return draw


def _twins_block(dr: ImageDraw.ImageDraw, xy: tuple[int, int], s: int, color: str) -> None:
    del color
    x, y = xy
    r = _R(s)
    rad = max(2, int(round(r * 0.42)))
    dr.rounded_rectangle((x - r, y - r * 0.10, x + r * 0.40, y + r), radius=rad, fill=rgb(RED))
    dr.rounded_rectangle((x - r * 0.40, y - r, x + r, y + r * 0.10), radius=rad, fill=rgb(PURPLE))
    er = max(1.6, r * 0.12)
    dr.ellipse(
        (x - r * 0.52 - er, y + r * 0.46 - er, x - r * 0.52 + er, y + r * 0.46 + er),
        fill=rgb(PAPER),
    )
    dr.ellipse(
        (x + r * 0.52 - er, y - r * 0.46 - er, x + r * 0.52 + er, y - r * 0.46 + er),
        fill=rgb(PAPER),
    )


GLYPHS = {
    "star": _star,
    "circle": _circle,
    "triangle": _triangle,
    "diamond": _diamond,
    "pentagon": _pentagon,
    "hexagon": _hexagon,
    "hex_flat": _hex_flat,
    "octagon": _octagon,
    "square": _square,
    "plus": _plus,
    "chevron": _chevron,
    "capsule": _capsule,
    "ring": _ring,
    "bars": _bars,
    "letter_s": _letter_s,
    "hex_s": _hex_s,
    "twins": _twins,
    "twins_co": _twins_co,
    "twins_py": _twins_py,
    "wrap": _wrap,
    "twins_rp": _pair_draw(),
    "twins_pr": _pair_draw(a=PURPLE, b=RED),
    "twins_fat": _pair_draw(t_mul=0.86, arm=0.62, size_mul=1.16),
    "twins_soft": _pair_draw(t_mul=0.78, rad_k=0.72, arm=0.50),
    "twins_tilt": _pair_draw(rot=-28),
    "twins_sq": _pair_draw(rot=45),
    "twins_quiet": _pair_draw(eyes=False, t_mul=0.80, arm=0.58),
    "twins_fill": _pair_draw(t_mul=0.90, arm=0.68, rad_k=0.42, size_mul=1.18),
    "twins_block": _twins_block,
    "ess": _ess_fn(),
    "ess_swap": _ess_fn(a=RED, b=PURPLE),
    "ess_heavy": _ess_fn(t_mul=0.84, span=0.62),
    "ess_more": _ess_fn(rot=-40),
    "ess_less": _ess_fn(rot=-16),
    "ess_wide": _ess_fn(span=0.68, t_mul=0.76),
    "ess_align": _ess_fn(rot=-36),
}
TYPES = ("circle", "triangle", "star", "diamond")
FIVE = (*TYPES, "hexagon")


def _v(
    id: int,
    name: str,
    note: str,
    *,
    bg: str = PAPER,
    c_color: str = RED,
    o_color: str = PURPLE,
    c_width: int = 64,
    radius: int = 250,
    o_radius: int = 92,
    o_width: int = 64,
    o_shift: int = 14,
    o_fill: bool = False,
    c_start: int = 90,
    c_end: int = 270,
    glyphs: tuple[str, ...] = FIVE,
    glyph_scale: int = 72,
    glyph_scales: tuple[int, ...] = (),
    glyph_inset: float = 0.85,
    type_color: str = RED,
    glyph_colors: tuple[str, ...] = (RED, RED, PURPLE, RED, RED),
    arc_start: float = 308.0,
    arc_end: float = 52.0,
) -> Variant:
    return Variant(
        id=id,
        name=name,
        note=note,
        bg=bg,
        c_color=c_color,
        o_color=o_color,
        c_width=c_width,
        radius=radius,
        o_radius=o_radius,
        o_width=o_width,
        o_shift=o_shift,
        o_fill=o_fill,
        c_start=c_start,
        c_end=c_end,
        glyphs=glyphs,
        glyph_scale=glyph_scale,
        glyph_scales=glyph_scales,
        glyph_inset=glyph_inset,
        type_color=type_color,
        glyph_colors=glyph_colors,
        arc_start=arc_start,
        arc_end=arc_end,
    )


_ARC4 = TYPES


VARIANTS: list[Variant] = [
    _v(1, "Lead", "Purple hex, o larger, C and o the same stroke, o sat in the C."),
    _v(2, "Bigger o", "o radius 104, same stroke as the C.", o_radius=104),
    _v(3, "Smaller o", "o radius 82, same stroke as the C.", o_radius=82),
    _v(4, "Heavy stroke", "C and o both 76.", c_width=76, o_width=76),
    _v(5, "Light stroke", "C and o both 52.", c_width=52, o_width=52),
    _v(6, "Large marks", "Type marks at 84.", glyph_scale=84),
    _v(7, "Small marks", "Type marks at 60.", glyph_scale=60),
    _v(8, "Big hex", "Purple hex larger than the four.", glyph_scales=(68, 68, 68, 68, 92)),
    _v(9, "Small hex", "Purple hex smaller than the four.", glyph_scales=(76, 76, 76, 76, 56)),
    _v(10, "o deeper", "o sat further into the C.", o_shift=6),
    _v(11, "o out a little", "o a bit less deep in the C.", o_shift=24),
    _v(12, "Marks in", "Marks closer to the o.", glyph_inset=1.2),
    _v(13, "Marks out", "Marks on the C stroke.", glyph_inset=0.55),
    _v(14, "Flat hex", "Purple hex sitting flat.", glyphs=(*_ARC4, "hex_flat")),
]


assert [v["id"] for v in VARIANTS] == list(range(1, len(VARIANTS) + 1))
assert len({v["name"] for v in VARIANTS}) == len(VARIANTS)


def _glyph_color(v: Variant, index: int) -> str:
    if v["glyph_colors"]:
        return v["glyph_colors"][index % len(v["glyph_colors"])]
    if v["type_color"]:
        return v["type_color"]
    return v["c_color"]


def _scales(v: Variant) -> list[int]:
    n = len(v["glyphs"])
    if v["glyph_scales"]:
        return [v["glyph_scales"][i % len(v["glyph_scales"])] for i in range(n)]
    return [v["glyph_scale"]] * n


def _c_mid(radius: float, width: float) -> float:
    return max(1.0, radius - width / 2)


def _glyph_reach(name: str, scale: int) -> float:
    del name
    return _R(scale)


def _glyph_radius(v: Variant) -> float:
    """How far out the type marks sit. Inset 0 is the C outer edge; 1 is the inner edge."""
    return max(80.0, v["radius"] - v["c_width"] * v["glyph_inset"])


def _arc_c(
    dr: ImageDraw.ImageDraw,
    cx: int,
    cy: int,
    radius: int,
    start: int,
    end: int,
    color: str,
    width: int,
) -> None:
    """C as a thick arc with square terminals."""
    box = (cx - radius, cy - radius, cx + radius, cy + radius)
    dr.arc(box, start, end, fill=rgb(color), width=width)


def render_variant(v: Variant, *, transparent: bool = False) -> Image.Image:
    factor = 4
    size = (S * factor, S * factor)
    if transparent:
        im = Image.new("RGBA", size, (0, 0, 0, 0))
        hole: tuple[int, ...] = (0, 0, 0, 0)
        ring = (*rgb(v["o_color"]), 255)
    else:
        im = Image.new("RGB", size, rgb(v["bg"]))
        hole = rgb(v["bg"])
        ring = rgb(v["o_color"])
    dr = ImageDraw.Draw(im)
    cx, cy = CX * factor, CY * factor
    r = v["radius"] * factor
    _arc_c(dr, cx, cy, r, v["c_start"], v["c_end"], v["c_color"], v["c_width"] * factor)

    ox = cx + v["o_shift"] * factor
    oy = cy
    o_r = v["o_radius"] * factor
    o_w = v["o_width"] * factor
    dr.ellipse((ox - o_r, oy - o_r, ox + o_r, oy + o_r), fill=ring)
    ir = max(1, o_r - o_w)
    dr.ellipse((ox - ir, oy - ir, ox + ir, oy + ir), fill=hole)

    for name, gx, gy, scale, col in layout_glyphs(v):
        GLYPHS[name](dr, (gx * factor, gy * factor), scale * factor, col)
    return im.resize((S, S), Image.Resampling.LANCZOS)


def layout_glyphs(v: Variant) -> list[tuple[str, int, int, int, str]]:
    """Sit the type marks on the right half of the C's circle, completing it."""
    scales = _scales(v)
    glyph_r = _glyph_radius(v)
    start, end = v["arc_start"], v["arc_end"]
    span = (end - start) % 360
    n = len(v["glyphs"])
    out: list[tuple[str, int, int, int, str]] = []
    for i, name in enumerate(v["glyphs"]):
        t = 0.5 if n == 1 else i / (n - 1)
        ang = math.radians(start + span * t)
        gx = int(CX + glyph_r * math.cos(ang))
        gy = int(CY + glyph_r * math.sin(ang))
        out.append((name, gx, gy, scales[i], _glyph_color(v, i)))
    return out


def _blit(
    im: Image.Image,
    fn,
    xy: tuple[int, int],
    s: int,
    color: str,
) -> None:
    factor = 4
    pad = s * 2 + 16
    hi = Image.new("RGBA", (pad * factor, pad * factor), (0, 0, 0, 0))
    hdr = ImageDraw.Draw(hi)
    c = pad * factor // 2
    fn(hdr, (c, c), s * factor, color)
    lo = hi.resize((pad, pad), Image.Resampling.LANCZOS)
    x, y = xy
    im.paste(lo, (x - pad // 2, y - pad // 2), lo)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    keep = {f"{v['id']:02d}.png" for v in VARIANTS}
    for stale in OUT.glob("*.png"):
        if stale.name not in keep:
            stale.unlink()
    for v in VARIANTS:
        dest = OUT / f"{v['id']:02d}.png"
        render_variant(v).save(dest, "PNG")
        sys.stdout.write(f"wrote {dest.name} {v['name']} ({dest.stat().st_size} bytes)\n")


if __name__ == "__main__":
    main()
