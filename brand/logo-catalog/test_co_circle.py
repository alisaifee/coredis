"""Drive the co-circle variant renderer."""

from __future__ import annotations

import math

from PIL import Image

from co_circle import (
    CX,
    CY,
    FIVE,
    GLYPHS,
    PURPLE,
    RED,
    TYPES,
    VARIANTS,
    _blit,
    _c_mid,
    _glyph_radius,
    _glyph_reach,
    layout_glyphs,
    render_variant,
)


def _ink_extent(name: str, scale: int) -> float:
    im = Image.new("RGB", (400, 400), (255, 255, 255))
    _blit(im, GLYPHS[name], (200, 200), scale, "#E01010")
    px = im.load()
    farthest = 0.0
    for y in range(400):
        for x in range(400):
            r, g, b = px[x, y]
            if r > 160 and g < 90 and b < 90:
                d = float((x - 200) ** 2 + (y - 200) ** 2) ** 0.5
                if d > farthest:
                    farthest = d
    return farthest


def test_variants_are_numbered_without_gaps() -> None:
    assert [v["id"] for v in VARIANTS] == list(range(1, len(VARIANTS) + 1))
    assert len(VARIANTS) == 14
    assert len({v["name"] for v in VARIANTS}) == len(VARIANTS)


def test_type_glyphs_are_redis_data_type_shapes() -> None:
    assert TYPES == ("circle", "triangle", "star", "diamond")
    assert set(TYPES) <= set(GLYPHS)
    used = {name for v in VARIANTS for name in v["glyphs"]}
    assert set(TYPES) <= used
    assert all(v["glyphs"][:4] == TYPES for v in VARIANTS)
    assert all(len(v["glyphs"]) == 5 for v in VARIANTS)
    assert "snake_twins" not in GLYPHS


def test_canonical_is_red_with_a_purple_o() -> None:
    v = VARIANTS[0]
    assert v["c_color"] == RED
    assert v["type_color"] == RED
    assert v["o_color"] == PURPLE
    assert v["bg"] == "#FFF8F5"
    colors = [p[4] for p in layout_glyphs(v)]
    assert colors == [RED, RED, PURPLE, RED, RED]
    assert v["o_width"] == v["c_width"]
    assert v["o_radius"] > 74
    assert all(row["bg"] == "#FFF8F5" for row in VARIANTS)
    assert all(row["o_width"] == row["c_width"] for row in VARIANTS)
    assert VARIANTS[0]["glyphs"] == FIVE
    assert VARIANTS[0]["glyphs"][-1] == "hexagon"
    assert VARIANTS[0]["glyph_inset"] == 0.85


def test_type_marks_share_circumradius_at_one_scale() -> None:
    scale = 80
    names = TYPES
    extents = {n: _ink_extent(n, scale) for n in names}
    vals = list(extents.values())
    assert min(vals) > 20, extents
    assert max(vals) - min(vals) <= 8, extents


def test_canonical_type_marks_stay_apart() -> None:
    v = VARIANTS[0]
    boxes: list[tuple[int, int, int]] = []
    for name, gx, gy, scale, _col in layout_glyphs(v):
        boxes.append((gx, gy, scale))
    for i, (ax, ay, as_) in enumerate(boxes):
        for bx, by, bs in boxes[i + 1 :]:
            dist = ((ax - bx) ** 2 + (ay - by) ** 2) ** 0.5
            min_sep = (as_ + bs) * 0.42
            assert dist > min_sep, (i, dist, min_sep)


def test_type_marks_complete_the_right_half() -> None:
    v = VARIANTS[0]
    ring = _glyph_radius(v)
    placed = layout_glyphs(v)
    assert [p[0] for p in placed] == list(FIVE)
    for name, gx, gy, _scale, _col in placed:
        assert gx > CX, name
        assert abs(math.hypot(gx - CX, gy - CY) - ring) < 4, name
    _name, sx, sy, _s, _c = placed[-1]
    assert sx > CX + 40
    assert sy > CY


def test_type_marks_clear_the_c_ends() -> None:
    for v in VARIANTS:
        mid = _c_mid(v["radius"], v["c_width"])
        cap_r = v["c_width"] / 2
        placed = layout_glyphs(v)
        for name, gx, gy, scale, _col in (placed[0], placed[-1]):
            reach = _glyph_reach(name, scale)
            for deg in (v["c_start"], v["c_end"]):
                a = math.radians(deg)
                cx = CX + mid * math.cos(a)
                cy = CY + mid * math.sin(a)
                dist = math.hypot(gx - cx, gy - cy)
                assert dist > cap_r + reach + 8, (v["id"], name, dist, cap_r + reach)


def test_each_variant_renders_a_unique_mark() -> None:
    hashes: set[bytes] = set()
    for v in VARIANTS:
        im = render_variant(v)
        assert im.size == (1024, 1024)
        extrema = im.getextrema()
        assert any(lo != hi for lo, hi in extrema), f"variant {v['id']} is a flat field"
        hashes.add(im.tobytes())
    assert len(hashes) == len(VARIANTS)
