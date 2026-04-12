#!/usr/bin/env python3
"""Generate a simplified printable STL for a Tesla Model 3 Highland at 1:100."""

from __future__ import annotations

import math
from pathlib import Path


# Tesla owner manual dimensions for the 2024+ Model 3 (RWD/Long Range).
# Source used while generating this asset:
# https://www.tesla.com/ownersmanual/model3/en_us/GUID-56562137-FC31-4110-A13C-9A9FC6657BF0.html
LENGTH_MM = 4720 / 100
WIDTH_MM = 1850 / 100
HEIGHT_MM = 1440 / 100

NX = 96
NY = 34
OUTPUT_PATH = Path("models/tesla_model_3_highlander_1_100.stl")


def smoothstep(value: float) -> float:
    value = max(0.0, min(1.0, value))
    return value * value * (3.0 - 2.0 * value)


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def piecewise(points: list[tuple[float, float]], t: float) -> float:
    if t <= points[0][0]:
        return points[0][1]
    if t >= points[-1][0]:
        return points[-1][1]

    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if x0 <= t <= x1:
            local_t = smoothstep((t - x0) / (x1 - x0))
            return lerp(y0, y1, local_t)
    return points[-1][1]


def half_width_profile(t: float) -> float:
    profile = [
        (0.00, 0.04),
        (0.04, 0.34),
        (0.10, 0.72),
        (0.18, 0.92),
        (0.28, 1.00),
        (0.76, 1.00),
        (0.88, 0.78),
        (0.96, 0.42),
        (1.00, 0.05),
    ]
    return 0.5 * WIDTH_MM * piecewise(profile, t)


def center_height_profile(t: float) -> float:
    profile = [
        (0.00, 0.18),
        (0.08, 0.26),
        (0.18, 0.48),
        (0.30, 0.93),
        (0.46, 1.00),
        (0.64, 0.98),
        (0.78, 0.80),
        (0.90, 0.60),
        (1.00, 0.34),
    ]
    return HEIGHT_MM * piecewise(profile, t)


def side_height_profile(t: float, center_height: float) -> float:
    shoulder_factor = 0.40 + 0.13 * math.exp(-((t - 0.52) / 0.30) ** 2)
    return center_height * shoulder_factor


def top_surface_height(t: float, v: float) -> float:
    center_height = center_height_profile(t)
    side_height = side_height_profile(t, center_height)
    roof_curve = max(0.0, 1.0 - abs(v) ** 2.4) ** 0.72
    crown = side_height + (center_height - side_height) * roof_curve

    # Slight greenhouse taper makes the roof feel less slab-sided.
    window_dip = 0.06 * HEIGHT_MM * math.exp(-((t - 0.54) / 0.22) ** 2) * abs(v) ** 1.6
    return max(0.0, crown - window_dip)


def bottom_surface_height(_t: float, _v: float) -> float:
    return 0.0


def build_vertices() -> tuple[list[tuple[float, float, float]], list[tuple[float, float, float]]]:
    top_vertices: list[tuple[float, float, float]] = []
    bottom_vertices: list[tuple[float, float, float]] = []

    for ix in range(NX):
        # Cosine-spaced samples put more fidelity at the nose and tail.
        t = 0.5 * (1.0 - math.cos(math.pi * ix / (NX - 1)))
        x = t * LENGTH_MM
        half_width = half_width_profile(t)

        for iy in range(NY):
            v = -1.0 + (2.0 * iy / (NY - 1))
            y = v * half_width
            top_vertices.append((x, y, top_surface_height(t, v)))
            bottom_vertices.append((x, y, bottom_surface_height(t, v)))

    return top_vertices, bottom_vertices


def top_index(ix: int, iy: int) -> int:
    return ix * NY + iy


def bottom_index(ix: int, iy: int, top_count: int) -> int:
    return top_count + ix * NY + iy


def add_quad(faces: list[tuple[int, int, int]], a: int, b: int, c: int, d: int) -> None:
    faces.append((a, b, c))
    faces.append((a, c, d))


def build_faces(top_count: int) -> list[tuple[int, int, int]]:
    faces: list[tuple[int, int, int]] = []

    for ix in range(NX - 1):
        for iy in range(NY - 1):
            a = top_index(ix, iy)
            b = top_index(ix + 1, iy)
            c = top_index(ix + 1, iy + 1)
            d = top_index(ix, iy + 1)
            add_quad(faces, a, b, c, d)

            ab = bottom_index(ix, iy, top_count)
            bb = bottom_index(ix + 1, iy, top_count)
            cb = bottom_index(ix + 1, iy + 1, top_count)
            db = bottom_index(ix, iy + 1, top_count)
            add_quad(faces, ab, db, cb, bb)

    for ix in range(NX - 1):
        a = top_index(ix, 0)
        b = top_index(ix + 1, 0)
        c = bottom_index(ix + 1, 0, top_count)
        d = bottom_index(ix, 0, top_count)
        add_quad(faces, a, b, c, d)

        a = top_index(ix, NY - 1)
        b = bottom_index(ix, NY - 1, top_count)
        c = bottom_index(ix + 1, NY - 1, top_count)
        d = top_index(ix + 1, NY - 1)
        add_quad(faces, a, b, c, d)

    for iy in range(NY - 1):
        a = top_index(0, iy)
        b = bottom_index(0, iy, top_count)
        c = bottom_index(0, iy + 1, top_count)
        d = top_index(0, iy + 1)
        add_quad(faces, a, b, c, d)

        a = top_index(NX - 1, iy)
        b = top_index(NX - 1, iy + 1)
        c = bottom_index(NX - 1, iy + 1, top_count)
        d = bottom_index(NX - 1, iy, top_count)
        add_quad(faces, a, b, c, d)

    return faces


def normal(a: tuple[float, float, float], b: tuple[float, float, float], c: tuple[float, float, float]) -> tuple[float, float, float]:
    ux, uy, uz = b[0] - a[0], b[1] - a[1], b[2] - a[2]
    vx, vy, vz = c[0] - a[0], c[1] - a[1], c[2] - a[2]
    nx = uy * vz - uz * vy
    ny = uz * vx - ux * vz
    nz = ux * vy - uy * vx
    length = math.sqrt(nx * nx + ny * ny + nz * nz) or 1.0
    return nx / length, ny / length, nz / length


def write_ascii_stl(path: Path, vertices: list[tuple[float, float, float]], faces: list[tuple[int, int, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="ascii") as handle:
        handle.write("solid tesla_model_3_highlander_1_100\n")
        for ia, ib, ic in faces:
            a, b, c = vertices[ia], vertices[ib], vertices[ic]
            nx, ny, nz = normal(a, b, c)
            handle.write(f"  facet normal {nx:.6e} {ny:.6e} {nz:.6e}\n")
            handle.write("    outer loop\n")
            handle.write(f"      vertex {a[0]:.6e} {a[1]:.6e} {a[2]:.6e}\n")
            handle.write(f"      vertex {b[0]:.6e} {b[1]:.6e} {b[2]:.6e}\n")
            handle.write(f"      vertex {c[0]:.6e} {c[1]:.6e} {c[2]:.6e}\n")
            handle.write("    endloop\n")
            handle.write("  endfacet\n")
        handle.write("endsolid tesla_model_3_highlander_1_100\n")


def bounding_box(vertices: list[tuple[float, float, float]]) -> tuple[float, float, float]:
    xs = [v[0] for v in vertices]
    ys = [v[1] for v in vertices]
    zs = [v[2] for v in vertices]
    return max(xs) - min(xs), max(ys) - min(ys), max(zs) - min(zs)


def main() -> None:
    top_vertices, bottom_vertices = build_vertices()
    vertices = top_vertices + bottom_vertices
    faces = build_faces(len(top_vertices))
    write_ascii_stl(OUTPUT_PATH, vertices, faces)

    length, width, height = bounding_box(vertices)
    print(f"Wrote {OUTPUT_PATH}")
    print(f"Triangles: {len(faces)}")
    print(f"Bounding box (mm): {length:.2f} x {width:.2f} x {height:.2f}")


if __name__ == "__main__":
    main()
