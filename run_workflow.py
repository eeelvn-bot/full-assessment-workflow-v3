import argparse
import json
import subprocess
from pathlib import Path


def run_cmd(cmd):
    print("RUN:", " ".join(str(c) for c in cmd))
    subprocess.run(cmd, check=True)


def load_vision_map(path):
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_kmls(kml_args):
    files = []
    for item in kml_args:
        p = Path(item)
        if any(ch in item for ch in ["*", "?", "["]):
            files.extend(sorted(Path(".").glob(item)))
        elif p.is_dir():
            files.extend(sorted(p.glob("*.kml")))
        else:
            files.append(p)
    unique = []
    seen = set()
    for f in files:
        abs_p = f.resolve()
        if abs_p not in seen:
            seen.add(abs_p)
            unique.append(abs_p)
    return unique


def main():
    parser = argparse.ArgumentParser(
        description="Run full route analysis workflow (capture -> GIS -> population -> risk assessment)."
    )
    parser.add_argument(
        "--kml",
        action="append",
        required=True,
        help="KML file path, directory, or glob. Repeat for multiple inputs.",
    )
    parser.add_argument("--out-dir", default="output/full-workflow", help="Output directory.")
    parser.add_argument("--xlsx", default="RA.xlsx", help="Risk assessment log xlsx path.")
    parser.add_argument(
        "--vision-notes",
        default="",
        help="Default vision notes for all routes. Format: lat,lon:desc|conf;...",
    )
    parser.add_argument(
        "--vision-notes-map",
        help="Optional JSON file mapping route base name (without .kml) to vision notes.",
    )
    parser.add_argument("--capture-zoom", type=int, default=16)
    parser.add_argument("--capture-wait", type=int, default=4)
    parser.add_argument("--gis-zoom", type=int, default=14)
    parser.add_argument("--gis-wait", type=int, default=5)
    parser.add_argument("--population-zoom", type=int, default=12)
    parser.add_argument("--population-wait", type=int, default=8)
    parser.add_argument("--vision-mode", choices=["vlm", "notes"], default="vlm")
    parser.add_argument("--vlm-model", default="gemini-3-flash-preview")
    parser.add_argument("--vlm-max-points", type=int, default=8)
    parser.add_argument("--vlm-api-base", help="Optional Gemini API base URL.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = (root / args.xlsx).resolve()
    pop_tif = (root / "data/population/chn_pd_2020_1km.tif").resolve()
    vision_map = load_vision_map(args.vision_notes_map)

    kml_files = resolve_kmls(args.kml)
    if not kml_files:
        raise FileNotFoundError("No KML files found from provided --kml inputs.")

    # 0) Tile prefetch for all routes before analysis
    prefetch_cmd = [
        "python3",
        str(root / "skills/tile-prefetch/scripts/prefetch_tiles.py"),
    ]
    for kml in kml_files:
        prefetch_cmd.extend(["--kml", str(kml)])
    run_cmd(prefetch_cmd)

    summary = []
    for kml in kml_files:
        if not kml.exists():
            raise FileNotFoundError(f"KML not found: {kml}")
        route_name = kml.stem
        route_dir = out_dir / route_name
        route_dir.mkdir(parents=True, exist_ok=True)

        capture_png = route_dir / f"{route_name}_capture.png"
        landuse_png = route_dir / f"{route_name}_landuse.png"
        population_png = route_dir / f"{route_name}_population.png"
        risk_prefix = route_dir / route_name
        tiles_dir = root / f"data/population/tiles_{route_name}"
        vision_notes = vision_map.get(route_name, args.vision_notes)

        # 1) Route capture
        run_cmd(
            [
                "python3",
                str(root / "skills/route_capture/capture_route.py"),
                "--kml",
                str(kml),
                "--output",
                str(capture_png),
                "--zoom",
                str(args.capture_zoom),
                "--wait",
                str(args.capture_wait),
            ]
        )

        # 2) GIS analysis (landuse + roads)
        run_cmd(
            [
                "python3",
                str(root / "skills/gis_analysis/analyze_landuse.py"),
                "--kml",
                str(kml),
                "--output",
                str(landuse_png),
                "--type",
                "landuse",
                "--roads",
                "--zoom",
                str(args.gis_zoom),
                "--wait",
                str(args.gis_wait),
            ]
        )

        # 3) Population analysis (auto tile prep in script)
        run_cmd(
            [
                "python3",
                str(root / "skills/population_density/analyze_population.py"),
                "--kml",
                str(kml),
                "--output",
                str(population_png),
                "--tiles",
                str(tiles_dir),
                "--zoom",
                str(args.population_zoom),
                "--wait",
                str(args.population_wait),
            ]
        )

        # 4) Risk assessment (vision-only)
        risk_cmd = [
            "python3",
            str(root / "skills/risk_assessment/assess_risk.py"),
            "--kml",
            str(kml),
            "--satellite",
            str(capture_png),
            "--landuse-image",
            str(landuse_png),
            "--population-image",
            str(population_png),
            "--tiles-dir",
            str(tiles_dir),
            "--prefix",
            str(risk_prefix),
            "--pop-tif",
            str(pop_tif),
            "--xlsx",
            str(xlsx_path),
            "--vision-mode",
            str(args.vision_mode),
            "--vlm-model",
            str(args.vlm_model),
            "--vlm-max-points",
            str(args.vlm_max_points),
        ]
        if args.vlm_api_base:
            risk_cmd.extend(["--vlm-api-base", args.vlm_api_base])
        if vision_notes:
            risk_cmd.extend(["--vision-notes", vision_notes])
        run_cmd(risk_cmd)

        summary.append(
            {
                "route": route_name,
                "kml": str(kml),
                "capture": str(capture_png),
                "vision_roi": str(route_dir / f"{route_name}_vision_roi.png"),
                "landuse": str(landuse_png),
                "population": str(population_png),
                "risk_map": str(route_dir / f"{route_name}_risk_map.png"),
            }
        )

    summary_path = out_dir / "workflow_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({"routes": summary, "xlsx": str(xlsx_path)}, f, ensure_ascii=False, indent=2)
    print(f"Workflow completed for {len(summary)} route(s).")
    print(f"Summary: {summary_path}")
    print(f"Risk log: {xlsx_path}")


if __name__ == "__main__":
    main()
