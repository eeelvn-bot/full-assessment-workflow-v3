---
name: Full Assessment Workflow
description: End-to-end sequential workflow that runs route capture, GIS analysis, population analysis, and risk assessment for one or more routes.
---

# Full Assessment Workflow Skill

Runs a complete route analysis pipeline in this order:
1. Tile Prefetch (`tile-prefetch`) - pre-download `tiles_<route>` for all KML inputs
2. Route Capture (`route_capture`)
3. GIS Analysis (`gis_analysis`)
4. Population Analysis (`population_density`)
5. Risk Assessment (`risk_assessment`, vision-only)

For the new POI/city-data driven workflow (no AI satellite recognition), use `run_workflow_v2.py`:

1. Detect covered cities from all route KML buffers.
2. Reuse/download city datasets (`city-data-downloader`) with cache skip.
3. Build one HTML per route with route+100m buffer and city layers (POI/landuse/hydro/population), supporting normal/satellite base map switch.
   - Hydro layers include `水面分布` and `河道分布` (toggle independently).
   - Also includes two transport layers from city data: `道路` and `高铁` (toggle independently).
4. Compute buffer metrics:
   - Sensitive POI counts
   - Average population density from TIF, with water-overlap area density set to 0 then weighted.
   - If route intersects high-speed rail geometry, add one `关键设施风险点` item: `穿越高铁线(...)`.
5. Write results to a new RA workbook (`RA_v2.xlsx` by default), including max altitude and height risk level.
   - Height risk thresholds (v2): `>=200m` = 高, `120m < 高度 < 200m` = 中, `<=120m` = 低.

For 航线风险评估 (evaluation only, no route planning), use `run_workflow_v3.py`:

1. Detect covered cities from all route KML buffers.
2. Reuse/download city datasets (`city-data-downloader`) with cache skip.
3. Pull open-data constraint sources aligned to `plan-auto-route`:
   - Civil airport no-fly polygons (CAAC dataset)
   - Military/heliport open-data no-fly
   - School/kindergarten hard-avoid zones
   - High-voltage/HSR/highway line-risk data
4. Evaluate existing routes with 3 dimensions:
   - Hard-constraint breach count
   - Average population density in 100m buffer
   - Normalized weighted score
5. Output RA + layered HTML + per-route meta JSON.
6. Optional整改闭环（仅单条航线）:
   - If route is non-compliant, ask/auto trigger minimal-change replanning via `plan-auto-route`.
   - Replanned route + original route are rendered in the same evaluation HTML:
     - `安全优先（3D高度）` (original)
     - `整改航线（最小改动）` (replanned)
   - RA/meta add remediation status and comparison metrics.

Hard-constraint rules in v3:
- Follows `plan-auto-route` efficiency-route constraint baseline for no-fly/school standards.
- Adds mandatory max true-height hard cap: `200m`.
- Civil airport no-fly is judged by overlap between no-fly polygons and route 100m buffer.

Supports single or multiple KML files in one command for evaluation.
When remediation mode is enabled, only one KML is supported per run.

Notes:
- Workflow passes landuse image to risk assessment for population fallback estimation.
- Visual inspection is ROI-first: system generates `{route}_vision_roi.png` (buffer-only view), then uses AI multimodal analysis in-buffer.
- `vision-notes` is optional manual supplement, not required for batch runs.

## Usage

```bash
python3 skills/full_assessment_workflow/run_workflow.py \
  --kml route2.kml \
  --out-dir output/full-workflow \
  --xlsx RA.xlsx
```

## V2 Usage

```bash
python3 skills/full_assessment_workflow/run_workflow_v2.py \
  --kml "routes/*.kml" \
  --out-dir output/full-workflow-v2 \
  --xlsx RA_v2.xlsx
```

## 航线风险评估 Usage (Evaluation Only)

```bash
python3 skills/full_assessment_workflow/run_workflow_v3.py \
  --kml "routes/*.kml" \
  --out-dir output/full-workflow-v3 \
  --xlsx RA_v3.xlsx
```

## 航线风险评估 + 整改 Usage (Single Route)

```bash
python3 skills/full_assessment_workflow/run_workflow_v3.py \
  --kml "routes/12710-xxx.kml" \
  --out-dir output/full-workflow-v3 \
  --xlsx RA_v3.xlsx \
  --replan-on-noncompliant auto \
  --replan-policy balanced
```

交互确认模式：

```bash
python3 skills/full_assessment_workflow/run_workflow_v3.py \
  --kml "routes/12710-xxx.kml" \
  --replan-on-noncompliant confirm \
  --replan-policy balanced
```

POI 分类规则可通过 JSON 配置：
- 默认：`skills/full_assessment_workflow/poi_rules.json`
- 可覆盖：`--poi-rules <path>`

### Batch Mode (Multiple Routes)

```bash
python3 skills/full_assessment_workflow/run_workflow.py \
  --kml route1.kml \
  --kml route2.kml \
  --kml "routes/*.kml" \
  --out-dir output/full-workflow \
  --xlsx RA.xlsx
```

### Per-route Vision Notes Map

Create a JSON file:

```json
{
  "route1": "30.123,120.456:临时聚集|0.80",
  "route2": "33.7815,119.2879:高密住区|0.89;33.7730,119.2522:桥梁节点|0.86"
}
```

Then run:

```bash
python3 skills/full_assessment_workflow/run_workflow.py \
  --kml route1.kml \
  --kml route2.kml \
  --vision-notes-map vision_notes.json
```

`vision_notes.json` 可按需提供，仅用于人工补充风险点。

## Key Arguments

- `--kml` (required, repeatable): KML file, directory, or glob pattern.
- `--out-dir`: Output root directory. Default `output/full-workflow`.
- `--xlsx`: RA log file path. Default `RA.xlsx`.
- `--vision-notes`: Default vision notes applied to all routes.
- `--vision-notes-map`: JSON mapping by route base name to vision notes.

### V3 Remediation Arguments

- `--replan-on-noncompliant`: `off | confirm | auto`.
  - `off`: no replanning
  - `confirm`: prompt user in terminal for non-compliant route
  - `auto`: run replanning automatically for non-compliant route
- `--replan-policy`: `balanced | strict | compliance_first`.
- `--replan-output-dir`: optional remediation output root, default `<route_dir>/replan`.
- `--replan-select-candidate`: `safety_default | efficiency`.
- `--replan-max-detour-ratio`: optional override.
- `--replan-max-mean-offset-m`: optional override.
- `--replan-reference-corridor-m`: optional override.
- `--replan-reference-deviation-weight`: optional override.
- `--replan-profile`: `fastest | balanced | safest` passed to planner.

## Outputs

For each route `<name>`:
- `output/full-workflow/<name>/<name>_capture.png`
- `output/full-workflow/<name>/<name>_vision_roi.png`
- `output/full-workflow/<name>/<name>_landuse.png`
- `output/full-workflow/<name>/<name>_population.png`
- `output/full-workflow/<name>/<name>_risk_map.png`

For v3 evaluation:
- `output/full-workflow-v3/<name>/<name>_map.html`
  - includes original route layers by default
  - when remediation succeeds, includes both original + replanned route layers in the same page
- `output/full-workflow-v3/<name>/<name>_meta_v3.json`

For v3 remediation (if triggered):
- `output/full-workflow-v3/<name>/replan/<name>_replan.kml`
- `output/full-workflow-v3/<name>/replan/<name>_replan.html`
- `output/full-workflow-v3/<name>/replan/<name>_replan_meta.json`
- `output/full-workflow-v3/<name>/replan/<name>_replan_candidates.json`

Global:
- `RA.xlsx` (updated with a row per route run)
- `output/full-workflow/workflow_summary.json`
