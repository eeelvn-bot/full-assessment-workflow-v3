---
name: Full Assessment Workflow v3
description: 航线风险评估与整改闭环 workflow v3（仅评估与整改，不包含 v1/v2 旧流程）。
---

# Full Assessment Workflow v3 Skill

仅保留并支持 `run_workflow_v3.py`（已排除 v1/v2 旧版本）。

## Workflow v3

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

## Usage (Evaluation Only)

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

## Key Arguments

- `--kml` (required, repeatable): KML file, directory, or glob pattern.
- `--out-dir`: Output root directory. Default `output/full-workflow-v3`.
- `--xlsx`: RA log file path. Default `RA_v3.xlsx`.

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
- `RA_v3.xlsx` (updated with a row per route run)
- `output/full-workflow-v3/workflow_summary_v3.json`
