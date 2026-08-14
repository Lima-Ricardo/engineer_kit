"""Safe terminal and HTML renderers for ProfileReport."""

from __future__ import annotations

from html import escape

from engineer_kit.profiling.model import ProfileReport


def _pct(value: int, total: int) -> str:
    if total <= 0:
        return "0.00%"
    return f"{(value / total) * 100:.2f}%"


def render_text(report: ProfileReport) -> str:
    """Render an aggregate-only terminal report."""
    lines = [
        f"DATA PROFILE v{report.version}",
        f"scope={report.scope} | records={report.records_analyzed:,}",
        f"metrics={','.join(report.requested_metrics)}",
        "",
    ]
    if report.duplicates is not None:
        lines.extend(
            [
                "DATA QUALITY",
                f"unique_rows={report.duplicates.unique_rows:,}",
                f"duplicate_rows={report.duplicates.duplicate_rows:,} "
                f"({_pct(report.duplicates.duplicate_rows, report.records_analyzed)})",
                "",
            ]
        )
    quality = report.quality
    if report.fields:
        lines.extend(
            [
                f"fields={len(report.fields):,} | missing_fields={quality.fields_with_missing:,} "
                f"| null_fields={quality.fields_with_nulls:,} "
                f"| empty_fields={quality.fields_with_empty:,} "
                f"| mixed_type_fields={quality.mixed_type_fields:,}",
                "",
                "FIELDS",
            ]
        )
        for path, field in report.fields.items():
            details = [
                f"present={field.records_present:,}/{report.records_analyzed:,}",
                f"occurrences={field.occurrences:,}",
            ]
            if field.missing is not None:
                details.append(f"missing={field.missing:,}")
            if field.nulls is not None:
                details.append(f"nulls={field.nulls:,}")
            if field.empty is not None:
                details.append(f"empty={field.empty:,}")
            if field.cardinality is not None:
                suffix = ""
                if (
                    field.cardinality.precision == "approximate"
                    and field.cardinality.relative_error
                ):
                    suffix = f" (~±{field.cardinality.relative_error * 100:.2f}%)"
                details.append(
                    f"unique={field.cardinality.count:,} "
                    f"[{field.cardinality.precision}]{suffix}"
                )
            if field.types:
                details.append(
                    "types=" + ",".join(f"{key}:{value}" for key, value in field.types.items())
                )
            lines.append(f"- {path}: " + " | ".join(details))
    if report.warnings:
        lines.extend(["", "WARNINGS"])
        lines.extend(f"- {warning}" for warning in report.warnings)
    return "\n".join(lines).rstrip() + "\n"


def render_html(report: ProfileReport) -> str:
    """Render a standalone escaped HTML representation of the same report."""
    quality = report.quality
    rows: list[str] = []
    for path, field in report.fields.items():
        cardinality = "—"
        if field.cardinality is not None:
            cardinality = (
                f"{field.cardinality.count:,} ({escape(field.cardinality.precision)})"
            )
        types = (
            ", ".join(
                f"{escape(name)}: {count:,}" for name, count in (field.types or {}).items()
            )
            or "—"
        )
        missing = "—" if field.missing is None else f"{field.missing:,}"
        nulls = "—" if field.nulls is None else f"{field.nulls:,}"
        empty = "—" if field.empty is None else f"{field.empty:,}"
        rows.append(
            "<tr>"
            f"<td><code>{escape(path)}</code></td>"
            f"<td>{field.records_present:,}</td>"
            f"<td>{missing}</td>"
            f"<td>{nulls}</td>"
            f"<td>{empty}</td>"
            f"<td>{cardinality}</td>"
            f"<td>{types}</td>"
            "</tr>"
        )
    duplicate_html = ""
    if report.duplicates is not None:
        duplicate_html = (
            "<div class='profile-card'><strong>Duplicate rows</strong>"
            f"<span>{report.duplicates.duplicate_rows:,}</span></div>"
            "<div class='profile-card'><strong>Unique rows</strong>"
            f"<span>{report.duplicates.unique_rows:,}</span></div>"
        )
    warning_html = "".join(f"<li>{escape(warning)}</li>" for warning in report.warnings)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Data Profile v{escape(report.version)}</title>
<style>
body{{font-family:system-ui,sans-serif;margin:2rem;line-height:1.4;color:#171717}}h1{{margin-bottom:.25rem}}
.profile-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.75rem;margin:1rem 0}}
.profile-card{{border:1px solid #ddd;border-radius:8px;padding:1rem;display:flex;flex-direction:column;gap:.35rem}}
.profile-card span{{font-size:1.35rem;font-weight:700}}table{{border-collapse:collapse;width:100%;font-size:.9rem}}
th,td{{border-bottom:1px solid #ddd;padding:.55rem;text-align:left;vertical-align:top}}code{{white-space:nowrap}}
</style></head><body>
<h1>Data Profile v{escape(report.version)}</h1>
<p>scope={escape(report.scope)} · records={report.records_analyzed:,} · metrics={escape(','.join(report.requested_metrics))}</p>
<div class="profile-grid"><div class="profile-card"><strong>Records analyzed</strong><span>{report.records_analyzed:,}</span></div>
<div class="profile-card"><strong>Fields</strong><span>{len(report.fields):,}</span></div>{duplicate_html}
<div class="profile-card"><strong>Fields with missing</strong><span>{quality.fields_with_missing:,}</span></div>
<div class="profile-card"><strong>Fields with nulls</strong><span>{quality.fields_with_nulls:,}</span></div></div>
<table><thead><tr><th>Field</th><th>Present</th><th>Missing</th><th>Nulls</th><th>Empty</th><th>Unique</th><th>Types</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
{'<h2>Warnings</h2><ul>'+warning_html+'</ul>' if warning_html else ''}
</body></html>"""


__all__ = ["render_html", "render_text"]
