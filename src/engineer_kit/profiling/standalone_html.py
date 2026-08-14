"""Self-contained HTML presentation for :class:`ProfileReport`."""

from __future__ import annotations

from html import escape

from engineer_kit.profiling.model import ProfileReport


def _number(value: int | None) -> str:
    return "—" if value is None else f"{value:,}"


def render_html(report: ProfileReport, language: str = "en") -> str:
    """Render the report without external assets, raw values or host-page CSS."""
    if language not in {"en", "pt-BR"}:
        raise ValueError("language must be 'en' or 'pt-BR'")

    rows = []
    for path, field in report.fields.items():
        cardinality = "—"
        if field.cardinality:
            cardinality = f"{field.cardinality.count:,} ({escape(field.cardinality.precision)})"
        types = ", ".join(
            f"{escape(name)}: {count:,}" for name, count in (field.types or {}).items()
        ) or "—"
        issue = any((value or 0) > 0 for value in (field.missing, field.nulls, field.empty))
        issue = issue or len([name for name in (field.types or {}) if name != "null"]) > 1
        rows.append(
            f'<tr data-field="{escape(path, quote=True)}" data-issue="{str(issue).lower()}">'
            f"<td><code>{escape(path)}</code></td><td>{field.records_present:,}</td>"
            f"<td>{_number(field.missing)}</td><td>{_number(field.nulls)}</td>"
            f"<td>{_number(field.empty)}</td><td>{cardinality}</td><td>{types}</td></tr>"
        )

    cards = [
        ("records_analyzed", report.records_analyzed),
        ("fields", len(report.fields)),
    ]
    if report.duplicates:
        if report.duplicates.key_fields:
            cards.extend(
                [
                    ("candidate_pk", ", ".join(report.duplicates.key_fields)),
                    ("invalid_pk", report.duplicates.invalid_key_rows),
                    ("duplicate_rows", report.duplicates.duplicate_rows),
                    ("unique_valid_pk", report.duplicates.unique_rows),
                ]
            )
        else:
            cards.extend(
                [
                    ("duplicate_rows", report.duplicates.duplicate_rows),
                    ("unique_rows", report.duplicates.unique_rows),
                ]
            )
    quality = report.quality
    cards.extend(
        [
            ("fields_missing", quality.fields_with_missing),
            ("fields_nulls", quality.fields_with_nulls),
            ("fields_empty", quality.fields_with_empty),
            ("mixed_fields", quality.mixed_type_fields),
        ]
    )
    card_html = "".join(
        "<div class='profile-card'><strong data-i18n='{}'>{}</strong><span>{}</span></div>".format(
            key, key.replace("_", " ").title(), escape(str(_number(value) if isinstance(value, (int, type(None))) else value))
        )
        for key, value in cards
    )
    warnings = "".join(f"<li>{escape(warning)}</li>" for warning in report.warnings)
    warning_html = (
        f'<section class="warnings"><h2 data-i18n="warnings">Warnings</h2><ul>{warnings}</ul></section>'
        if warnings
        else ""
    )
    css = """
:root{--bg:#fbfbfd;--surface:#fff;--alt:#f5f6fc;--border:#e6e9f0;--text:#171a21;--soft:#3d4453;--muted:#727c8c;--indigo:#5b4ee6;--violet:#8a7bff;--amber:#f6a623;--shadow:0 6px 22px rgba(21,24,38,.08)}
[data-theme=dark]{--bg:#1e1e1e;--surface:#252526;--alt:#2d2d30;--border:#3c3c3c;--text:#d4d4d4;--soft:#b8b8b8;--muted:#858585;--shadow:none}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.55 Inter,"Segoe UI",system-ui,sans-serif}button,input{font:inherit}.shell{max-width:1240px;margin:auto;padding:28px}header{background:#151826;color:#fff;border-radius:16px;padding:28px 32px;display:flex;justify-content:space-between;gap:24px;align-items:center;box-shadow:var(--shadow)}.brand{display:flex;gap:14px;align-items:center}.mark{width:44px;height:44px;border-radius:12px;background:linear-gradient(140deg,var(--indigo),var(--violet));display:grid;place-items:center;font-weight:800;font-size:20px}h1{font-size:28px;margin:0;letter-spacing:-.7px}.subtitle{color:#b9bac4;margin:4px 0 0}.controls,.seg{display:flex;gap:4px;background:rgba(255,255,255,.1);padding:4px;border-radius:10px}.controls button,.seg button{border:0;border-radius:7px;padding:7px 11px;background:transparent;color:inherit;cursor:pointer;font-weight:600}.controls button.active,.seg button.active{background:var(--surface);color:var(--indigo);box-shadow:0 1px 5px rgba(0,0,0,.2)}.meta{font-family:ui-monospace,Consolas,monospace;color:var(--muted);margin:20px 2px}.profile-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:18px 0 28px}.profile-card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:20px;display:flex;flex-direction:column;gap:8px;box-shadow:var(--shadow)}.profile-card strong{color:var(--muted);font-size:12px;font-weight:600}.profile-card span{font-size:28px;font-weight:750;letter-spacing:-.8px}section{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;box-shadow:var(--shadow);margin-bottom:22px}.section-head{padding:18px 20px;border-bottom:1px solid var(--border);display:flex;gap:14px;align-items:center;justify-content:space-between;flex-wrap:wrap}h2{margin:0;font-size:16px}.filters{display:flex;gap:8px;align-items:center;flex-wrap:wrap}input[type=search]{min-width:220px;background:var(--alt);color:var(--text);border:1px solid var(--border);border-radius:9px;padding:8px 11px;outline:none}input:focus{border-color:var(--indigo)}.seg{background:var(--alt)}.seg button{color:var(--muted)}.table-wrap{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}th,td{border-bottom:1px solid var(--border);padding:12px 18px;text-align:left;vertical-align:top}th{color:var(--muted);font:600 10px ui-monospace,Consolas,monospace;text-transform:uppercase;letter-spacing:1px}tbody tr:hover{background:var(--alt)}code{white-space:nowrap;color:var(--indigo);background:var(--alt);padding:2px 6px;border-radius:5px}.warnings{padding:18px 22px;border-left:3px solid var(--amber)}.empty-state{display:none;padding:30px;text-align:center;color:var(--muted)}@media(max-width:700px){.shell{padding:12px}header{align-items:flex-start;flex-direction:column;padding:22px}.controls{width:100%;justify-content:space-between}.section-head{align-items:stretch;flex-direction:column}input[type=search]{width:100%}}
"""
    translations = """const T={en:{subtitle:'Aggregated quality report',light:'Light',dark:'Dark',scope:'scope',records:'records',metrics:'metrics',records_analyzed:'Records analyzed',fields:'Fields',candidate_pk:'Candidate PK',invalid_pk:'Invalid PK rows',duplicate_rows:'Duplicate rows',unique_rows:'Unique rows',unique_valid_pk:'Unique valid PK rows',fields_missing:'Fields with missing',fields_nulls:'Fields with nulls',fields_empty:'Fields with empty',mixed_fields:'Mixed-type fields',field_details:'Field details',search:'Search fields',all_fields:'All fields',issues:'Issues',field:'Field',present:'Present',missing:'Missing',nulls:'Nulls',empty:'Empty',unique:'Unique',types:'Types',no_fields:'No fields match the filters.',warnings:'Warnings'},'pt-BR':{subtitle:'Relatório agregado de qualidade',light:'Claro',dark:'Escuro',scope:'escopo',records:'registros',metrics:'métricas',records_analyzed:'Registros analisados',fields:'Campos',candidate_pk:'PK candidata',invalid_pk:'Registros com PK inválida',duplicate_rows:'Registros duplicados',unique_rows:'Registros únicos',unique_valid_pk:'PKs válidas únicas',fields_missing:'Campos ausentes',fields_nulls:'Campos com nulos',fields_empty:'Campos vazios',mixed_fields:'Campos com tipos mistos',field_details:'Detalhes dos campos',search:'Buscar campos',all_fields:'Todos os campos',issues:'Problemas',field:'Campo',present:'Presentes',missing:'Ausentes',nulls:'Nulos',empty:'Vazios',unique:'Únicos',types:'Tipos',no_fields:'Nenhum campo corresponde aos filtros.',warnings:'Alertas'}};"""
    script = translations + f"""let lang=localStorage.getItem('ek-language')||'{language}',filter='all';const q=document.getElementById('field-search');function translate(){{document.documentElement.lang=lang;document.querySelectorAll('[data-i18n]').forEach(e=>e.textContent=T[lang][e.dataset.i18n]);document.querySelectorAll('[data-i18n-placeholder]').forEach(e=>e.placeholder=T[lang][e.dataset.i18nPlaceholder]);document.querySelectorAll('[data-lang]').forEach(e=>e.classList.toggle('active',e.dataset.lang===lang))}}function applyFilters(){{let shown=0,term=q.value.toLocaleLowerCase();document.querySelectorAll('tbody tr').forEach(r=>{{let visible=r.dataset.field.toLocaleLowerCase().includes(term)&&(filter==='all'||r.dataset.issue==='true');r.hidden=!visible;if(visible)shown++}});document.querySelector('.empty-state').style.display=shown?'none':'block'}}document.querySelectorAll('[data-lang]').forEach(b=>b.onclick=()=>{{lang=b.dataset.lang;localStorage.setItem('ek-language',lang);translate()}});document.querySelectorAll('[data-theme-choice]').forEach(b=>b.onclick=()=>{{let theme=b.dataset.themeChoice;document.documentElement.dataset.theme=theme;localStorage.setItem('ek-theme',theme);document.querySelectorAll('[data-theme-choice]').forEach(x=>x.classList.toggle('active',x.dataset.themeChoice===theme))}});document.querySelectorAll('[data-filter]').forEach(b=>b.onclick=()=>{{filter=b.dataset.filter;document.querySelectorAll('[data-filter]').forEach(x=>x.classList.toggle('active',x===b));applyFilters()}});q.oninput=applyFilters;let theme=localStorage.getItem('ek-theme')||'light';document.documentElement.dataset.theme=theme;document.querySelectorAll('[data-theme-choice]').forEach(x=>x.classList.toggle('active',x.dataset.themeChoice===theme));translate();applyFilters();"""
    return f"""<!doctype html><html lang="{language}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Data Profile v{escape(report.version)}</title><style>{css}</style></head><body><main class="shell"><header><div class="brand"><div class="mark">EK</div><div><h1>Data Profile <span>v{escape(report.version)}</span></h1><p class="subtitle" data-i18n="subtitle">Aggregated quality report</p></div></div><div class="controls"><button type="button" data-lang="pt-BR">PT-BR</button><button type="button" data-lang="en">EN</button><button type="button" data-theme-choice="light" data-i18n="light">Light</button><button type="button" data-theme-choice="dark" data-i18n="dark">Dark</button></div></header><p class="meta"><span data-i18n="scope">scope</span>={escape(report.scope)} · <span data-i18n="records">records</span>={report.records_analyzed:,} · <span data-i18n="metrics">metrics</span>={escape(','.join(report.requested_metrics))}</p><div class="profile-grid">{card_html}</div><section><div class="section-head"><h2 data-i18n="field_details">Field details</h2><div class="filters"><input id="field-search" type="search" data-i18n-placeholder="search" placeholder="Search fields"><div class="seg"><button class="active" type="button" data-filter="all" data-i18n="all_fields">All fields</button><button type="button" data-filter="issues" data-i18n="issues">Issues</button></div></div></div><div class="table-wrap"><table><thead><tr><th data-i18n="field">Field</th><th data-i18n="present">Present</th><th data-i18n="missing">Missing</th><th data-i18n="nulls">Nulls</th><th data-i18n="empty">Empty</th><th data-i18n="unique">Unique</th><th data-i18n="types">Types</th></tr></thead><tbody>{''.join(rows)}</tbody></table><div class="empty-state" data-i18n="no_fields">No fields match the filters.</div></div></section>{warning_html}</main><script>(function(){{'use strict';{script}}})();</script></body></html>"""


__all__ = ["render_html"]
