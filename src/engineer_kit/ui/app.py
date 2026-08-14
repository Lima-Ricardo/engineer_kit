"""Interface web local para aprender, configurar e observar pipelines.

A UI e propositalmente um extra opcional e orientado ao runtime local.
Ela usa DuckDB para permitir uma experiencia zero-infra, mas apresenta
os mesmos conceitos do core: Connector, StateStore, Destination,
RunLogBackend e transformacao opcional (dbt).
"""

from __future__ import annotations

import glob
import secrets
from pathlib import Path

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool

from engineer_kit.config.pipeline_config import (
    AuthConfig,
    ColumnConfig,
    ConnectorConfig,
    DateParamsConfig,
    DestinationConfig,
    IncrementalConfig,
    PaginationConfig,
    PipelineConfig,
    PipelineConfigError,
    TransformConfig,
    list_pipeline_configs,
    load_pipeline_config,
    save_pipeline_config,
)
from engineer_kit.connectors.api_connector import DEFAULT_MAX_PAGES
from engineer_kit.connectors.extraction import DEFAULT_EXTRACTION_BATCH_SIZE
from engineer_kit.connectors.pagination import STANDARD_PAGINATION_TYPES
from engineer_kit.profiling.config import connector_from_config
from engineer_kit.profiling.engine import PROFILE_METRICS
from engineer_kit.security.redaction import redact_text
from engineer_kit.ui.run_manager import RunManager
from engineer_kit.ui.security import (
    SecurityHeadersMiddleware,
    enforce_same_origin,
    validate_resource_name,
)

BASE_DIR = Path(__file__).parent
PREVIEW_ROW_LIMIT = 200
PROFILE_SAMPLE_LIMIT = 10_000
PROFILE_MAX_SAMPLE_LIMIT = 1_000_000


def _workspace_child(workspace: Path, value: str) -> Path:
    candidate = (workspace / value).resolve()
    try:
        candidate.relative_to(workspace)
    except ValueError as exc:
        raise ValueError(f"Caminho da UI deve permanecer dentro do workspace: {value!r}") from exc
    return candidate


def create_app(
    workspace_dir: str = ".",
    pipelines_dirname: str = "pipelines",
    warehouse_filename: str = "warehouse.duckdb",
    dbt_project_dirname: str = "dbt_project",
    username: str = "admin",
    password: str | None = None,
) -> FastAPI:
    if not password:
        raise ValueError(
            "create_app() exige password explicita. Para uso local pela CLI, execute "
            "`engineer_kit ui`; ela gera uma senha temporaria quando nenhuma e informada."
        )
    if not username:
        raise ValueError("create_app() exige username nao vazio.")

    workspace = Path(workspace_dir).resolve()
    pipelines_dir = _workspace_child(workspace, pipelines_dirname)
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    warehouse_path = str(_workspace_child(workspace, warehouse_filename))
    dbt_project_dir = _workspace_child(workspace, dbt_project_dirname)

    run_manager = RunManager(
        warehouse_path=warehouse_path,
        dbt_project_dir=str(dbt_project_dir),
    )
    security = HTTPBasic()

    def check_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
        valid_user = secrets.compare_digest(credentials.username, username)
        valid_pass = secrets.compare_digest(credentials.password, password)
        if not (valid_user and valid_pass):
            raise HTTPException(
                status_code=401,
                detail="Credenciais invalidas",
                headers={"WWW-Authenticate": "Basic"},
            )

    app = FastAPI(title="engineer_kit local lab")
    app.add_middleware(SecurityHeadersMiddleware)
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    def _warehouse_conn(read_only: bool = True) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(warehouse_path, read_only=read_only)

    def _last_run_by_pipeline() -> dict[str, dict]:
        try:
            conn = _warehouse_conn()
        except duckdb.IOException:
            return {}
        try:
            rows = conn.execute(
                "SELECT connector_name, status, started_at, finished_at, rows_loaded "
                "FROM _meta.run_log QUALIFY row_number() OVER "
                "(PARTITION BY connector_name ORDER BY finished_at DESC) = 1"
            ).fetchall()
        except duckdb.CatalogException:
            rows = []
        finally:
            conn.close()
        return {
            row[0]: {
                "status": row[1],
                "started_at": row[2],
                "finished_at": row[3],
                "rows_loaded": row[4],
            }
            for row in rows
        }

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request, _: None = Depends(check_auth)):
        configs = list_pipeline_configs(pipelines_dir)
        last_runs = _last_run_by_pipeline()
        pipelines = [
            {"config": config, "last_run": last_runs.get(config.name)}
            for _, config in configs
        ]
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "pipelines": pipelines,
                "dbt_available": dbt_project_dir.exists(),
                "runtime": "DuckDB local",
            },
        )

    @app.get("/architecture", response_class=HTMLResponse)
    def architecture(request: Request, _: None = Depends(check_auth)):
        return templates.TemplateResponse(
            request,
            "architecture.html",
            {"dbt_available": dbt_project_dir.exists()},
        )

    @app.get("/pipelines/new", response_class=HTMLResponse)
    def new_pipeline_form(request: Request, _: None = Depends(check_auth)):
        return templates.TemplateResponse(
            request,
            "pipeline_form.html",
            {
                "config": None,
                "pagination_types": list(STANDARD_PAGINATION_TYPES),
                "error": None,
                "dbt_available": dbt_project_dir.exists(),
            },
        )

    @app.get("/pipelines/{name}/edit", response_class=HTMLResponse)
    def edit_pipeline_form(request: Request, name: str, _: None = Depends(check_auth)):
        config = _load_or_404(name)
        return templates.TemplateResponse(
            request,
            "pipeline_form.html",
            {
                "config": config,
                "pagination_types": list(STANDARD_PAGINATION_TYPES),
                "error": None,
                "dbt_available": dbt_project_dir.exists(),
            },
        )

    @app.post("/pipelines/save")
    async def save_pipeline(request: Request, _: None = Depends(check_auth)):
        enforce_same_origin(request)
        form = await request.form()
        try:
            config = _config_from_form(form)
            save_pipeline_config(config, pipelines_dir / f"{config.name}.yaml")
        except (PipelineConfigError, ValueError) as exc:
            return templates.TemplateResponse(
                request,
                "pipeline_form.html",
                {
                    "config": None,
                    "pagination_types": list(STANDARD_PAGINATION_TYPES),
                    "error": str(exc),
                    "dbt_available": dbt_project_dir.exists(),
                },
                status_code=400,
            )
        return RedirectResponse(url=f"/pipelines/{config.name}", status_code=303)

    @app.get("/pipelines/{name}", response_class=HTMLResponse)
    def pipeline_detail(request: Request, name: str, _: None = Depends(check_auth)):
        config = _load_or_404(name)
        history = _run_history(name)
        return templates.TemplateResponse(
            request,
            "pipeline_detail.html",
            {
                "config": config,
                "history": history,
                "dbt_available": dbt_project_dir.exists(),
            },
        )

    def _profile_context(
        config: PipelineConfig,
        *,
        report=None,
        error: str | None = None,
        selected_metrics: list[str] | tuple[str, ...] = (),
        scope: str = "sample",
        limit: int = PROFILE_SAMPLE_LIMIT,
    ) -> dict:
        return {
            "config": config,
            "metrics": sorted(PROFILE_METRICS),
            "selected_metrics": tuple(selected_metrics),
            "scope": scope,
            "limit": limit,
            "report": report,
            "error": error,
        }

    @app.get("/pipelines/{name}/profile", response_class=HTMLResponse)
    def profile_form(request: Request, name: str, _: None = Depends(check_auth)):
        config = _load_or_404(name)
        return templates.TemplateResponse(
            request,
            "profile_report.html",
            _profile_context(config),
        )

    @app.post("/pipelines/{name}/profile", response_class=HTMLResponse)
    async def profile_run(request: Request, name: str, _: None = Depends(check_auth)):
        enforce_same_origin(request)
        config = _load_or_404(name)
        form = await request.form()
        selected = tuple(form.getlist("metric")) if hasattr(form, "getlist") else ()
        scope = str(form.get("scope") or "sample").strip().lower()
        if scope not in {"sample", "full"}:
            return templates.TemplateResponse(
                request,
                "profile_report.html",
                _profile_context(config, error="Scope deve ser sample ou full."),
                status_code=400,
            )
        try:
            requested_limit = int(form.get("limit") or PROFILE_SAMPLE_LIMIT)
            if requested_limit <= 0 or requested_limit > PROFILE_MAX_SAMPLE_LIMIT:
                raise ValueError
        except (TypeError, ValueError):
            return templates.TemplateResponse(
                request,
                "profile_report.html",
                _profile_context(
                    config,
                    error=(
                        f"Limite do sample deve estar entre 1 e "
                        f"{PROFILE_MAX_SAMPLE_LIMIT:,}."
                    ),
                    selected_metrics=selected,
                    scope=scope,
                ),
                status_code=400,
            )

        resolved_limit = requested_limit if scope == "sample" else None
        try:
            connector = connector_from_config(config)
            report = await run_in_threadpool(
                connector.profile,
                *selected,
                scope=scope,
                limit=resolved_limit,
            )
        except Exception as exc:
            return templates.TemplateResponse(
                request,
                "profile_report.html",
                _profile_context(
                    config,
                    error=redact_text(exc),
                    selected_metrics=selected,
                    scope=scope,
                    limit=requested_limit,
                ),
                status_code=502,
            )

        return templates.TemplateResponse(
            request,
            "profile_report.html",
            _profile_context(
                config,
                report=report,
                selected_metrics=selected,
                scope=scope,
                limit=requested_limit,
            ),
        )

    @app.post("/pipelines/{name}/run")
    def trigger_run(request: Request, name: str, _: None = Depends(check_auth)):
        enforce_same_origin(request)
        config = _load_or_404(name)
        run_id = run_manager.start_run(config)
        return RedirectResponse(url=f"/runs/{run_id}", status_code=303)

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    def run_view(request: Request, run_id: str, _: None = Depends(check_auth)):
        state = run_manager.get_state(run_id)
        if state is None:
            raise HTTPException(status_code=404, detail="Execucao nao encontrada.")
        return templates.TemplateResponse(request, "run_view.html", {"state": state})

    @app.get("/runs/{run_id}/stream")
    def run_stream(run_id: str, _: None = Depends(check_auth)):
        state = run_manager.get_state(run_id)
        if state is None:
            raise HTTPException(status_code=404, detail="Execucao nao encontrada.")

        def event_source():
            for line in run_manager.stream_log(run_id):
                # One SSE data line per logical log line. Newlines are split so
                # log content cannot inject a new event field/type.
                for part in str(line).replace("\r", "").split("\n"):
                    yield f"data: {part}\n"
                yield "\n"
            final_state = run_manager.get_state(run_id)
            status = final_state.status if final_state is not None else "unknown"
            yield f"event: done\ndata: {status}\n\n"

        return StreamingResponse(event_source(), media_type="text/event-stream")

    @app.get("/data", response_class=HTMLResponse)
    def data_browser(request: Request, _: None = Depends(check_auth)):
        try:
            conn = _warehouse_conn()
        except duckdb.IOException:
            return templates.TemplateResponse(request, "data_browser.html", {"tables": []})
        try:
            tables = conn.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY 1, 2"
            ).fetchall()
            rows = []
            for schema, table in tables:
                quoted_schema = _quote_duckdb_identifier(schema)
                quoted_table = _quote_duckdb_identifier(table)
                count = conn.execute(
                    f"SELECT count(*) FROM {quoted_schema}.{quoted_table}"  # nosec B608
                ).fetchone()[0]
                rows.append({"schema": schema, "table": table, "rows": count})
        finally:
            conn.close()
        return templates.TemplateResponse(request, "data_browser.html", {"tables": rows})

    @app.get("/data/{schema}/{table}", response_class=HTMLResponse)
    def table_preview(
        request: Request,
        schema: str,
        table: str,
        _: None = Depends(check_auth),
    ):
        _validate_identifier(schema)
        _validate_identifier(table)
        quoted_schema = _quote_duckdb_identifier(schema)
        quoted_table = _quote_duckdb_identifier(table)
        conn = _warehouse_conn()
        try:
            result = conn.execute(
                f"SELECT * FROM {quoted_schema}.{quoted_table} "  # nosec B608
                f"LIMIT {PREVIEW_ROW_LIMIT}"
            )
            columns = [description[0] for description in result.description]
            rows = result.fetchall()
        finally:
            conn.close()
        return templates.TemplateResponse(
            request,
            "table_preview.html",
            {
                "schema": schema,
                "table": table,
                "columns": columns,
                "rows": rows,
                "limit": PREVIEW_ROW_LIMIT,
            },
        )

    @app.get("/dbt", response_class=HTMLResponse)
    def dbt_models(request: Request, _: None = Depends(check_auth)):
        layers = {"staging": [], "silver": [], "gold": []}
        for layer in layers:
            pattern = str(dbt_project_dir / "models" / layer / "*.sql")
            layers[layer] = sorted(Path(path).stem for path in glob.glob(pattern))
        return templates.TemplateResponse(
            request,
            "dbt_models.html",
            {
                "layers": layers,
                "dbt_available": dbt_project_dir.exists(),
                "project_dir": dbt_project_dir,
            },
        )

    def _load_or_404(name: str) -> PipelineConfig:
        try:
            safe_name = validate_resource_name(name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        path = pipelines_dir / f"{safe_name}.yaml"
        if not path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline '{safe_name}' nao encontrado.",
            )
        try:
            return load_pipeline_config(path)
        except PipelineConfigError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Configuracao invalida: {exc}",
            ) from exc

    def _run_history(name: str) -> list[dict]:
        try:
            conn = _warehouse_conn()
        except duckdb.IOException:
            return []
        try:
            rows = conn.execute(
                "SELECT started_at, finished_at, status, rows_loaded, error_message "
                "FROM _meta.run_log WHERE connector_name = ? "
                "ORDER BY finished_at DESC LIMIT 20",
                [name],
            ).fetchall()
        except duckdb.CatalogException:
            rows = []
        finally:
            conn.close()
        return [
            {
                "started_at": row[0],
                "finished_at": row[1],
                "status": row[2],
                "rows_loaded": row[3],
                "error_message": row[4],
            }
            for row in rows
        ]

    def _config_from_form(form) -> PipelineConfig:
        name = (form.get("name") or "").strip()
        if not name:
            raise ValueError("O nome do pipeline e obrigatorio.")
        validate_resource_name(name)
        base_url = (form.get("base_url") or "").strip()
        if not base_url:
            raise ValueError("A URL base e obrigatoria.")

        pagination_type = form.get("pagination_type", "none")
        pagination_params = {}
        if pagination_type == "page":
            pagination_params = {
                "page_param": form.get("page_param") or "page",
                "page_size_param": form.get("page_size_param") or "per_page",
                "page_size": int(form.get("page_size") or 100),
            }
        elif pagination_type == "offset":
            pagination_params = {
                "offset_param": form.get("offset_param") or "offset",
                "limit_param": form.get("limit_param") or "limit",
                "limit": int(form.get("limit") or 100),
            }
        elif pagination_type == "cursor":
            pagination_params = {
                "cursor_param": form.get("cursor_param") or "cursor",
                "cursor_field": form.get("cursor_field") or "next_cursor",
            }
        elif pagination_type == "link_header":
            pagination_params = {"header_name": form.get("header_name") or "Link"}
        elif pagination_type == "next_url":
            pagination_params = {
                "next_url_field": form.get("next_url_field") or "next"
            }

        columns = []
        col_names = form.getlist("column_name") if hasattr(form, "getlist") else []
        col_dtypes = form.getlist("column_dtype") if hasattr(form, "getlist") else []
        for col_name, col_dtype in zip(col_names, col_dtypes):
            if col_name.strip():
                columns.append(
                    ColumnConfig(
                        name=col_name.strip(),
                        dtype=col_dtype.strip() or "string",
                    )
                )

        connector = ConnectorConfig(
            base_url=base_url,
            method=form.get("method", "GET"),
            auth=AuthConfig(
                type=form.get("auth_type", "none"),
                secret_key=form.get("auth_secret_key") or None,
                param_name=form.get("auth_param_name") or "api_key",
                location=form.get("auth_location") or "query",
            ),
            pagination=PaginationConfig(
                type=pagination_type,
                params=pagination_params,
            ),
            incremental=IncrementalConfig(
                mode=form.get("incremental_mode", "data_date"),
                initial_start=form.get("initial_start") or None,
                date_field=form.get("date_field") or None,
            ),
            date_params=DateParamsConfig(
                start=form.get("date_param_start") or None,
                end=form.get("date_param_end") or None,
                format=form.get("date_param_format") or "%Y-%m-%d",
            ),
            records_path=form.get("records_path") or None,
            dedup=(form.get("dedup") or "off") == "on",
            extraction_batch_size=int(
                form.get("extraction_batch_size") or DEFAULT_EXTRACTION_BATCH_SIZE
            ),
            max_pages=int(form.get("max_pages") or DEFAULT_MAX_PAGES),
        )
        destination = DestinationConfig(
            type=form.get("destination_type") or "duckdb",
            schema=form.get("destination_schema") or "bronze",
            batch_size=int(form.get("batch_size") or 1000),
            write_mode=form.get("write_mode") or "append",
        )
        transform = TransformConfig(
            type=form.get("transform_type") or "none",
            select=form.get("dbt_select") or None,
        )
        return PipelineConfig(
            name=name,
            connector=connector,
            columns=columns,
            destination=destination,
            transform=transform,
            run_log=(form.get("run_log") or "on") == "on",
        )

    return app


def _validate_identifier(name: str) -> None:
    import re

    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise HTTPException(status_code=400, detail=f"Nome invalido: '{name}'.")


def _quote_duckdb_identifier(name: str) -> str:
    """Quote an identifier using SQL-standard doubled double-quotes."""
    return '"' + str(name).replace('"', '""') + '"'
