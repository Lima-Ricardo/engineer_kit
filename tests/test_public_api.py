"""Garante que a ergonomia de import 'tipo pandas' continua funcionando:
`from engineer_kit import RestConnector` sem precisar saber em qual
submodulo cada classe mora.
"""

import engineer_kit


def test_top_level_import_exposes_main_classes():
    from engineer_kit import (
        STANDARD_PAGINATION_TYPES,
        APIConnector,
        ColumnSpec,
        Connector,
        CursorPagination,
        DEFAULT_EXTRACTION_BATCH_SIZE,
        DuckDBLoader,
        EndpointSchema,
        ExtractionSession,
        IncrementalStrategy,
        LinkHeaderPagination,
        NextUrlPagination,
        PageNumberPagination,
        Pipeline,
        PipelineSource,
        RestConnector,
        Scheduler,
    )

    exports = (
        APIConnector,
        ColumnSpec,
        Connector,
        CursorPagination,
        DuckDBLoader,
        EndpointSchema,
        ExtractionSession,
        IncrementalStrategy,
        LinkHeaderPagination,
        NextUrlPagination,
        PageNumberPagination,
        Pipeline,
        PipelineSource,
        RestConnector,
        Scheduler,
        STANDARD_PAGINATION_TYPES,
    )
    assert all(export is not None for export in exports)
    assert issubclass(RestConnector, APIConnector)
    assert issubclass(APIConnector, Connector)
    assert DEFAULT_EXTRACTION_BATCH_SIZE == 25_000


def test_version_is_exposed():
    assert isinstance(engineer_kit.__version__, str)
    assert engineer_kit.__version__
