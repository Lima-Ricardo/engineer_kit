"""Log visual para o terminal, pensado para leitura humana durante uma
execucao (inicio, fim, blocos gravados, avisos) — separado do logging
tecnico (`logging` padrao) usado no resto da biblioteca, para nao
interferir nos testes que auditam esse logging tecnico (ex.: os testes
que garantem que nenhum segredo vaza em log).

Usa o `loguru` no modo simples: sem sink customizado, so ajustando o
nivel para DEBUG para garantir que toda a narrativa apareca.
"""

from __future__ import annotations

import sys

from loguru import logger as visual_logger

visual_logger.remove()
visual_logger.add(sys.stderr, level="DEBUG")

__all__ = ["visual_logger"]
