"""Schema declarado explicitamente pelo engenheiro para cada endpoint.

Substitui a inferencia dinamica de colunas: o loader nunca decide por
conta propria o que existe na tabela. Ele so aplica o que foi
declarado aqui. Qualquer campo que a API mandar fora dessa lista cai
em `_extra` — nunca quebra o pipeline; retipar corretamente e uma
decisao explicita de quem escreve o schema, nao algo automatico.

Todas as colunas comecam como VARCHAR por padrao (coerente com a
decisao de que tudo que sai de um conector e string). Passe `dtype`
so quando quiser tipar de verdade.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from engineer_kit.storage.identifiers import validate_identifier, validate_type


@dataclass
class ColumnSpec:
    name: str
    dtype: str = "VARCHAR"

    def __post_init__(self) -> None:
        validate_identifier(self.name, "Nome de coluna")
        validate_type(self.dtype)


@dataclass
class EndpointSchema:
    columns: list[ColumnSpec] = field(default_factory=list)

    @classmethod
    def from_names(cls, names: list[str]) -> "EndpointSchema":
        """Atalho: declarar so os nomes das colunas esperadas, todas VARCHAR."""
        return cls(columns=[ColumnSpec(name=n) for n in names])

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]
