import json
import re
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    TypeVar,
)

from mypy_boto3_athena.type_defs import DataCatalogTypeDef
from pydantic import BaseModel, Field, field_validator

from dbt.adapters.athena.constants import LOGGER, SPARK_CATALOG_TYPES


def clean_sql_comment(comment: str) -> str:
    split_and_strip = [line.strip() for line in comment.split("\n")]
    return " ".join(line for line in split_and_strip if line)


def stringify_table_parameter_value(value: Any) -> Optional[str]:
    """Convert any variable to string for Glue Table property."""
    try:
        if isinstance(value, (dict, list)):
            value_str: str = json.dumps(value)
        else:
            value_str = str(value)
        return value_str[:512000]
    except (TypeError, ValueError) as e:
        # Handle non-stringifiable objects and non-serializable objects
        LOGGER.warning(f"Non-stringifiable object. Error: {str(e)}")
        return None


def is_valid_table_parameter_key(key: str) -> bool:
    """Check if key is valid for Glue Table property according to official documentation."""
    # Simplified version of key pattern which works with re
    # Original pattern can be found here https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html
    key_pattern: str = r"^[\u0020-\uD7FF\uE000-\uFFFD\t]*$"
    return len(key) <= 255 and bool(re.match(key_pattern, key))


def get_catalog_id(catalog: Optional[DataCatalogTypeDef]) -> Optional[str]:
    return (
        catalog["Parameters"]["catalog-id"]
        if catalog and catalog["Type"] == AthenaCatalogType.GLUE.value
        else None
    )


class AthenaCatalogType(Enum):
    GLUE = "GLUE"
    LAMBDA = "LAMBDA"
    HIVE = "HIVE"


def get_catalog_type(catalog: Optional[DataCatalogTypeDef]) -> Optional[AthenaCatalogType]:
    return AthenaCatalogType(catalog["Type"]) if catalog else None


RESERVED_SPARK_CATALOG_NAMES = ("awsdatacatalog", "spark_catalog")
SPARK_CATALOG_OVERRIDE_KEYS = frozenset({"type", "glue_id", "region"})


class CatalogOverrideSpec(BaseModel):
    name: str = Field(pattern=r"^[\w][\w-]*[\w]$|^[\w]$")
    type: Literal["iceberg"]
    glue_id: Optional[str] = Field(None, pattern=r"^\d{12}$")
    region: Optional[str] = Field(None, pattern=r"^[a-z0-9-]+$")

    @field_validator("name")
    @classmethod
    def validate_not_reserved(cls, v: str) -> str:
        if v.lower() in RESERVED_SPARK_CATALOG_NAMES:
            raise ValueError(f"Cannot override reserved catalog: {v!r}")
        return v

    @classmethod
    def from_user_dict(cls, name: str, spec: dict) -> "CatalogOverrideSpec":
        if not isinstance(spec, dict):
            raise ValueError(f"spark_catalog_overrides[{name!r}] must be a dict")

        unknown = set(spec) - SPARK_CATALOG_OVERRIDE_KEYS
        if unknown:
            raise ValueError(
                f"Unknown key(s) in spark_catalog_overrides[{name!r}]: {sorted(unknown)}. "
                f"Allowed: {sorted(SPARK_CATALOG_OVERRIDE_KEYS)}"
            )

        try:
            return cls(name=name, **spec)
        except Exception as e:
            raise ValueError(f"spark_catalog_overrides[{name!r}]: {e}") from e

    def resolve_glue_id(self, catalog_ids: Mapping[str, str]) -> str:
        if self.glue_id:
            return self.glue_id

        glue_id = catalog_ids.get(self.name)
        if not glue_id:
            raise ValueError(
                f"spark_catalog_overrides[{self.name!r}]: set glue_id, or register {self.name!r} "
                "as a GLUE catalog in Athena"
            )
        return glue_id

    def to_spark_properties(self, glue_id: str) -> Dict[str, str]:
        templates = SPARK_CATALOG_TYPES.get(self.type)
        if not templates:
            raise ValueError(f"Unsupported catalog type: {self.type}")
        prefix = f"spark.sql.catalog.{self.name}"

        properties = {key.format(prefix=prefix): value for key, value in templates.items()}
        properties[f"{prefix}.glue.id"] = glue_id
        properties[f"{prefix}.glue.account-id"] = glue_id

        if self.region:
            properties[f"{prefix}.client.region"] = self.region

        return properties


def resolve_spark_catalog_overrides(
    overrides: Any, lookup_catalog_ids: Callable[[], Mapping[str, str]]
) -> Dict[str, Dict[str, str]]:
    if overrides is None:
        return {}
    if not isinstance(overrides, dict):
        raise ValueError(
            f"spark_catalog_overrides must be a dict. Got: {type(overrides).__name__}"
        )
    if not overrides:
        return {}

    specs = [CatalogOverrideSpec.from_user_dict(name, spec) for name, spec in overrides.items()]

    catalog_ids = None
    if any(spec.glue_id is None for spec in specs):
        catalog_ids = lookup_catalog_ids()

    return {
        spec.name: spec.to_spark_properties(
            spec.resolve_glue_id(catalog_ids if catalog_ids is not None else {})
        )
        for spec in specs
    }


T = TypeVar("T")


def get_chunks(lst: List[T], n: int) -> Generator[List[T], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def chunk_iterable(iterable: Iterable[T], n: int) -> Generator[List[T], None, None]:
    """Yield successive n-sized chunks from any iterable, including generators."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def ellipsis_comment(s: str, max_len: int = 255) -> str:
    """Ellipsis string if it exceeds max length"""
    return f"{s[:(max_len - 3)]}..." if len(s) > max_len else s
