import json
import re
from enum import Enum
from typing import Any, Callable, Dict, Generator, List, Mapping, Optional, TypeVar

from mypy_boto3_athena.type_defs import DataCatalogTypeDef

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


def resolve_spark_catalog_overrides(
    overrides: Any, lookup_catalog_ids: Callable[[], Mapping[str, str]]
) -> Dict[str, Dict[str, str]]:
    """Turn ``spark_catalog_overrides`` into Spark properties per catalog name.

    A catalog without ``glue_id`` takes its ID from ``lookup_catalog_ids()``
    (Athena data catalog name -> catalog ID), which is only called when needed.
    """
    if overrides is None:
        return {}
    if not isinstance(overrides, dict):
        raise ValueError(f"spark_catalog_overrides must be a mapping. Got: {overrides!r}")

    resolved: Dict[str, Dict[str, str]] = {}
    catalog_ids: Optional[Mapping[str, str]] = None
    for name, spec in overrides.items():
        if not isinstance(name, str) or not re.fullmatch(r"[\w-]+", name):
            raise ValueError(f"Invalid catalog name in spark_catalog_overrides: {name!r}")
        if name.lower() in RESERVED_SPARK_CATALOG_NAMES:
            raise ValueError(f"spark_catalog_overrides cannot override {name!r}")
        if not isinstance(spec, dict):
            raise ValueError(f"spark_catalog_overrides[{name!r}] must be a mapping. Got: {spec!r}")
        unknown_keys = sorted(set(spec) - SPARK_CATALOG_OVERRIDE_KEYS)
        if unknown_keys:
            raise ValueError(
                f"Unknown key(s) in spark_catalog_overrides[{name!r}]: {unknown_keys}. "
                f"Allowed: {sorted(SPARK_CATALOG_OVERRIDE_KEYS)}"
            )
        catalog_type = spec.get("type")
        if not isinstance(catalog_type, str) or catalog_type not in SPARK_CATALOG_TYPES:
            raise ValueError(
                f"spark_catalog_overrides[{name!r}].type must be one of "
                f"{sorted(SPARK_CATALOG_TYPES)}. Got: {catalog_type!r}"
            )
        templates = SPARK_CATALOG_TYPES[catalog_type]
        glue_id = spec.get("glue_id")
        if glue_id is None:
            if catalog_ids is None:
                catalog_ids = lookup_catalog_ids()
            glue_id = catalog_ids.get(name)
        elif not isinstance(glue_id, str) or not re.fullmatch(r"\d{12}", glue_id):
            raise ValueError(
                f"spark_catalog_overrides[{name!r}].glue_id must be a 12-digit string "
                f"(quote it in YAML). Got: {glue_id!r}"
            )
        if not glue_id:
            raise ValueError(
                f"spark_catalog_overrides[{name!r}]: set glue_id, or register {name!r} "
                "as a GLUE data catalog in Athena"
            )

        prefix = f"spark.sql.catalog.{name}"
        properties = {key.format(prefix=prefix): value for key, value in templates.items()}
        properties[f"{prefix}.glue.id"] = str(glue_id)
        properties[f"{prefix}.glue.account-id"] = str(glue_id)
        if spec.get("region"):
            properties[f"{prefix}.client.region"] = str(spec["region"])
        resolved[name] = properties
    return resolved


T = TypeVar("T")


def get_chunks(lst: List[T], n: int) -> Generator[List[T], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def ellipsis_comment(s: str, max_len: int = 255) -> str:
    """Ellipsis string if it exceeds max length"""
    return f"{s[:(max_len - 3)]}..." if len(s) > max_len else s
