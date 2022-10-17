from dataclasses import dataclass
from typing import (
    Any,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from beanie.odm.enums import SortDirection as BeanieSortDirection
from beanie.odm.queries.find import FindMany
from pydantic import BaseModel, create_model

from beaniekeyset.cursor import unserialize_bookmark
from beaniekeyset.serializer import InvalidPageError

FindQueryResultType = TypeVar("FindQueryResultType", bound=BaseModel)
T = TypeVar("T")

PaginationMode = Union[Literal["forward"], Literal["backwards"]]
SortDirection = Union[Literal[1], Literal[-1]]


class BeaniePage(Generic[T]):
    def __init__(
        self,
        documents: List[T],
        ordering_fields: List[dict],
    ) -> None:
        super().__init__()


@dataclass
class TransformedFields:
    find_expressions: List[Mapping]
    sort_expressions: List[Tuple[str, SortDirection]]
    projection_map: dict


def __convert_prjection_model_to_dict(projection_model: Type[BaseModel]) -> dict:
    schema = projection_model.schema()
    fields = list(schema.get("properties", {}).keys())
    return {field: 1 for field in fields}


def __get_model_with_added_fields(
    projection_model: Type[BaseModel], final_fields: dict
) -> Type[BaseModel]:
    existing_field_names = list(projection_model.schema()["properties"].keys())
    final_field_names = list(final_fields.keys())
    fields_to_add = set(final_field_names) - set(existing_field_names)
    _fields = {field: (Optional[Any], ...) for field in fields_to_add}
    return create_model(
        "BeanieKeysetDynamicProjectionModel", __base__=projection_model, **_fields
    )


def __build_pagination_query(
    cursor_values: List[Any], sort_expressions: List[Tuple[str, SortDirection]]
) -> dict:
    value = cursor_values[0]
    field, direction = sort_expressions[0]
    operator = "$lt" if direction == -1 else "$gt"
    if len(cursor_values) == 1:
        return {field: {operator: value}}
    pagination_query = {"$or": []}
    pagination_query["$or"].append({field: {operator: value}})
    pagination_query["$or"].append(
        {
            "$and": [
                {field: value},
                __build_pagination_query(cursor_values[1:], sort_expressions[1:]),
            ]
        }
    )
    return pagination_query


def construct_pagination_query(
    cursor_values: List[Any],
    sort_expressions: List[Tuple[str, SortDirection]],
    mode: PaginationMode,
) -> Tuple[List[Tuple[str, SortDirection]], dict]:
    # cursor based pagination works by comparing cursor values with ordering fields.
    # For a simple scenario, let us say that the ordering fields are (a1, a2, a3) and
    # the cursor values are (b1, b2, b3). We want elements after this cursor, so the condition
    # is going to be (a1, a2, a3) > (b1, b2, b3). Now, mongodb doesn't have tuple
    # comparison, but it does have $or operator! So the above comparison can be
    # expanded into  a1 > b1 OR (a1 == b1 AND a2 > b2 OR (a2 == b2 and a3 > b3)).
    # We reverse the direction if we want elements before this cursor.
    # There's other nuances, like ordering fields direction.
    # In forward pagination (for $gt), we keep the directions as is.
    # In backward pagination (for $lt), we reverse the direction of all fields.
    # More over, if the field we are sorting on is ASC (after reversing directions if needed),
    # The comparison will be field > value. If the direction is DESC, the comparison will be
    # value > field. So if b1 is ASC, b2 is DESC, b3 is ASC in forward,
    # the tuple comparison will be (a1, b2, a3) > (b1, a2, b3), and sort fields will not be reversed.
    # in backward, the tuple comparison will be (b1, a2, b3) > (a1, b2, a3).

    multiplier = 1 if mode == "forward" else -1
    final_sort_expressions = [
        (field, multiplier * direction) for field, direction in sort_expressions
    ]
    pagination_query = __build_pagination_query(cursor_values, final_sort_expressions)
    return final_sort_expressions, pagination_query


def get_transformed_fields(
    find_expressions: List[Mapping],
    sort_expressions: List[Tuple[str, SortDirection]],
    projection_map: dict,
    mode: PaginationMode = "forward",
    cursor: Optional[str] = None,
    combine_find_statements: bool = False,
) -> TransformedFields:
    # projection_map must contain everything in the sort expressions to be able to calculate the cursor
    _sort_fields = [field for field, _ in sort_expressions]
    _project_fields = [field for field, val in projection_map.items() if val]
    fields_to_add = {field: 1 for field in set(_sort_fields) - set(_project_fields)}
    projection_map.update(fields_to_add)

    # unserialise the cursor.
    if cursor:
        print("Handling cursor")
        place, _ = unserialize_bookmark(cursor)
        if len(place) != len(sort_expressions):
            raise InvalidPageError(
                "Length of provided cursor is not equal to length of ordering fields."
            )
        sort_expressions, pagination_query = construct_pagination_query(
            place, sort_expressions, mode
        )
        if combine_find_statements and find_expressions:
            find_expressions = [{"$and": [find_expressions, pagination_query]}]
        else:
            find_expressions.append(pagination_query)

    return TransformedFields(
        find_expressions=find_expressions,
        sort_expressions=sort_expressions,
        projection_map=projection_map,
    )


def transform_beanie_query(
    beanie_query: FindMany[FindQueryResultType],
    mode: PaginationMode = "forward",
    cursor: Optional[str] = None,
    per_page: int = 25,
) -> FindMany[FindQueryResultType]:
    def __get_beanie_expression(direction: int):
        if direction == 1:
            return BeanieSortDirection.ASCENDING
        return BeanieSortDirection.DESCENDING

    transformed_fields = get_transformed_fields(
        find_expressions=beanie_query.find_expressions,
        sort_expressions=[
            (field, direction.value)
            for field, direction in beanie_query.sort_expressions
        ],
        projection_map=__convert_prjection_model_to_dict(beanie_query.projection_model),
        mode=mode,
        cursor=cursor,
    )
    beanie_query.find_expressions = transformed_fields.find_expressions
    beanie_query.sort_expressions = [
        (field, __get_beanie_expression(direction))
        for field, direction in transformed_fields.sort_expressions
    ]
    beanie_query.projection_model = __get_model_with_added_fields(
        beanie_query.projection_model, transformed_fields.projection_map
    )  # type: ignore
    beanie_query.limit(per_page + 1)
    return beanie_query


async def get_page_beanie(
    beanie_query: FindMany[FindQueryResultType],
    mode: PaginationMode = "forward",
    cursor: Optional[str] = None,
    per_page: int = 25,
) -> BeaniePage[FindQueryResultType]:
    query = transform_beanie_query(beanie_query, mode, cursor, per_page)
    await query.to_list()
    return BeaniePage(documents=[], ordering_fields=[])
