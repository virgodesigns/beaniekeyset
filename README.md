# Beaniekeyset
This repository is inspired from sqlakeyset repository found [here](https://github.com/djrobstep/sqlakeyset/pulls). That repository enables keyset based pagination for sqlalchemy, this repository enables keyset / cursor based pagination for [Beanie](https://github.com/roman-right/beanie), but methods are also provided to get pagination queries for regular pymongo/motor queries.

Note that this is v0 of this project, and changes are expected.

## Usage
Install using poetry or pip
```
poetry add beaniekeyset
```
or you can build from source. Just do a git clone, and `poetry build`.

```python
# Beanie model
from beanie import Document
from pymongo import IndexModel, DESCENDING

class BasicDocument(Document):
    name: str
    number: int

    class Settings:
        name = "tests"
        indexes = [
            IndexModel(
                keys=[("name", DESCENDING)],
                unique=True
            ),
            IndexModel(
                keys=[("number", DESCENDING)]
            ),
        ]
__beanie_models__ = [BasicDocument]

# db session
from motor.motor_asyncio import AsyncIOMotorClient

db_session = AsyncIOMotorClient(<database_dsn>)

# init models
await init_beanie(
    database=db_session.db_name,
    document_models=__beanie_models__,
    allow_index_dropping=True,
)

# insert some dummy data
...

# query
from pydantic import BaseModel, Field
from pymongo import ASCENDING, DESCENDING

class Projection(BaseModel):
    id: Any = Field(alias="_id")
    number: int

mdb_query = (
    BasicDocument
    .find(BasicDocument.number > 5)
    .project(Projection)
    .sort([
        (BasicDocument.name, ASCENDING),
        (BasicDocument.id, ASCENDING) # always add a sort to an unique field
    ])
)

# get a page
page = await get_page_beanie(mdb_query, per_page=5)
# page.documents has the actual data, page.documents_with_cursors gives cursor for each document along with the document. page.has_next, page.has_previous tell whether any previous and next documents are available, and page.bookmark_last, page.bookmark_first provide first and last books.

end_cursor = page.bookmark_last
mdb_query = (
    BasicDocument
    .find(BasicDocument.number > 5)
    .project(Projection)
    .sort([
        (BasicDocument.name, ASCENDING),
        (BasicDocument.id, ASCENDING) # always add a sort to an unique field
    ])
)
page_next = await get_page_beanie(mdb_query, per_page=5, cursor=end_cursor)

# you can also go backward
start_cursor = page_next.bookmark_first
mdb_query = (
    BasicDocument
    .find(BasicDocument.number > 5)
    .project(Projection)
    .sort([
        (BasicDocument.name, ASCENDING),
        (BasicDocument.id, ASCENDING) # always add a sort to an unique field
    ])
)
page_og = await get_page_beanie(mdb_query, per_page=5, cursor=end_cursor, mode="backwards")

# page_og should be same as our original page
```

## Warnings
- The query passed to `get_page_beanie` is transformed by the function, because I have no idea how to clone a beanie find object.
- I haven't tested all possible combinations of some fields being sorted by ascending, some by descending, with forward and backward modes. But basic pagination should work.
