# DDB-Compositor
An AWS DynamoDB table wrapper for a simplified interface to multiple indexes with complex composite keys

## Summary
DDB-Compositor is built to wrangle the complexity of CRUD operations in [AWS DynamoDB](https://aws.amazon.com/dynamodb/) when utilizing multiple indexes particularly when those indexes utilize composite keys. The goal of DDB-Compositor started out as a way to apply [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) principals my own code when working with DynamoDB table objects in this way. In refactoring the get_item capability, it seemed plausible to programmatically select the correct index (primary, global secondary, or local secondary) based on the collective properties provided at query time. This project is the result of that theory implementied in a mostly Pythonic manor. Certainly there are gaps in capabilites and edge-cases that simply haven't been overcome to-date. However, through a community effort, we strive to enable DDB-Compositor to achieve an intuitive, simplified, interface to AWS DynamoDB tables.

### Composite Key Definition
Composite key syntax utilized the common Python [f-string](https://realpython.com/python-f-strings/#f-strings-a-new-and-improved-way-to-format-strings-in-python) or [format-string keyword arguments](https://realpython.com/python-formatted-output/#keyword-arguments) annotation.
#### Example
Composite Key: `partition_key_format = "someItemType:{uid}:{version}"`

Composite Key Attributes: `[uid, version]`

### Auto-Item-Versioning
DDB-Compositor has implemented optional item versioning as a native capability when Primary Key attributes align.

### Index Weighting
DDB-Compositor has implemented an index weighting system to help prioritizing an index at query-time. While the Primary index weighting will be constant, Global and Local Secondary index weighting is configurable either by list order or optional index weight argument.

## Usage Examples
Functional examples of 
### Defining a Table
```python

from ddb-compositor import CompositorTable, PrimaryIndex, GlobalSecondaryIndex


table =  CompositorTable(
    table_name="example_table",
    attribute_list=[
        "some_val",
        "some_val2",
        "some_new_val",
        "some_new_val2",
        "uid",
        "name",
        "description",
        "status",
        "createdAt",
        "createdBy",
    ],
    primary_index=PrimaryIndex(
        parition_key_name="pk",
        parition_key_format="aslr:{some_val}:{some_val2}",
        sort_key_name="sk",
        sort_key_format="dslr:{some_new_val}:{some_new_val2}",
        name="PrimaryIndex",
        composite_separator=":",
    ),
    secondary_indexes=[
        GlobalSecondaryIndex(
            parition_key_name="pk", # NOTE: Same table attribute name. partition_key_format is not required.
            sort_key_name="gs1sk",
            sort_key_format="someItemType:{uid}",
            composite_separator=":",
        ),
    ],
    unique_id_attribute_name="uid",
)
```

### Creating an Item in the Table

### Reading and Paginating

### Updating and Deleting Items


## To-Do List for 1.0
- ~~Rename `hash_key` to `partition_key` to align with AWS documentation~~
- ~~Rename `range_key` to `sort_key` to align with AWS documentation~~
- ~~Define a standard response model and implement as dataclass~~
  - CANCELLED - Returning native DDB Table response
- Refactor CRUD operations to be more readable and DRY
  - put_item
  - get_item
  - get_items
  - update_item
  - delete_item
- Add pagination utilizing `next_token` property
- Implement table flag to optionally return DynamoDB consumed capacity
- Implement FilterExpression capabilities as part of read operations
- Include projection expression in secondary index automatic selection
- Implement doc-strings for all classes and methods

## Project Status Tracking
DDB-Compositor roadmap and development progress can be tracked on the [DDB-Compositor Project](https://github.com/travisdieckmann/ddb-compositor/projects/1) As feature requests are qualified, they will be added to the Icebox column. As they are prioritized by the core team (or direct contribution identified) features will be promoted to the "To do" list.

## Contributing
Details on how to contribute to the project can be found here on the [CONTRIBUTING](https://github.com/travisdieckmann/ddb-compositor/blob/main/CONTRIBUTING.md) page.

## License
DDB-Compositor is licensed unter the [MIT License](https://github.com/travisdieckmann/ddb-compositor/blob/main/LICENSE)