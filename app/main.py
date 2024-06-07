import struct
import sys
from collections import namedtuple

import app.parser as parser

def main():
    # Get the database file path and command from the command line arguments
    database_file_path = sys.argv[1]
    command = sys.argv[2]

    # Open the database file in binary read mode
    with open(database_file_path, "rb") as database_file:
        # Skip the first 16 bytes of the header
        database_file.seek(16)
        # Read the next 2 bytes to get the page size
        page_size = int.from_bytes(database_file.read(2), byteorder="big")

        # Skip to the 56th byte to read the text encoding
        database_file.seek(56)
        text_encoding = ["utf-8", "utf-16-le", "utf-16-be"][
            int.from_bytes(database_file.read(4), byteorder="big") - 1
        ]

        # Create a DBConfig namedtuple with the page size and text encoding
        db_config = DBConfig(page_size=page_size, text_encoding=text_encoding)

        if command == ".dbinfo":
            # Print the database page size
            print(f"database page size: {page_size}")

            # Read the first page and parse the B-tree header
            database_file.seek(0)
            page = database_file.read(page_size)
            btree_header = parse_btree_header(page, is_first_page=True)[0]
            # Print the number of tables
            print(f"number of tables: {btree_header.cell_count}")

        elif command == ".tables":
            # Print the names of all tables in the database
            print(
                " ".join(
                    row.tbl_name
                    for row in select_all_from_sqlite_schema(
                        database_file, db_config
                    )
                    if row.type == "table" and not row.tbl_name.startswith("sqlite_")
                )
            )
        else:
            # Parse the SQL command
            stmt = next(parser.parse(command))

            if not isinstance(stmt, parser.SelectStmt):
                # Only SELECT statements are supported
                print("Only know select", file=sys.stderr)
                return 1

            table_name = stmt.from_table
            if table_name.casefold() in (
                "sqlite_schema".casefold(),
                "sqlite_master".casefold(),
                "sqlite_temp_schema".casefold(),
                "sqlite_temp_master".casefold(),
            ):
                # Handle special SQLite tables
                table_schema = SqliteSchema(
                    0,
                    "table",
                    "sqlite_schema",
                    "sqlite_schema",
                    1,
                    "CREATE TABLE sqlite_schema (\n"
                    "  type text,\n"
                    "  name text,\n"
                    "  tbl_name text,\n"
                    "  rootpage integer,\n"
                    "  sql text\n"
                    ");",
                )
            else:
                # Find the table schema in the SQLite schema
                try:
                    table_schema = next(
                        table_info
                        for table_info in select_all_from_sqlite_schema(
                            database_file, db_config
                        )
                        if table_info.type == "table"
                        and table_info.tbl_name.casefold() == table_name.casefold()
                    )
                except StopIteration:
                    print(f"Unknown table '{table_name}'", file=sys.stderr)
                    return 1

            # Get the root page of the table
            page = get_page(database_file, db_config, table_schema.rootpage)
            btree_header, bytes_read = parse_btree_header(
                page, table_schema.rootpage == 1
            )

            if (
                len(stmt.selects) == 1
                and isinstance(stmt.selects[0], parser.FunctionExpr)
                and stmt.selects[0].name == "COUNT"
                and len(stmt.selects[0].args) == 1
                and isinstance(stmt.selects[0].args[0], parser.StarExpr)
            ):
                # Handle COUNT(*) queries
                print(btree_header.cell_count)
            else:
                # Parse the column definitions from the table schema
                columns = [
                    tuple(column_spec.strip().split(None, 1))
                    for column_spec in (
                        table_schema.sql.split("(", 1)[1].rsplit(")", 1)[0].split(",")
                    )
                ]
                column_order = {
                    name.casefold(): i for i, (name, _type) in enumerate(columns)
                }

                # Find the primary key column index
                primary_key_column_idx = next(
                    (
                        column_index
                        for column_index, (_name, type_) in enumerate(columns)
                        if type_.casefold().split()[:3]
                        == [
                            "integer".casefold(),
                            "primary".casefold(),
                            "key".casefold(),
                        ]
                    ),
                    None,
                )

                table_info = TableInfo(rootpage=table_schema.rootpage, int_pk_column=primary_key_column_idx)

                try:
                    if len(stmt.selects) == 1 and isinstance(
                        stmt.selects[0], parser.StarExpr
                    ):
                        # Select all columns
                        selected_columns = list(range(len(column_order)))
                    elif not all(
                        isinstance(select, parser.NameExpr) for select in stmt.selects
                    ):
                        print("Only simple queries are supported", file=sys.stderr)
                        return 1
                    else:
                        # Select specific columns
                        selected_columns = [
                            column_order[name_expr.name.casefold()]
                            for name_expr in stmt.selects
                        ]
                except KeyError as e:
                    print(f"Unknown column {e}", file=sys.stderr)
                    return 1

                where = None
                if stmt.where:
                    # Handle WHERE clause
                    where = (
                        column_order[stmt.where.lhs.name.casefold()],
                        stmt.where.rhs.text,
                    )

                # Read and print the selected columns from the table
                for column_values in read_table(
                    database_file,
                    db_config,
                    table_info,
                    selected_columns,
                    where,
                ):
                    print("|".join(str(val) for val in column_values))

    return 0

# Define namedtuples for B-tree header and database configuration
BTreeHeader = namedtuple(
    "BTreeHeader",
    [
        "type",
        "first_freeblock",
        "cell_count",
        "cell_content_start",
        "fragmented_free_bytes",
        "rightmost_pointer",
    ],
)

# Constants for B-tree page types
BTREE_PAGE_INTERIOR_INDEX = 0x02
BTREE_PAGE_INTERIOR_TABLE = 0x05
BTREE_PAGE_LEAF_INDEX = 0x0A
BTREE_PAGE_LEAF_TABLE = 0x0D

def parse_btree_header(page, is_first_page=False):
    # Parse the B-tree header from the page
    offset = 100 if is_first_page else 0
    type_, first_freeblock, cell_count, cell_content_start, fragmented_free_bytes = (
        struct.unpack_from(">BHHHB", page, offset)
    )
    if type_ in (BTREE_PAGE_INTERIOR_INDEX, BTREE_PAGE_INTERIOR_TABLE):
        (rightmost_pointer,) = struct.unpack_from(">I", page, offset + 8)
        bytes_read = 12
    else:
        rightmost_pointer = 0
        bytes_read = 8
    return BTreeHeader(
        type_,
        first_freeblock,
        cell_count,
        cell_content_start or 65536,
        fragmented_free_bytes,
        rightmost_pointer,
    ), bytes_read

def parse_varint(buf, offset=0):
    # Parse a variable-length integer from the buffer
    n = 0
    for i in range(offset, offset + 9):
        byte = buf[i]
        n <<= 7
        n |= byte & 0x7F
        if byte & 0x80 == 0:
            break
    else:
        i = -1
    return n, i + 1 - offset

def size_for_type(serial_type):
    # Determine the size of a value based on its serial type
    if serial_type < 5:
        return serial_type
    elif serial_type == 5:
        return 6
    elif 6 <= serial_type <= 7:
        return 8
    elif 8 <= serial_type <= 9:
        return 0
    elif serial_type >= 12 and serial_type % 2 == 0:
        return (serial_type - 12) // 2
    elif serial_type >= 13 and serial_type % 2 == 1:
        return (serial_type - 13) // 2
    else:
        raise NotImplementedError(serial_type)

def parse_record(db_config, table_info, page, rowid, offset, selection, where):
    # Parse a record from the page
    initial_offset = offset
    header_size, bytes_read = parse_varint(page, offset)
    header_end = offset + header_size
    offset += bytes_read
    column_types = []
    total_size = header_size
    while offset != header_end:
        column_serial_type, bytes_read = parse_varint(page, offset)
        column_size = size_for_type(column_serial_type)
        column_types.append((column_serial_type, column_size))
        offset += bytes_read
        total_size += column_size

    column_selection = {column_id: order for order, column_id in enumerate(selection)}

    column_values: list = [None] * len(column_selection)
    for column_id, (column_serial_type, size) in enumerate(column_types):
        if column_id not in column_selection and (not where or column_id != where[0]):
            offset += size
            continue

        if column_serial_type == 0:
            if column_id == table_info.int_pk_column:
                value = rowid
            else:
                continue  # None is already stored in column_values
        elif 1 <= column_serial_type <= 6:
            number_byte_size = (
                column_serial_type
                if column_serial_type < 5
                else 6
                if column_serial_type == 5
                else 8
            )
            value = int.from_bytes(
                page[offset : offset + number_byte_size], byteorder="big", signed=True
            )
        elif column_serial_type == 7:
            value = struct.unpack_from(">d", page, offset)
        elif column_serial_type in (8, 9):
            value = int(column_serial_type == 9)
        elif column_serial_type >= 12 and column_serial_type % 2 == 0:
            value_len = (column_serial_type - 12) // 2
            value = page[offset : offset + value_len]
        elif column_serial_type >= 13 and column_serial_type % 2 == 1:
            value_len = (column_serial_type - 13) // 2
            blob_value = page[offset : offset + value_len]
            try:
                value = blob_value.decode(db_config.text_encoding)
            except UnicodeDecodeError:
                # FIXME: why does this happen?
                value = blob_value
        else:
            raise NotImplementedError(column_serial_type)

        offset += size

        if where and column_id == where[0] and value != where[1]:
            return None, initial_offset + total_size

        if column_id in column_selection:
            column_values[column_selection[column_id]] = value

    return column_values, offset - initial_offset

# Define namedtuples for database configuration and table information
DBConfig = namedtuple("DBConfig", "page_size,text_encoding")
TableInfo = namedtuple("TableInfo", "rootpage,int_pk_column")

def get_page(file, db_config, id_):
    # Get a page from the database file
    file.seek((id_ - 1) * db_config.page_size)
    return file.read(db_config.page_size)

def read_table(file, db_config, table_info, selection, where):
    # Read records from a table
    page = get_page(file, db_config, table_info.rootpage)
    btree_header, bytes_read = parse_btree_header(page, is_first_page=table_info.rootpage == 1)

    btree_offset = bytes_read
    if table_info.rootpage == 1:
        btree_offset += 100

    for _i in range(btree_header.cell_count):
        (cell_content_offset,) = struct.unpack_from(">H", page, btree_offset)
        btree_offset += 2

        _payload_size, bytes_read = parse_varint(page, cell_content_offset)
        cell_content_offset += bytes_read

        rowid, bytes_read = parse_varint(page, cell_content_offset)
        cell_content_offset += bytes_read

        column_values, bytes_read = parse_record(
            db_config, table_info, page, rowid, cell_content_offset, selection, where
        )

        # filtered out
        if column_values is None:
            continue

        # ???
        # assert bytes_read == payload_size, (bytes_read, payload_size)

        yield column_values

# Define a namedtuple for SQLite schema
SqliteSchema = namedtuple(
    "SqliteSchema", ["type", "name", "tbl_name", "rootpage", "sql"]
)

def select_all_from_sqlite_schema(file, db_config):
    # Select all rows from the SQLite schema table
    for column_values in read_table(
        file, db_config, TableInfo(1, None), list(range(5)), None
    ):
        yield SqliteSchema(*column_values)

if __name__ == "__main__":
    sys.exit(main())