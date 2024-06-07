import struct
import sys
from collections import namedtuple
import sqlparse

# Namedtuple to represent the B-Tree header
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

# Constants for B-Tree page types
BTREE_PAGE_INTERIOR_INDEX = 0x02
BTREE_PAGE_INTERIOR_TABLE = 0x05
BTREE_PAGE_LEAF_INDEX = 0x0A
BTREE_PAGE_LEAF_TABLE = 0x0D

# Namedtuple to represent the sqlite_schema table
SqliteSchema = namedtuple(
    "SqliteSchema", ["rowid", "type", "name", "tbl_name", "rootpage", "sql"]
)

def main():
    # Get the database file path and command from the command line arguments
    database_file_path = sys.argv[1]
    command = sys.argv[2]

    with open(database_file_path, "rb") as database_file:
        # Read the database page size and text encoding from the header
        page_size, text_encoding = read_header(database_file)

        if command == ".dbinfo":
            handle_dbinfo(database_file, page_size)
        elif command == ".tables":
            handle_tables(database_file, page_size, text_encoding)
        else:
            handle_select(database_file, page_size, text_encoding, command)

    return 0

def read_header(database_file):
    # Read the database page size from the header
    database_file.seek(16)
    page_size = int.from_bytes(database_file.read(2), byteorder="big")

    # Read the text encoding from the header
    database_file.seek(56)
    text_encoding = ["utf-8", "utf-16-le", "utf-16-be"][
        int.from_bytes(database_file.read(4), byteorder="big") - 1
    ]

    return page_size, text_encoding

def handle_dbinfo(database_file, page_size):
    # Print the database page size
    print(f"database page size: {page_size}")

    # Read the first page to get the number of tables
    page = get_page(database_file, 1, page_size)
    btree_header = parse_btree_header(page, is_first_page=True)[0]
    print(f"number of tables: {btree_header.cell_count}")

def handle_tables(database_file, page_size, text_encoding):
    # Print the names of the tables
    print(
        " ".join(
            row.tbl_name
            for row in select_all_from_sqlite_schema(
                database_file, page_size, text_encoding
            )
            if row.type == "table" and not row.tbl_name.startswith("sqlite_")
        )
    )

def handle_select(database_file, page_size, text_encoding, command):
    for query in sqlparse.split(command):
        tokens = query.strip().split()

        if tokens[0].casefold() != "SELECT".casefold():
            print("Only know select", file=sys.stderr)
            return 1

        selects = []
        for i, token in enumerate(tokens[1:]):
            if token.casefold() == "FROM".casefold():
                break
            selects.append(token.rstrip(",").casefold())
        else:
            print("Expected something after SELECT", file=sys.stderr)
            return 1

        table_name = tokens[i + 2].casefold()

        table_info = get_table_info(database_file, page_size, text_encoding, table_name)
        if not table_info:
            print(f"Unknown table '{table_name}'", file=sys.stderr)
            return 1

        page = get_page(database_file, table_info.rootpage, page_size)
        btree_header, bytes_read = parse_btree_header(
            page, table_info.rootpage == 1
        )

        if len(selects) == 1 and selects[0] == "count(*)".casefold():
            # Print the number of rows (cells) in the table
            print(btree_header.cell_count)
        else:
            # Parse the CREATE TABLE statement to find the column index
            columns = parse_create_table(table_info.sql)
            column_order = {name.casefold(): i for i, (name, _type) in enumerate(columns)}

            try:
                if "*" in selects:
                    selected_columns = list(range(len(column_order)))
                else:
                    selected_columns = [
                        column_order[name.casefold()]
                        for name in selects
                    ]

                primary_key_selected_column_idx = next(
                    (
                        selection_index
                        for selection_index, column_index in enumerate(selected_columns)
                        if columns[column_index][1].casefold().split()[:3]
                        == [
                            "integer".casefold(),
                            "primary".casefold(),
                            "key".casefold(),
                        ]
                    ),
                    None,
                )
            except KeyError as e:
                print(f"Unknown column {e}", file=sys.stderr)
                return 1

            # Read and print the data for the specified columns
            for rowid, column_values in read_table(
                database_file,
                table_info.rootpage,
                page_size,
                text_encoding,
                selected_columns,
            ):
                if primary_key_selected_column_idx is not None:
                    column_values[primary_key_selected_column_idx] = rowid
                print("|".join(str(val) for val in column_values))

def get_table_info(database_file, page_size, text_encoding, table_name):
    if table_name.casefold() in (
        "sqlite_schema".casefold(),
        "sqlite_master".casefold(),
        "sqlite_temp_schema".casefold(),
        "sqlite_temp_master".casefold(),
    ):
        return SqliteSchema(
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
        # Find the table information from the sqlite_schema table
        try:
            return next(
                table_info
                for table_info in select_all_from_sqlite_schema(
                    database_file, page_size, text_encoding
                )
                if table_info.type == "table"
                and table_info.tbl_name.casefold() == table_name
            )
        except StopIteration:
            return None

def parse_create_table(create_table_sql):
    # Parse the CREATE TABLE statement to find the column definitions
    parsed = sqlparse.parse(create_table_sql)[0]
    columns = []
    for token in parsed.tokens:
        if token.ttype is None and token.value.startswith('('):
            columns = [col.strip().split(None, 1) for col in token.value[1:-1].split(',')]
            break
    return columns

def parse_btree_header(page, is_first_page=False):
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

def parse_record(buf, offset, text_encoding, columns):
    initial_offset = offset
    header_size, bytes_read = parse_varint(buf, offset)
    header_end = offset + header_size
    offset += bytes_read
    column_types = []
    while offset != header_end:
        column_serial_type, bytes_read = parse_varint(buf, offset)
        column_types.append((column_serial_type, size_for_type(column_serial_type)))
        offset += bytes_read

    column_selection = {column_id: order for order, column_id in enumerate(columns)}

    column_values = [None] * len(column_selection)
    for i, (column_serial_type, size) in enumerate(column_types):
        if i not in column_selection:
            offset += size
            continue

        if column_serial_type == 0:
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
                buf[offset : offset + number_byte_size], byteorder="big", signed=True
            )
        elif column_serial_type == 7:
            value = struct.unpack_from(">d", buf, offset)
        elif column_serial_type in (8, 9):
            value = int(column_serial_type == 9)
        elif column_serial_type >= 12 and column_serial_type % 2 == 0:
            value_len = (column_serial_type - 12) // 2
            value = buf[offset : offset + value_len]
        elif column_serial_type >= 13 and column_serial_type % 2 == 1:
            value_len = (column_serial_type - 13) // 2
            blob_value = buf[offset : offset + value_len]
            try:
                value = blob_value.decode(text_encoding)
            except UnicodeDecodeError:
                value = blob_value
        else:
            raise NotImplementedError(column_serial_type)

        column_values[column_selection[i]] = value
        offset += size

    return column_values, offset - initial_offset

def get_page(file, id_, page_size):
    file.seek((id_ - 1) * page_size)
    return file.read(page_size)

def read_table(file, rootpage, page_size, text_encoding, columns):
    page = get_page(file, rootpage, page_size)
    btree_header, bytes_read = parse_btree_header(page, is_first_page=rootpage == 1)

    btree_offset = bytes_read
    if rootpage == 1:
        btree_offset += 100

    for _i in range(btree_header.cell_count):
        (cell_content_offset,) = struct.unpack_from(">H", page, btree_offset)
        btree_offset += 2

        _payload_size, bytes_read = parse_varint(page, cell_content_offset)
        cell_content_offset += bytes_read

        rowid, bytes_read = parse_varint(page, cell_content_offset)
        cell_content_offset += bytes_read

        column_values, bytes_read = parse_record(
            page, cell_content_offset, text_encoding, columns
        )

        yield (rowid, column_values)

def select_all_from_sqlite_schema(file, page_size, text_encoding):
    for rowid, column_values in read_table(
        file, 1, page_size, text_encoding, list(range(5))
    ):
        yield SqliteSchema(rowid, *column_values)

if __name__ == "__main__":
    sys.exit(main())