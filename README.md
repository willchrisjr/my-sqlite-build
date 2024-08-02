
# SQLite Query Engine

This project implements a basic SQLite query engine capable of handling `.dbinfo`, `.tables`, and `SELECT` commands with `WHERE` clauses. The engine can traverse B-trees to handle large tables that span multiple pages.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Usage](#usage)
- [File Structure](#file-structure)
- [Implementation Details](#implementation-details)
- [Setup](#setup)
- [Dependencies](#dependencies)
- [Sample Databases](#sample-databases)
- [References](#references)

## Introduction

This project is a simplified SQLite query engine that can read SQLite database files and execute basic SQL queries. It supports commands to display database information, list tables, and select data from tables with filtering conditions.

## Features

- **Display Database Information**: Retrieve and display the database page size and the number of tables.
- **List Tables**: List all tables in the database.
- **Execute SELECT Queries**: Execute `SELECT` queries with support for `WHERE` clauses.
- **B-tree Traversal**: Traverse B-trees to handle large tables that span multiple pages.
- **Support for Special SQLite Tables**: Handle special SQLite tables like `sqlite_schema`.

## Usage

To run the query engine, use the following command:

```sh
$ ./your_sqlite3.sh <database_file> "<SQL_command>"
```

### Examples

1. Display database information:

```sh
$ ./your_sqlite3.sh sample.db .dbinfo
```

2. List all tables in the database:

```sh
$ ./your_sqlite3.sh sample.db .tables
```

3. Execute a `SELECT` query with a `WHERE` clause:

```sh
$ ./your_sqlite3.sh superheroes.db "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"
```

## File Structure

- `main.py`: Main script that handles reading the database file, parsing SQL commands, and executing actions.
- `parser.py`: Module for parsing SQL commands.
- `your_sqlite3.sh`: Shell script to run the query engine.

## Implementation Details

### `main.py`

This script handles the main logic of reading the database file, parsing the SQL command, and executing the appropriate actions, including traversing the B-tree.

#### Key Functions

- `main()`: Entry point of the script.
- `handle_dbinfo()`: Handles the `.dbinfo` command.
- `handle_tables()`: Handles the `.tables` command.
- `handle_select()`: Handles the `SELECT` command.
- `parse_btree_header()`: Parses the B-Tree header from a page.
- `parse_varint()`: Parses a variable-length integer (varint) from a buffer.
- `size_for_type()`: Returns the size for a given serial type.
- `parse_record()`: Parses a record from a buffer.
- `get_page()`: Reads a page from the file.
- `read_table()`: Reads the table data from the file, applying the `WHERE` clause if provided.
- `select_all_from_sqlite_schema()`: Selects all rows from the `sqlite_schema` table.

### `parser.py`

This module handles the parsing of SQL commands.

#### Key Functions

- `scan()`: Tokenizes the input text.
- `_scan()`: Scans the input text and generates tokens.
- `parse()`: Parses the input text and generates statements.
- `_parse()`: Parses the tokens and generates a statement.
- `_parse_select_stmt()`: Parses a `SELECT` statement.
- `_parse_selection()`: Parses a selection expression.

#### Namedtuples

- `SelectStmt`: Represents a `SELECT` statement.
- `FunctionExpr`: Represents a function expression.
- `NameExpr`: Represents a name expression.
- `StarExpr`: Represents a star expression (`*`).
- `BinaryExpr`: Represents a binary expression (e.g., `lhs = rhs`).
- `StringExpr`: Represents a string expression.

#### Helper Classes

- `_peekable`: A helper class to allow peeking at the next item in an iterator.

#### Exceptions

- `ParseError`: Custom exception for parse errors.

## Setup

### Prerequisites

- Python 3.x
- `sqlparse` (for SQL parsing)

### Installation

1. Clone the repository:

```sh
git clone <repository_url>
cd <repository_directory>
```

2. Install the required dependencies:

```sh
pip install sqlparse
```

### Running the Query Engine

To run the query engine, use the following command:

```sh
$ ./your_sqlite3.sh <database_file> "<SQL_command>"
```

## Dependencies

- Python 3.x
- `sqlparse` (for SQL parsing)

## Sample Databases

To test the query engine, you can use sample databases provided in the repository. For example, `superheroes.db` is a sample database of superheroes.

## References

- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [SQLite B-tree Pages](https://www.sqlite.org/fileformat2.html#btree)
- [Vaidehi Joshi's Busying Oneself With B-Trees](https://medium.com/basecs/busying-oneself-with-b-trees-3c6f3f1f3f16)



