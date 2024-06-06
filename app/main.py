import sys

database_file_path = sys.argv[1]
command = sys.argv[2]

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # Read the database page size
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}")

        # Count the number of tables by searching for "CREATE TABLE" in the file
        database_file.seek(0)  # Go back to the beginning of the file
        number_of_tables = sum(line.count(b"CREATE TABLE") for line in database_file)
        print(f"number of tables: {number_of_tables}")
else:
    print(f"Invalid command: {command}")