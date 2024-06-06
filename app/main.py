import sys
from struct import unpack

# Get the database file path and command from the command line arguments
database_file_path = sys.argv[1]
command = sys.argv[2]

# Function to read a variable-length integer (varint) from the file
def read_varint(file, n):
    var = int.from_bytes(file.read(1), byteorder="big")
    ans = var & 0x7f
    n -= 1
    while (var >> 7) & 1:
        var = int.from_bytes(file.read(1), byteorder="big")
        ans = (ans << 7) + (var & 0x7f)
        n -= 1
    return ans, n

# Class to represent a page in the database
class Page:
    def __init__(self, database_file):
        self.database_file = database_file
        # Read the page header
        self.type, self.freeblock, self.num_cells, self.cell_start, self.num_fragment = unpack("!BHHHB", database_file.read(8))
        # Read the cell pointers
        self.offsets = [int.from_bytes(database_file.read(2), byteorder="big") for _ in range(self.num_cells)]

    # Method to get the cells in the page
    def get_cells(self):
        contents_sizes = [0, 1, 2, 3, 4, 6, 8, 8, 0, 0]
        cells = []
        for offset in self.offsets:
            self.database_file.seek(offset)
            num_payload = int.from_bytes(self.database_file.read(1), byteorder="big")
            row_id = int.from_bytes(self.database_file.read(1), byteorder="big")
            payload = num_payload - 2  # for num_payload & row_id
            num_bytes = int.from_bytes(self.database_file.read(1), byteorder="big") - 1  # -1 for self
            sizes = []
            while num_bytes > 0:
                type, num_bytes = read_varint(self.database_file, num_bytes)
                if type >= 13 and type % 2:
                    sizes.append((type - 13) // 2)
                elif type >= 12 and type % 2 == 0:
                    sizes.append((type - 12) // 2)
                else:
                    sizes.append(contents_sizes[type])
            cells.append([self.database_file.read(size) for size in sizes])
        return cells

# Class to represent the database
class Database:
    def __init__(self, database_file_path):
        self.database_file_path = database_file_path
        self.database_file = open(database_file_path, "rb")
        self.database_file.seek(16)  # Skip the first 16 bytes of the header
        self.page_size = int.from_bytes(self.database_file.read(2), byteorder="big")
        self.schema_table = self.get_page(1)

    def __del__(self):
        self.database_file.close()

    # Method to get a page by its number
    def get_page(self, num):
        if num == 1:
            self.database_file.seek(100)
        else:
            self.database_file.seek(self.page_size * (num - 1))
        page = Page(self.database_file)
        return page

# Create a Database instance
db = Database(database_file_path)

# Handle the .dbinfo command
if command == ".dbinfo":
    # Print the database page size and number of tables
    print(f"database page size: {db.page_size}")
    print(f"number of tables: {db.schema_table.num_cells}")

# Handle the .tables command
elif command == ".tables":
    # Print the names of the tables
    for schema in db.schema_table.get_cells():
        if schema[0] == b"table":
            print(schema[2].decode('utf-8'))  # table_name

# Handle the SELECT COUNT(*) FROM <table> command
elif command.startswith("select count(*) from "):
    # Extract the table name from the command
    table = command.split(" ")[-1]
    page_num = -1
    # Find the root page number for the table
    for schema in db.schema_table.get_cells():
        if schema[0] == b"table" and schema[2].decode('utf-8') == table:
            page_num = int.from_bytes(schema[3], 'big')
    # Print the number of rows (cells) in the table
    print(len(db.get_page(page_num).offsets))

# Handle invalid commands
else:
    print(f"Invalid command: {command}")