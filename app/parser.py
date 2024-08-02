from collections import namedtuple

# Define a namedtuple for tokens with type and text attributes
Token = namedtuple("Token", "type,text")
_NOTHING = object()  # Sentinel object to represent no value

# Custom exception for parse errors
class ParseError(Exception):
    pass

# class to make an iterator peekable
class _peekable:
    def __init__(self, it):
        self._it = it  # The underlying iterator
        self._peeked = _NOTHING  # The peeked value, initially set to _NOTHING

    def __next__(self):
        # If there's a peeked value, return it and reset _peeked
        if self._peeked is not _NOTHING:
            val = self._peeked
            self._peeked = _NOTHING
            return val
        # Otherwise, return the next value from the iterator
        return next(self._it)

    def peek(self):
        # If there's no peeked value, get the next value from the iterator
        if self._peeked is _NOTHING:
            try:
                self._peeked = next(self._it)
            except StopIteration:
                self._peeked = None
        return self._peeked

# Function to scan the input text and generate tokens
def scan(text):
    yield from _scan(_peekable(iter(text)))

# Mapping of single-character tokens to their types
_one_char_tokens = {
    ",": "COMMA",
    "(": "LPAREN",
    ")": "RPAREN",
    ";": "SEMICOLON",
    "*": "STAR",
    "=": "EQUAL",
}

# Mapping of keywords to their types
_keywords = {
    "SELECT".casefold(): "SELECT",
    "FROM".casefold(): "FROM",
    "WHERE".casefold(): "WHERE",
    "CREATE".casefold(): "CREATE",
    "TABLE".casefold(): "TABLE",
}

# Internal function to scan the input and generate tokens
def _scan(it):
    while True:
        c = next(it, None)  # Get the next character
        if c is None:
            break  # End of input

        if c.isspace():
            continue  # Skip whitespace
        elif c in _one_char_tokens:
            yield Token(_one_char_tokens[c], c)  # Single-character token
        elif c.isalpha():
            # Handle identifiers and keywords
            name = c
            while it.peek() is not None and (it.peek().isalnum() or it.peek() == "_"):
                name += next(it)
            if name.casefold() in _keywords:
                yield Token(_keywords[name.casefold()], name)
            else:
                yield Token("NAME", name)
        elif c in ("'", '"'):
            # Handle string literals
            terminator = c
            str_content = ""
            while True:
                c = next(it, None)
                if c is None:
                    raise ParseError("Unterminated string literal")
                if c == terminator:
                    if it.peek() == terminator:
                        str_content += terminator
                    else:
                        break
                else:
                    str_content += c
            yield Token("STRING", str_content)
        else:
            raise ParseError(f"Unexpected token {c!r}")

# Function to expect a specific token type from the iterator
def _expect(it, ty):
    try:
        tok = next(it)
    except StopIteration:
        raise ParseError(f"Expected {ty}, got end of input")
    if tok.type != ty:
        raise ParseError(f"Expected {ty}, got {tok.type}")
    return tok

# Function to parse the input text and generate parse trees
def parse(text):
    yield from _parse(_peekable(scan(text)))

# Define namedtuples for different types of parse trees
SelectStmt = namedtuple("SelectStmt", "selects,from_table,where")
CreateTableStmt = namedtuple("CreateTableStmt", "name,columns")
CreateTableField = namedtuple("CreateTableField", "name,type")
FunctionExpr = namedtuple("FunctionExpr", "name,args")
NameExpr = namedtuple("NameExpr", "name")
StarExpr = namedtuple("StarExpr", "")
BinaryExpr = namedtuple("BinaryExpr", "op,lhs,rhs")
StringExpr = namedtuple("StringExpr", "text")

# Internal function to parse the input and generate parse trees
def _parse(it):
    if it.peek() and it.peek().type == "SELECT":
        yield _parse_select_stmt(it)
    elif it.peek() and it.peek().type == "CREATE":
        yield _parse_create_table(it)
    else:
        raise ParseError(f"Unexpected token {it.peek()!r}")

    if it.peek() is not None:
        raise ParseError(f"Trailing characters after query: {it.peek()!r}")

# Function to parse a SELECT statement
def _parse_select_stmt(it):
    _expect(it, "SELECT")

    selects = []
    first = True
    while it.peek() and it.peek().type != "FROM":
        if first:
            first = False
        else:
            _expect(it, "COMMA")
        selects.append(_parse_selection(it))

    _expect(it, "FROM")

    from_table = _expect(it, "NAME")

    tok = next(it, None)
    where = None
    if tok and tok.type == "WHERE":
        # FIXME: proper expression parsing
        lhs = next(it, None)
        op = next(it, None)
        rhs = next(it, None)
        if (
            lhs is None
            or op is None
            or rhs is None
            or lhs.type != "NAME"
            or op.type != "EQUAL"
            or rhs.type != "STRING"
        ):
            raise ParseError("Unsupported WHERE clause")
        where = BinaryExpr(op.type, NameExpr(lhs.text), StringExpr(rhs.text))
    elif tok and tok.type != "SEMICOLON":
        raise ParseError(f"Expected end of input or semicolon, got {tok.text!r}")

    return SelectStmt(selects, from_table.text, where)

# Function to parse a selection in a SELECT statement
def _parse_selection(it):
    name = next(it, None)
    if not name or name.type not in ("NAME", "STAR"):
        raise ParseError(f"Expected name or '*', got {name!r}")

    if name.type == "STAR":
        return StarExpr()

    if not it.peek() or it.peek().type != "LPAREN":
        return NameExpr(name.text)

    args = []
    first = True
    next(it)
    while it.peek() and it.peek().type != "RPAREN":
        if first:
            first = False
        else:
            _expect(it, "COMMA")
        args.append(_parse_selection(it))

    _expect(it, "RPAREN")

    return FunctionExpr(name.text.upper(), args)

# Function to parse a CREATE TABLE statement
def _parse_create_table(it):
    _expect(it, "CREATE")
    _expect(it, "TABLE")

    name = next(it, None)
    if name is None:
        raise ParseError("Unexpected end of input, expected table name")
    elif name.type not in ("NAME", "STRING"):
        raise ParseError(f"Expected table name to be string or name, got {name.text!r}")

    table_name = name.text

    _expect(it, "LPAREN")
    columns = []
    first = True
    while it.peek() and it.peek().type != "RPAREN":
        if first:
            first = False
        else:
            _expect(it, "COMMA")
        col_name = _expect(it, "NAME").text
        type_parts = []
        while it.peek() and it.peek().type not in ("COMMA", "RPAREN"):
            type_parts.append(_expect(it, "NAME").text)
        col_type = " ".join(type_parts)
        columns.append(CreateTableField(col_name, col_type))

    _expect(it, "RPAREN")

    tok = next(it, None)
    if tok and tok.type != "SEMICOLON":
        raise ParseError(f"Expected end of input or semicolon, got {tok.text!r}")

    return CreateTableStmt(table_name, tuple(columns))