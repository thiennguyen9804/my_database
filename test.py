import subprocess 
from pytest import mark as m

def run_script(commands: list[str]) -> list[str]:
    """
    Chạy chương trình ./main, gửi commands vào stdin, lấy toàn bộ output stdout.
    Trả về list các dòng output (đã strip).
    """
    process = subprocess.Popen(
        ["./db", "mydb.db"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        # stderr=subprocess.PIPE,
        text=True,
    )

    input_texts = "\n".join(commands) + "\n"
    stdout, _ = process.communicate(input_texts)
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]

    return lines

@m.it("insert and retrieves a row")
def test_inserts_and_retrieves_a_row():
    commands = [
        "insert 1 user1 person1@example.com",
        "select",
        ".exit",
    ]

    outputs = run_script(commands)
    expected = [
        "db > Executed.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db >",
    ]

    assert outputs == expected, (
        f"Output không khớp mong đợi.\n"
        f"Thực tế:   {outputs}\n"
        f"Mong đợi: {expected}"
    )


@m.it("prints error message when table is full")
def test_prints_error_message_when_table_is_full():
    commands = [
        f"insert {i} user{i} person{i}@example.com" for i in range(1, 1402)
    ]

    commands.append(".exit")
    outputs = run_script(commands)
    error_message = "db > Error: Table full."
    assert outputs[-2] == error_message, (
        f"Dòng thứ 2 từ cuối lên không khớp mong đợi.\n"
        f"Thực tế:   {outputs[-2]}\n"
        f"Mong đợi: {error_message}"
    )

    
@m.it("allows inserting strings that are the maximum length")
def test_allows_inserting_strings_that_are_the_maximum_length():
    long_username = "a"*32
    long_email = "a"*255
    commands = [
        f"insert 1 {long_username} {long_email}",
        "select",
        ".exit",
    ]

    result = run_script(commands)
    expected = [
        "db > Executed.",
        f"db > (1, {long_username}, {long_email})",
        "Executed.",
        "db >",
    ]

    assert result == expected, (
        f"Output không khớp mong đợi.\n"
        f"Thực tế:   {result}\n"
        f"Mong đợi: {expected}"
    )

@m.it("prints an error message if id is negative")
def test_prints_an_error_message_if_id_is_negative():
    commands = [
        "insert -1 cstack foo@bar.com",
        "select",
        ".exit",
    ]

    result = run_script(commands)
    expected = [
      "db > ID must be positive.",
      "db > Executed.",
      "db >",
    ]

    assert result == expected, (
        f"Output không khớp mong đợi.\n"
        f"Thực tế:   {result}\n"
        f"Mong đợi: {expected}"
    )

@m.it("prints constants")
def test_prints_constants():
    commands = [
        ".constants",
        ".exit",
    ]
    
    outputs = run_script(commands)
    
    expected = [
        "db > Constants:",
        "ROW_SIZE: 293",
        "COMMON_NODE_HEADER_SIZE: 6",
        "LEAF_NODE_HEADER_SIZE: 10",
        "LEAF_NODE_CELL_SIZE: 297",
        "LEAF_NODE_SPACE_FOR_CELLS: 4086",
        "LEAF_NODE_MAX_CELLS: 13",
        "db >",
    ]
    
    assert outputs == expected, (
        f"Output không khớp mong đợi.\n"
        f"Thực tế:   {outputs}\n"
        f"Mong đợi: {expected}"
    )
