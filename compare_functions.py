"""additional functions module for compare script"""
from os import name as os_name, system as os_system
from time import time as time_time, strftime as time_strftime, gmtime as time_gmtime
from difflib import SequenceMatcher

def write_to_file(f_name: str, lst1: list) -> None:
    """writes data to file
    Args:
        f_name (string): filename
        lst1 (list): a list of strings for faster file write
    Returns:
        None: perform write data to file"""
    with open(f_name, 'a+', encoding='utf-8') as f:
        for i in lst1:
            f.write(i)


def clear_screen() -> None:
    """Clear console depends on OS
    Args:
        None
    Returns:
        None: perform console clear using command depends on os type"""
    # For Windows
    if os_name == 'nt':
        _ = os_system('cls')
    # For macOS and Linux
    else:
        _ = os_system('clear')


def split_arg_value(arg_val: str) -> str:
    """used for kwargs split into pairs
    Args:
        arg_val (string): a kwargs item
    Returns:
        string: an f-string, splitted kwargs item divided with ':'"""
    res = arg_val.split('=')
    return f"{res[0]}: {res[1]}"


def progress_bar(current: int, total: int, descr: str, *kwargs) -> None:
    """Printing progress bar
    Args:
        current (int): current value
        total (int): total value
        *kwargs: additional info fields, will splitted with '│'
    Returns:
        None: performs progress bar printing"""
    bar_length = 28
    print('\r', end='')
    if total == 0:
        percents = 0
        filled = 0
    else:
        percents = round(current / total * 100, 1)
        filled = round((current / total * 100) * (bar_length / 100))
    unfilled = bar_length - filled

    print(f"[ {'█' * round(filled)}{'░' * unfilled} ] {percents:7}%", end='')
    print(f" │ {descr} {current} / {total}", end='')

    if kwargs:
        for i in kwargs:
            print(f" │ {split_arg_value(i)}", end='')


def key_composer(rec: list, keys: list) -> str:
    """creates a key from key fields values
    Args:
        rec (list): a record string splitted into fields
        keys (list): a list of field numbers
    Returns:
        string: concatinated field values of fields listed in keys"""
    res = ''
    for i in keys:
        res += rec[i]
    return res


def get_val_with_percents(num1: int, num2: int, width: str) -> str:
    """returns value with percents, supress div by zero
    Args:
        num1 (int): first number
        num2 (int): secong number
        width (string): result string width, padded with spaces
    Returns:
        string: second number with percents from first number"""
    res = ''
    if num2 > 0:
        tmp = f"{num2} ({num2 / num1:.2%})"
        res = f"{tmp:{width}}"
    else:
        res = f"{'0 (0.00%)':{width}}"
    return res


def calculate_estimated_time(curr_item: int, total_items: int, start_t) -> str:
    """calculating estimated time left
    Args:
        curr_item (int): current item number
        total_items (int): total items number
        start_t (float): starttime
    Returns:
        string: calculated estimate time"""
    curr_time = time_time()
    elapsed = curr_time - start_t
    if curr_item - elapsed > 0:
        res = f"{time_strftime('%H:%M:%S', time_gmtime(total_items * elapsed / curr_item - elapsed))}"
    else:
        return '--:--:--'
    return res


def deep_diff(s1: str, s2: str) -> str:
    """returns an abstract diff pattern
    Args:
        s1 (string): first diff string
        s2 (string): second diff string
    Returns:
        string: diff pattern"""
    matcher = SequenceMatcher(None, s1, s2)
    result = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            result.append('.' * (i2 - i1))
        elif tag == 'replace':
            # result.append(f"[{s1[i1:i2]}→{s2[j1:j2]}]")
            result.append(f"[{'x' * len(s1[i1:i2])} → {'x' * len(s2[j1:j2])}]")
        elif tag == 'delete':
            result.append('-' * (i2 - i1))
        elif tag == 'insert':
            result.append('+' * len(s2[j1:j2]))
    return ''.join(result)
