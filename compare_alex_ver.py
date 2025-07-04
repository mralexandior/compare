"""diff script"""

import configparser
import time
from datetime import datetime
from re import split as re_split, findall as re_findall, fullmatch as re_fullmatch
from os import name as os_name, path as os_path, makedirs as os_makedirs
from sys import exit as sys_exit
from glob import glob
from threading import active_count as threading_active_count, Thread
from shutil import move
import compare_functions as cf


start_time = time.time()
diff_tag_prefix = datetime.now().strftime("%Y%m%d_%H%M%S")

SCRIPT_PATH = ''
if os_name == 'nt':
    SCRIPT_PATH = re_split(r'(.+\\).+$', __file__)[1] # for win
else:
    SCRIPT_PATH = re_split(r'(.+/).+$', __file__)[1] # for linux

LOGFILE_NAME = f"{SCRIPT_PATH}diff_{diff_tag_prefix}.log"
logfile = open(LOGFILE_NAME, 'a+', encoding='utf-8')
config = configparser.ConfigParser()
config_keys = configparser.ConfigParser()

fields_error_dict = {} # for output errors to livestat
# stats_list:
# 0 pairs total, 1 current pair, 2 oldSys recs count, 3 newsys recs count, 4 matched count,
# 5 losts count, 6 extras count, 7 oldSys repeats count, 8 newsys repeats count, 9 broken count,
# 10 identical, 11 current_pair_recs count, 12 current processing rec number,
# 13 current filename, 14 defective recs count, 15 current stat text, 16 curr rec for estimated
# 17 broken records
stats_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '', 0, '', 0, 0] # for total stats info
stats_list_curr_file = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '', 0, '', 0, 0]
REPORT_HEADER = (f"{'File name':26}\t{'OldSys records':20}\t{'NewSys records':20}\t"
                 f"{'Matched records':20}\t{'Lost records':20}\t{'Extra records':20}\t"
                 f"{'OldSys repeats':20}\t{'NewSys repeats':20}\t{'Broken records':20}\t"
                 f"{'Broken attributes':20}\t{'Defective records':20}\t{'Identical records':20}\n")
legacy_total_lines = 0
newsys_total_lines = 0
raw_lines_count_by_file = {} # keep number of lines for each file
errored_fields_set = set() # for proper closing err report files
thread_break_flag = False
interrupt_flag = False
diff_pattern_dict = {} # for top error patterns report


settings_dict = {
    'log_to_file': True,
    'log_file_size_limit': 20,
    'old_log_files_count': 1,
    'encode_etl': 'utf-8',
    'encode_src': 'utf-8',
    'diff_name': 'Diff',
    'diff_keys_type': 'auto',
    'diff_keys_search_count': 1000,
    'auto_keys_search_each_run': True,
    'diff_keys': '',
    'excluded_diff_keys': '',
    'legacy_system_name': 'OldSys',
    'new_system_name': 'NewSys',
    'work_dir': SCRIPT_PATH,
    'etalons':  SCRIPT_PATH + 'oldsys_out/',
    'sources':  SCRIPT_PATH + 'newsys_out/',
    'diff_out_dir': SCRIPT_PATH + 'res/',
    'files_mask_etalon': 'old_*',
    'files_mask_source': 'new_*',
    'files_rename_regex': '^.{3}_(.+)',
    'number_of_fields': 0,
    'excluded_fields': '',
    'delimiter_type': 'char',
    'delimiter': ',',
    'header_records_number': 0,
    'trailer_records_number': 0,
    'record_field_names': '',
    'record_field_sizes': '',
    'record_field_sizes_tail': False
}

sections_dict = {
    'log_to_file':'general',
    'log_file_size_limit': 'general',
    'old_log_files_count': 'general',
    'encode_etl': 'general',
    'encode_src': 'general',
    'diff_name': 'general',
    'diff_keys_type': 'general',
    'diff_keys_search_count': 'general',
    'auto_keys_search_each_run': 'general',
    'diff_keys': 'general',
    'excluded_diff_keys': 'general',
    'legacy_system_name': 'general',
    'new_system_name': 'general',
    'work_dir': 'storage_options',
    'etalons': 'storage_options',
    'sources': 'storage_options',
    'diff_out_dir': 'storage_options',
    'files_mask_etalon': 'storage_options',
    'files_mask_source': 'storage_options',
    'files_rename_regex': 'storage_options',
    'number_of_fields': 'cdr_options',
    'excluded_fields': 'cdr_options',
    'delimiter_type': 'cdr_options',
    'delimiter': 'cdr_options',
    'header_records_number': 'cdr_options',
    'trailer_records_number': 'cdr_options',
    'record_field_names': 'cdr_options',
    'record_field_sizes': 'cdr_options',
    'record_field_sizes_tail': 'cdr_options'
}

expected_dict = {
    'log_to_file': ['True', 'False'],
    'encode_etl': ['utf-8', 'utf-16', 'cp1251'],
    'encode_src': ['utf-8', 'utf-16', 'cp1251'],
    'diff_keys_type': ['auto', 'manual'],
    'auto_keys_search_each_run': ['True', 'False'], 
    'delimiter_type': ['char', 'fixed'],
    'record_field_sizes_tail': ['True', 'False']
}


def timestamp_output(msg: str) -> None:
    """console output with timestamp
    Args: 
        msg (string): message for output
    Returns: 
        None: performs console print of message with current timestamp"""
    ts = datetime.now()
    if threading_active_count() <= 2:
        print(ts, msg)
    # cf.write_to_file(LOGFILE_NAME, f"{ts} {msg}\n")
    logfile.write(f"{ts} {msg}\n")


def print_except_msg(key: str) -> None:
    """prints detailed exception message
    Args:
        key (string): name of settings attribute as key
    Returns:
        None: performs console print"""
    print(f"(WARN) Parameter {sections_dict[key]}/{key} in compare_settings.ini "
          f"is not set, invalid or absent. Used default setting: {settings_dict[key]}")


def settings_attribute_setter(settings_key: str, value: str) -> any:
    """returns value type depends on settings_dict value types
    Args:
        settings_key (string): name of settings attribute as key
        value (string): value of attribute
    Returns:
        Any: type depends on attribute type of settings_dict"""
    res = ''
    if isinstance(settings_dict[settings_key], bool):
        # init bool settings
        res = value in ['True']
    elif isinstance(settings_dict[settings_key], int):
        # init int settings
        res = int(value)
    elif isinstance(settings_dict[settings_key], str):
        # init str settings
        res = value
    return res


def init_settings() -> None:
    """init settings, check if settings is correct
    Args:
        None
    Returns:
        None: performs settings init process"""
    try:
        for k, v in settings_dict.items():
            val = config.get(sections_dict[k], k).strip()
            if val != '':
                if k in expected_dict:
                    if val not in expected_dict[k]:
                        timestamp_output(f"(WARN) Invalid value for attribute: "
                                        f"{sections_dict[k]}/{k}.\n"
                                        f"Expected: {expected_dict[k]}'. "
                                        f"Used default setting: {v}")
                    else:
                        settings_dict[k] = settings_attribute_setter(k, val)
                else:
                    settings_dict[k] = settings_attribute_setter(k, val)
            else:
                value = ''
                if v == '':
                    value = '[empty]'
                else:
                    value = v
                timestamp_output(f"(DEFAULTS) no value provided for {sections_dict[k]}/{k}, "
                                    f"used default setting: {value}")
    except:
        print_except_msg(k)

    # for proper tabulation symbol passthrough
    if settings_dict['delimiter'] == ('\\t'):
        settings_dict['delimiter'] = '\t'

    # checking number of record fields is set
    if settings_dict['number_of_fields'] == 0:
        timestamp_output('(ERROR) number_of_fields = 0, expected > 0')
        sys_exit()

    # checking if diff_keys are set in case of diff_keys_type = manual
    if settings_dict['diff_keys_type'] == 'manual' and settings_dict['diff_keys'] == '':
        timestamp_output('(ERROR) diff_keys must be provided if diff_keys_type = manual')
        sys_exit()

    # check if provided diff keys are in range of fields count, also check for duplicates and
    # empty keys
    if settings_dict['diff_keys_type'] == 'manual' and settings_dict['diff_keys'] != '':
        if check_key_overlimit(settings_dict['diff_keys']):
            timestamp_output('(ERROR) provided diff_keys out of number_of_fields,'
                  ' has empty keys or key numbers has duplicates')
            sys_exit()

    # check if provided excluded diff keys are in range of fields count, also check for
    # duplicates and empty keys
    if settings_dict['excluded_diff_keys'] != '':
        if check_key_overlimit(settings_dict['excluded_diff_keys']):
            timestamp_output('(ERROR) provided excluded_diff_keys out of number_of_fields,'
                  ' has empty keys or key numbers has duplicates')
            sys_exit()

    # check if provided excluded fields are in range of fields count, also check for
    # duplicates and empty fields
    if settings_dict['excluded_fields'] != '':
        if check_key_overlimit(settings_dict['excluded_fields']):
            timestamp_output('(ERROR) provided excluded_fields out of number_of_fields, '
                  'has empty keys or key numbers has duplicates')
            sys_exit()

    # check if record_field_sizes is provided in case of delimiter_type = fixed
    if settings_dict['delimiter_type'] == 'fixed' and settings_dict['record_field_sizes'] == '':
        timestamp_output('Error: record_field_sizes must be provided if delimiter_type = fixed')
        sys_exit()

    # check if count of provided record_field_sizes is equals to number_of_fields
    if settings_dict['delimiter_type'] == 'fixed' and settings_dict['record_field_sizes'] != '':
        if len(settings_dict['record_field_sizes'].split(',')) != settings_dict['number_of_fields']:
            timestamp_output('(ERROR) record_field_sizes count must be equal to number_of_fields')
            sys_exit()
        else:
            #for i in settings_dict['record_field_sizes'].replace(' ', '').split(','):
            for i in [x.strip() for x in settings_dict['record_field_sizes'].split(',')]:
                if i == '':
                    timestamp_output('(ERROR) some of record_field_sizes are empty')
                    sys_exit()

    # check if record_field_names has empty names
    for i in [x.strip() for x in settings_dict['record_field_names'].split(',')]:
        if i == '':
            timestamp_output('(ERROR) some of record_field_names are empty')
            sys_exit()

    # check if record_field_names count is equals to number_of_fields
    if len(settings_dict['record_field_names'].split(',')) != settings_dict['number_of_fields']:
        timestamp_output('(ERROR) record_field_names count must be equal to number_of_fields')
        sys_exit()


def get_file_list(path: str, mask: str) -> list:
    """get files list, check existence, also determine if is file or is dir
    Args:
        path (string): path to list
        mask (string): files search wildcard
    Returns:
        list: list of found files"""
    if os_path.exists(path) and os_path.isfile(path):
        return [path]
    if os_path.exists(path) and os_path.isdir(path):
        res = glob(os_path.join(path, mask))
        res = [f for f in res if os_path.isfile(f)]
        if res:
            return res
        else:
            timestamp_output(f"(ERROR) no files found at {path} with mask {mask}")
            sys_exit()
    else:
        timestamp_output(f"(ERROR) no files found at {path} with mask {mask} "
                         f"or path does not exists")
        sys_exit()


def regex_rename(name: str) -> str:
    """returns diff files main name part
    Args:
        name (string): filename
    Returns:
        string: a part of filename wo prefix such as 'old_'     
    Except:
        string: err msg, exit"""
    try:
        return ''.join(re_findall(settings_dict['files_rename_regex'], name))
    except IndexError:
        timestamp_output('error regex in ' + name)
        sys_exit()


def get_file_pairs(etls_list: list, srcs_list: list) -> list:
    """get a diff pair of files, check lost and extra files
    Args:
        etls_list (list): a list of old sys files
        srcs_list (list): a list of new sys files
    Returns:
        list: [res, extra_etalons, extra_sources]
            res - dict, a dict of file pairs fo diff, key - name of old sys file, 
            value - name of new sys file
            extra_etalons - list, represents files only from old sys, no same files from new sys
            extra_sources - list, represents files only from new sys, no same files from old sys"""
    res = {}
    renamed_etls_list = []
    renamed_srcs_list = []

    timestamp_output(f"<<< checking {settings_dict['legacy_system_name']} files.. "
                     f"( using {settings_dict['etalons']})")
    for i in sorted(etls_list):
        renamed_etls_list.append(regex_rename(os_path.split(i)[1]))
    timestamp_output(f">>> found '{str(len(renamed_etls_list))} "
                     f"{settings_dict['legacy_system_name']} generated files")

    timestamp_output(f"<<< checking {settings_dict['new_system_name']} files.. "
                     f"( using '{settings_dict['sources']})")
    for i in sorted(srcs_list):
        renamed_srcs_list.append(regex_rename(os_path.split(i)[1]))
    timestamp_output(f">>> found {len(renamed_srcs_list)} "
                     f"{settings_dict['new_system_name']} generated files")

    renamed_etls_set = set(renamed_etls_list)
    renamed_srcs_set = set(renamed_srcs_list)
    matched = sorted(list(renamed_etls_set & renamed_srcs_set))

    timestamp_output('<<< checking lost and extra files..')
    extra_etalons = sorted(list(renamed_etls_set - renamed_srcs_set))
    if extra_etalons:
        timestamp_output(f">>> files only in {settings_dict['legacy_system_name']} "
                         f"system (losts): {extra_etalons}")
    extra_sources = sorted(list(renamed_srcs_set - renamed_etls_set))
    if extra_sources:
        timestamp_output(f">>> files only in {settings_dict['new_system_name']} "
                         f"system (extra): {extra_sources}")
    if not extra_etalons and not extra_sources:
        timestamp_output(f">>> {settings_dict['legacy_system_name']} and "
                         f"{settings_dict['new_system_name']} system file lists are equal")

    timestamp_output(f">>> {len(matched)} file pairs found")

    for i in matched:
        res[f"{settings_dict['etalons']}/etl_{i}"] = f"{settings_dict['sources']}/src_{i}"
    return [res, extra_etalons, extra_sources]


def line_to_record(record: str) -> list:
    """splits line to fields according cdr options
    Args:
        record (string): record as raw line from file
    Returns:
        list: a list of record fields"""
    if settings_dict['delimiter_type'] == 'char':
        return record.replace('\n', '').split(settings_dict['delimiter'])
    elif settings_dict['delimiter_type'] == 'fixed':
        # generating regex for record split according record_field_sizes
        regex_str = '^'
        for i in settings_dict['record_field_sizes'].split(','):
            regex_str += '(.{' + i + '})'
        if settings_dict['record_field_sizes_tail']:
            regex_str += '(.+)'
        regex_str += '\n'

        if re_fullmatch(regex_str, record):
            return [x.strip() for x in re_split(regex_str, record)[1:-1]]


def get_records_from_file(filename: str, is_from_oldsys: bool) -> list:
    """get lines from files, separate header, trailer from records, also
       split lines into fields according settings
    Args:
        filename (string): filename
        is_from_oldsys (bool):
            True - means file is from old sys
            False - means file is from new sys
    Returns:
        list: [records_dict, heder_list, trailer_list]
            records_dict - dict, a dict of lines from file transformed into list of fields
                key - concatinated key field values
                value - record line splitted into fields
            header_list - list, a list of header lines as is
            trailer_list - list, a list of trailer lines as is"""
    records_dict = {}
    header_list = []
    trailer_list = []
    encoding = ''
    header_lines_num = settings_dict['header_records_number']
    trailer_lines_num = settings_dict['trailer_records_number']
    header_indexes = []
    trailer_indexes = []
    file_total_records = 0

    if is_from_oldsys:
        encoding = settings_dict['encode_etl']
    else:
        encoding = settings_dict['encode_src']

    # calculating indexes of header lines
    if header_lines_num > 0:
        for i in range(header_lines_num):
            header_indexes.append(i)

    # calculating indexes of trailer lines
    if trailer_lines_num > 0:
        last_index = raw_lines_count_by_file[filename] - 1
        for i in range(trailer_lines_num):
            trailer_list.append(last_index)
            last_index =- 1

    # processing records
    with open(filename, mode='r', encoding=encoding) as f:
        for n, line in enumerate(f):
            if n in header_indexes:
                header_list.append(line)
                continue
            elif n in trailer_indexes:
                trailer_list.append(line)
                continue
            else:
                rec = line_to_record(line)
                if settings_dict['diff_keys'] == '':
                    key = str(n)
                else:
                    key = cf.key_composer(rec, settings_dict['diff_keys'])

                if len(rec) != settings_dict['number_of_fields']:
                    timestamp_output('>>> defective record!')

                if key not in records_dict:
                    records_dict[key] = rec
                    file_total_records += 1
                else:
                    if is_from_oldsys:
                        stats_list[7] += 1 # add to oldsys repeats
                    else:
                        stats_list[8] += 1 # add to newsys repeats
    return [records_dict, header_list, trailer_list, file_total_records]


def get_prior_keys_list(lines1: list) -> list:
    """get prioritized keys list
    Args:
        lines1 (list): a list of records
    Returns:
        list: a list of field numbers, wich can be used as diff keys, sorted by unique.
        More unique fields are placed in the beginning. non unique fields will not be added"""
    field_sets_dict = {}
    field_lengths_dict = {}
    prior_keys_list = []
    # creating a dict: key - field number, value - field value
    # keep a set of values for each field number
    for line in lines1:
        for n, field in enumerate(line):
            if n not in field_sets_dict:
                field_sets_dict[n] = set()
                field_sets_dict[n].add(field)
            else:
                field_sets_dict[n].add(field)

    # creating a dict: key - length of set, value - field number
    # keep number of fields, combined together, according length of their field value sets
    for k, v in field_sets_dict.items():
        set_len = len(v)
        if set_len not in field_lengths_dict:
            field_lengths_dict[set_len] = [k]
        else:
            field_lengths_dict[set_len].append(k)

    # creating a list of field numbers, sorted by unique, keys with same unique appends
    # one by one as it goes
    for k, v in sorted(field_lengths_dict.items(), reverse=True):
        if k == 1:
            continue
        for item in v:
            prior_keys_list.append(item)

    return prior_keys_list


def get_unique_keys(lines: list, keys: list, excluded_keys: list) -> list:
    """get keys wich combination gives absolutely unique field values merge
    Args:
        lines (list): a list of record lines
        keys (list): a list of diff keys
        excluded_keys (list): a list of excluded keys
    Returns:
        list: a list of keys, wich gives a 100% unique combination of 
        concatinated fiels values. Returns an empty list if unique combination is impossible"""
    if not keys:
        return []
    left_keys = []
    for key in keys:
        if key not in excluded_keys:
            left_keys.append(key)

    for i in range(len(left_keys)):
        res = left_keys[:i + 1]
        test_set = set()
        for line in lines:
            tmp = ''
            for j in res:
                tmp += line[j]
            test_set.add(tmp)
        if len(test_set) == len(lines):
            return res
    return []


def get_diff_keys(lines: list) -> list:
    """returns a list of auto found diff keys
    Args:
        lines (list): a list of record lines
    Returns:
        list: a list of keys, wich gives a 100% unique combination of 
        concatinated fiels values
        This function is literally a launcher for get_unique_keys function"""
    excluded_keys = [x.strip() for x in str(settings_dict['excluded_diff_keys']).split(',')]
    return get_unique_keys(lines, get_prior_keys_list(lines), excluded_keys)


def save_found_keys(name: str, keys: str) -> None:
    """create a config file with found diff keys
    Args:
        name (string): filename
        keys (string): a list of keys as string
    Returns:
        None: performs save found keys to file"""
    keys = [str(item) for item in keys]
    str_keys = ','.join(keys)
    config_keys.add_section('auto_generated_keys')
    config_keys.set('auto_generated_keys', 'keys', str_keys)
    with open(name, 'w', encoding='utf-8') as f:
        config_keys.write(f)


def check_key_overlimit(key_str: str) -> bool:
    """check if keys values are in range, without duplicates and empty fields
    Args:
        key_str (string): a list of keys as stting
    Returns:
        bool: True - means keys are overlimit or empty or has duplicates
              False - means keys are ok to use"""
    if key_str == '':
        return False
    tmp_list = key_str.replace(' ', '').split(',')
    if '' in tmp_list:
        return True
    if int(max(tmp_list)) >= settings_dict['number_of_fields']:
        return True
    if len(tmp_list) != len(set(tmp_list)):
        return True
    return False


def stat_out(stats_lst: list, start_t: float, field_names: list) -> None:
    """printing stats to console
    Args:
        stats_lst (list): a list of values
        start_t (float): a script start time
        fiels_names (list): a list of record field names
    Returns:
        None: performs a console print of detailed, tabulated stats
        This function also performs finishing operations on diff complete or user interrupt"""

    while not thread_break_flag:
        cf.clear_screen()
        print()
        print(f"Diff tag : {settings_dict['diff_name']} │ ", end='')
        print('Time left: ', end='')
        print(f"{cf.calculate_estimated_time(stats_list[16], stats_list[2], start_t)} │ ", end='')
        print(f"{stats_list[15]} │", end='')
        print('Press Ctrl+C once to interrupt diff!')
        print('─' * 148)
        cf.progress_bar(stats_lst[1], stats_lst[0], 'Files:',
                     f"Keys = {settings_dict['diff_keys']}",
                     f"Excluded keys = {settings_dict['excluded_diff_keys']}",
                     f"Excluded fields = {settings_dict['excluded_fields']}")
        print()
        print('─' * 148)
        cf.progress_bar(stats_lst[12], stats_lst[11], 'Diff pair records:',
                     f"Diff pair = {stats_list[13]}")
        print()
        print()
        report_print(stats_list, False, field_names)
        time.sleep(3) # stat refresh delay
    if interrupt_flag:
        stats_list[15] = 'Diff process interrupted by user'
    else:
        stats_list[15] = 'Diff process complete'
    cf.clear_screen()
    print()
    print(f"Diff tag : {settings_dict['diff_name']} │ ", end='')
    print('Time left: ', end='')
    print(f"{cf.calculate_estimated_time(stats_list[1], stats_list[0], start_t)} │ ", end='')
    print(f"{stats_list[15]}")
    print('─' * 148)
    cf.progress_bar(stats_lst[1], stats_lst[0], 'Files:',
                 f"Keys = {settings_dict['diff_keys']}",
                 f"Excluded keys = {settings_dict['excluded_diff_keys']}",
                 f"Excluded fields = {settings_dict['excluded_fields']}")
    print()
    print('─' * 148)
    cf.progress_bar(stats_lst[11], stats_lst[11], 'Diff pair records',
                 f"Diff pair = {stats_list[13]}")
    print()
    print()
    report_print(stats_list, True, field_names)

    if interrupt_flag:
        timestamp_output('(STOP) Interrupted by user')
    else:
        timestamp_output('(INFO) Diff process complete.')
    time_spent = round(time.time() - start_t, 4)
    timestamp_output(f"(INFO) Total diff time: "
                     f"{time.strftime('%H:%M:%S', time.gmtime(time_spent))}")

    # closing report files
    timestamp_output('<<< closing report files')
    for i in errored_fields_set:
        rep_file_name = ''
        if i == 'defective':
            rep_file_name = (f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}/"
                                    f"!!!_defective.rep")
        else:
            rep_file_name = (f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}/"
                                 f"{str(i).zfill(3)}_{field_names[i]}.rep")
        rep_file = open(rep_file_name, 'a+', encoding='utf-8')
        rep_file.close()
    timestamp_output('>>> closing report files done')

    timestamp_output('<<< moving log file')
    timestamp_output('<<< stopping script')

    # closing log file
    logfile.close()

    # moving logfile to result dir
    move(f"{LOGFILE_NAME}", f"{settings_dict['diff_out_dir']}{settings_dict['diff_name']}")


def report_print(values_list: list, is_total: bool, field_names: list) -> None:
    """Printing diff report
    Args:
        values_list (list): stats list to output
        is_total (bool): 
            True - output a report table, styled for TOTAL with header, double lines border
            False - output a report table without header, single line border
        field_names (list): a list of record field names
    Returns:
        None: perform a console print of report table"""

    # Records in IUM,Records in PiOne,Total matched records,Lost records,
    # Extra records,IUM records repeat,
    # PiOne records repeat,Broken attributes,Identical records

    if is_total:
        # print double line header with TOTAL
        print(f"╒{'═' * 25}╤{'═' * 24}╤{'═' * 21}╤[ TOTAL ]{'═' * 12}╤", end='')
        print(f"{'═' * 29}╤{'═' * 21}╕")
    else:
        # print single line header without TOTAL
        print(f"┌{'─' * 25}┬{'─' * 24}┬{'─' * 21}┬{'─' * 21}┬{'─' * 29}┬", end='')
        print(f"{'─' * 21}┐")
    # print column names
    print(f"│ {'Total records':23s} │ ", end='')
    print(f"{'Unmatched/Matched recs':22s} │ ", end='')
    print(f"{'Lost/Extra records':19s} │ ", end='')
    print(f"{'Record repeats':19s} │ ", end='')
    print(f"{'Defective/Broken records':27s} │ ", end='')
    print(f"{'Identical records':19s} │ ")

    # print values
    ## 1st row
    # OldSys recs
    print(f"│ {settings_dict['legacy_system_name'][:10]:10}: {values_list[2]:>11} │ ",
          end='')
    #print(f"{'':22} │ ", end='') # blank for matched column
    print(cf.get_val_with_percents(values_list[2], (values_list[5] + values_list[6] +
                          values_list[7] + values_list[8]), '>22'), '│', end='')
    # Lost recs (%)
    print(cf.get_val_with_percents(values_list[2], values_list[5], '>20'), '│', end='')
    # OldSys repeats (%)
    print(cf.get_val_with_percents(values_list[2], values_list[7], '>20'), '│', end='')
    # Defective recs (%)
    print(cf.get_val_with_percents(values_list[2], values_list[14], '>28'), '│', end='')
    print(f"{'':20} │ ", end='') # blank for identical column
    print()

    ## 2nd row
    # NewSys recs
    print(f"│ {settings_dict['new_system_name'][:10]:10}: {values_list[3]:>11} │ ", end='')
    # Matched (%)
    print(cf.get_val_with_percents(values_list[2], values_list[4], '>22'), '│', end='')
    # Extra recs (%)
    print(cf.get_val_with_percents(values_list[2], values_list[6], '>20'), '│', end='')
    # NewSys repeats (%)
    print(cf.get_val_with_percents(values_list[2], values_list[8], '>20'), '│', end='')
    # Broken attrs (%)
    # print(cf.get_val_with_percents(values_list[2], values_list[9], '>28'), '│', end='')
    print(cf.get_val_with_percents(values_list[2], values_list[17], '>28'), '│', end='')
    # Identical resc (%)
    print(cf.get_val_with_percents(values_list[2], values_list[10], '>20'), '│', end='')
    print()

    # Optional bottom double line for 'TOTAL'
    if is_total:
        print(f"╘{'═' * 25}╧{'═' * 24}╧{'═' * 21}╧{'═' * 21}╧{'═' * 29}", end='')
        print(f"╧{'═' * 21}╛")
    else:
        print(f"└{'─' * 25}┴{'─' * 24}┴{'─' * 21}┴{'─' * 21}┴{'─' * 29}", end='')
        print(f"┴{'─' * 21}┘")

    if fields_error_dict.keys():
        print(f"{'Field errors':<39}: {stats_list[9]}")
        print('─' * 45)
        for k, v in sorted(fields_error_dict.items()):
            print(f"{k:>3} {field_names[k]:<35}: {v}")
        print()


def diff(file_pairs: dict, field_names: list) -> None:
    """diff func
    Agrs:
        file_pairs (dict): a dict of file pairs to diff
            Key - name of old sys file
            Value - name of new sys file
        field_names (list): a list of record field names
    Returns:
        None: performs all diff processes"""
    global thread_break_flag
    err_threshold_to_write = 0 # for faster err report write
    # write settings report
    report_file_name = (f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}/"
                       f"diff_settings.rep")
    report_file = open(report_file_name, 'a+', encoding='utf-8')

    for k, v in settings_dict.items():
        if v == '\t':
            report_file.write(f"{k}: \\t\n")
        else:
            report_file.write(f"{k}: {v}\n")
    report_file.close()

    stats_list[0] = len(file_pairs) # for total files stat

    # start diff processing
    detailed_errors_dict = {}
    for k, v in file_pairs.items():
        if thread_break_flag:
            break

        stats_list[11] = 0
        stats_list[12] = 0
        tmp_headers = []
        tmp_tralers = []

        # reset curr file rec stats
        for n, i in enumerate(stats_list_curr_file):
            if isinstance(i, int):
                stats_list_curr_file[n] = 0
            elif isinstance(i, str):
                stats_list_curr_file[n] = ''

        # current pair filenames to stat
        stats_list[13] = f"{k.split('/')[-1]} / {v.split('/')[-1]}"
        stats_list_curr_file[13] = stats_list[13]

        # get oldsys records
        stats_list[15] = f"reading {settings_dict['legacy_system_name']} records.."
        oldsys_records, tmp_headers, tmp_tralers, file_records_num = get_records_from_file(k, True)
        stats_list[11] = file_records_num # current file records number
        stats_list_curr_file[2] = stats_list[11] # oldsys curr file total recs

        # get newsys records
        stats_list[15] = f"reading {settings_dict['new_system_name']} records.."
        # timestamp_output(f"<<< reading {settings_dict['new_system_name']} records)")
        newsys_records, tmp_headers, tmp_tralers, file_records_num = get_records_from_file(v, False)
        stats_list_curr_file[3] = file_records_num # newsys curr file total recs

        # timestamp_output(f">>> reading {settings_dict['new_system_name']} records) done")
        tmp_headers = []
        tmp_tralers = []

        # count matched, lost, extra records
        stats_list[15] = 'calculating matched, lost, extra records..'
        matched_recs_by_key = oldsys_records.keys() & newsys_records.keys()
        lost_recs = oldsys_records.keys() - newsys_records.keys()
        extra_recs = newsys_records.keys() - oldsys_records.keys()
        stats_list[4] += len(matched_recs_by_key) # to matched out
        stats_list_curr_file[4] = len(matched_recs_by_key)
        stats_list[5] += len(lost_recs) # to losts out
        stats_list_curr_file[5] = len(lost_recs) # to losts out
        stats_list[6] += len(extra_recs) # to extras out
        stats_list_curr_file[6] = len(extra_recs) # to extras out

        # write reports for losts and extras
        stats_list[15] = 'writing lost and extra reports..'
        if lost_recs:
            rep_file_name = (f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}"
                             f"/!!_losts.rep")
            for i in lost_recs:
                rep_file = open(rep_file_name, 'a+', encoding='utf-8')
                rep_str = f"{stats_list[13]}:\n{oldsys_records[i]}\n\n"
                rep_file.write(rep_str)
        if extra_recs:
            rep_file_name = (f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}"
                             f"/!_extras.rep")
            for i in extra_recs:
                rep_file = open(rep_file_name, 'a+', encoding='utf-8')
                rep_str = f"{stats_list[13]}:\n{newsys_records[i]}\n\n"
                rep_file.write(rep_str)

        # diff records
        stats_list[15] = 'processing diff..'
        for i in matched_recs_by_key:
            if thread_break_flag:
                break
            res = compare_records(oldsys_records[i], newsys_records[i])
            if res[0]:
                stats_list[17] += 1 # add total broken record
                stats_list_curr_file[17] += 1 # add current broken record

            # write error reports
            for k, v in res[0].items():
                errored_fields_set.add(k) # for closing report files
                rep_file_name = (f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}/"
                                f"{str(k).zfill(3)}_{field_names[k]}.rep")
                if rep_file_name not in detailed_errors_dict:
                    detailed_errors_dict[rep_file_name] = [v]
                    err_threshold_to_write += 1
                else:
                    detailed_errors_dict[rep_file_name].append(v)
                    err_threshold_to_write += 1

            # check err_threshold_to_write, dump data to report if needed
            if err_threshold_to_write > 5000:
                for k, v in detailed_errors_dict.items():
                    cf.write_to_file(k, v)
                detailed_errors_dict.clear()
                err_threshold_to_write = 0

            # write defective reports
            if res[1]:
                errored_fields_set.add('defective') # for closing defective report file
                rep_file_name = (f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}/"
                                    f"!!!_defective.rep")
                rep_file = open(rep_file_name, 'a+', encoding='utf-8')
                for j in res[1]:
                    rep_file.write(j)
            stats_list[12] += 1 # current record
            stats_list[16] += 1 # curr rec for estimated

        # final write error data from detailed_errors_dict to report
        for k, v in detailed_errors_dict.items():
            cf.write_to_file(k, v)
        detailed_errors_dict.clear()
        stats_list[1] += 1 # current file pair number

        # write current file info
        curr_file_stats = '\t'.join([
                # filename
                f"{stats_list_curr_file[13]:<26}",
                # old sys records
                f"{stats_list_curr_file[2]:<20}",
                # new sys records
                f"{stats_list_curr_file[3]:<20}",
                # Matched records
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[4], '<20'),
                # Lost records
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[5], '<20'),
                # Extra records
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[6], '<20'),
                # old sys repeats
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[7], '<20'),
                # new sys repeats
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[8], '<20'),
                # broren records
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[17], '<20'),
                # broken attributes
                f"{stats_list_curr_file[9]:<20}",
                # defective records
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[14], '<20'),
                # identical records
                cf.get_val_with_percents(stats_list_curr_file[2], stats_list_curr_file[10], '<20')])
        cf.write_to_file(f"{settings_dict['diff_out_dir']}"
                        f"/{settings_dict['diff_name']}/diff_result.txt", [curr_file_stats + '\n'])

    # write top 10 field errors
    rep_file_name = (f"{settings_dict['diff_out_dir']}"
                     f"/{settings_dict['diff_name']}/diff_top_field_errors.txt")
    rep_file = open(rep_file_name, 'a+', encoding='utf-8')
    for k, v in sorted(diff_pattern_dict.items()):
        n_cnt = 0
        rep_file.write('\n')
        rep_file.write(f"{k} {field_names[k]}:\n")
        for k1, v1 in sorted(v.items(), key=lambda item: item[1], reverse=True ):
            if n_cnt == 10:
                break
            rep_file.write(f"{k1:<40} : {v1}\n")
            n_cnt += 1

    # put column names before total stats in diff_result.txt
    cf.write_to_file(f"{settings_dict['diff_out_dir']}" \
                  f"/{settings_dict['diff_name']}/diff_result.txt", '\n' + REPORT_HEADER)
    # write TOTAL info
    final_stats = '\t'.join([
                   # filename
                   f"{'TOTAL':<26}",
                   # total old sys records
                   f"{stats_list[2]:<20}",
                   # total new sys records
                   f"{stats_list[3]:<20}",
                   # total matched records
                   cf.get_val_with_percents(stats_list[2], stats_list[4], '<20'),
                   # total lost records
                   cf.get_val_with_percents(stats_list[2], stats_list[5], '<20'),
                   # total extra records
                   cf.get_val_with_percents(stats_list[2], stats_list[6], '<20'),
                   # total old sys repeats
                   cf.get_val_with_percents(stats_list[2], stats_list[7], '<20'),
                   # total new sys repeats
                   cf.get_val_with_percents(stats_list[2], stats_list[8], '<20'),
                   # total broken records
                   cf.get_val_with_percents(stats_list[2], stats_list[17], '<20'),
                   # total broken attributes
                   f"{stats_list[9]:<20}",
                   # total defective records
                   cf.get_val_with_percents(stats_list[2], stats_list[14], '<20'),
                   # total identical records
                   cf.get_val_with_percents(stats_list[2], stats_list[10], '<20')])
    cf.write_to_file(f"{settings_dict['diff_out_dir']}"
                  f"/{settings_dict['diff_name']}/diff_result.txt", final_stats + '\n')


def compare_records(etl_rec: list, src_rec: list) -> list:
    """check number of fields, compare values
    Args:
        etl_rec (list): an old sys records list
        src_rec (list): a new sys records list
    Returns:
        list: [res, defective]
            res (dict): field errors
                key - field number
                value - an f-string, error info strings for report files to write
            defective (list): a list of defective records"""
    matched = 0
    broken = 0
    res = {}
    defective = []

    if (len(etl_rec) == settings_dict['number_of_fields'] and
        len(src_rec) == settings_dict['number_of_fields']):
        for i in range(settings_dict['number_of_fields']):
            if str(i) in settings_dict['excluded_fields']:
                continue
            # if fields are not matched
            if etl_rec[i] != src_rec[i]:
                stats_list[9] += 1 # add broken stat
                stats_list_curr_file[9] += 1 # add current broken stat
                broken += 1
                if i not in fields_error_dict:
                    fields_error_dict[i] = 1
                else:
                    fields_error_dict[i] += 1

                diff_pattern = cf.deep_diff(etl_rec[i], src_rec[i])
                if i not in diff_pattern_dict:
                    diff_pattern_dict[i] = {}
                    diff_pattern_dict[i][diff_pattern] = 1
                else:
                    if diff_pattern not in diff_pattern_dict[i]:
                        diff_pattern_dict[i][diff_pattern] = 1
                    else:
                        diff_pattern_dict[i][diff_pattern] += 1

                report_str = (
                f"{stats_list[13]} :: [{etl_rec[i]}]:[{src_rec[i]}] :: "
                f"({diff_pattern})\n"
                f"{settings_dict['legacy_system_name']:10}: {etl_rec}\n"
                f"{settings_dict['new_system_name']:10}: {src_rec}\n\n")
                res[i] = report_str
            else:
                matched += 1

        if broken == 0:
            stats_list[10] += 1 # add identical
            stats_list_curr_file[10] += 1 # add identical curr file stats

    else:
        if len(etl_rec) != settings_dict['number_of_fields']:
            timestamp_output('(ERROR) Legacy records has incorrect number of fields:')
            timestamp_output(f"Legacy record: {len(etl_rec)}, "
                             f"expected: {settings_dict['number_of_fields']}")
            timestamp_output(f"filename: {stats_list[13].split('/', maxsplit=1)[0]}")
            timestamp_output(f"defective record: {etl_rec}")
            report_str = f"{stats_list[13].split('/', maxsplit=1)[0]}\n{etl_rec}\n"
            defective.append(report_str)

        if len(src_rec) != settings_dict['number_of_fields']:
            timestamp_output('(ERROR) NewSys records has incorrect number of fields:')
            timestamp_output(f"NewSys record: {len(src_rec)}, "
                             f"expected: {settings_dict['number_of_fields']}")
            timestamp_output(f"filename: {stats_list[13].split('/')[-1]}")
            timestamp_output(f"defective record: {src_rec}")
            report_str = f"{stats_list[13].split('/', maxsplit=1)[-1]}\n{src_rec}\n"
            defective.append(report_str)
        stats_list[14] += 1 # add defective to total
        stats_list_curr_file[14] += 1 # add defective to curr file stats
    return [res, defective]


def main_f():
    """main func
    Args:
        None
    Returns:
        None"""

    ## starting
    timestamp_output('>>> starting script')

    timestamp_output('<<< reading settings')
    if os_path.exists(SCRIPT_PATH + 'compare_settings.ini'):
        config.read(SCRIPT_PATH + 'compare_settings.ini')
    else:
        timestamp_output(f"configuration file is not found at {SCRIPT_PATH}")
        sys_exit()

    ## check and init settings
    init_settings()
    timestamp_output('>>> reading settings done')

    # update diff name tag, add time_prefix
    settings_dict['diff_name'] = f"{diff_tag_prefix}_{settings_dict['diff_name']}"

    # create result dir
    os_makedirs(f"{settings_dict['diff_out_dir']}/{settings_dict['diff_name']}", exist_ok=True)

    # split field names string for lifestat field err report
    settings_dict['record_field_names'] = settings_dict['record_field_names'].replace(' ', '')
    field_names_list = settings_dict['record_field_names'].split(',')

    # check and init files
    timestamp_output('<<< listing files')
    etalon_files = get_file_list(settings_dict['etalons'], settings_dict['files_mask_etalon'])
    source_files = get_file_list(settings_dict['sources'], settings_dict['files_mask_source'])
    timestamp_output('>>> listing files done')

    # get file pairs
    timestamp_output('<<< getting file pairs')
    pairs = get_file_pairs(etalon_files, source_files)
    timestamp_output('>>> getting file pairs done')

    # count total lines, lines for each file
    timestamp_output(f"<<< counting {settings_dict['legacy_system_name']} and "
                     f"{settings_dict['new_system_name']} record lines")
    for k, v in pairs[0].items():
        global legacy_total_lines
        global newsys_total_lines
        with open(k, 'r', encoding=settings_dict['encode_etl']) as leg_f:
            n = 0
            for n, _ in enumerate(leg_f):
                pass
            legacy_total_lines += (n + 1) - (settings_dict['header_records_number'] +
                                             settings_dict['trailer_records_number'])
            raw_lines_count_by_file[k] = n + 1
        with open(v, 'r', encoding=settings_dict['encode_src']) as new_f:
            for n, _ in enumerate(new_f):
                pass
            newsys_total_lines += (1 + n) - (settings_dict['header_records_number'] +
                                             settings_dict['trailer_records_number'])
            raw_lines_count_by_file[v] = n + 1

    timestamp_output(f">>> legacy_record_lines: {legacy_total_lines}, "
                     f"newsys_record_lines: {newsys_total_lines}")

    # for livestat Total records
    stats_list[2] = legacy_total_lines
    stats_list[3] = newsys_total_lines

    # get best diff keys or use previously found
    timestamp_output('<<< checking diff keys')
    if settings_dict['diff_keys_type'] == 'auto':
        if (settings_dict['auto_keys_search_each_run'] or
        not os_path.exists(f"{SCRIPT_PATH}auto_generated_keys.ini")):
            timestamp_output(">>> diff keys aren't defined yet or auto_keys_search_each_run = True")
            timestamp_output(f"<<< browsing {settings_dict['diff_keys_search_count']}"
                                ' lines from etalon samples..')   
            test_records_list = []
            break_flag = False

            for file in etalon_files:
                for line in get_records_from_file(file, True)[0].values():
                    # defective records cutoff
                    if len(line) != settings_dict['number_of_fields']:
                        print('defective')
                        continue
                    #records count limiter
                    if len(test_records_list) == settings_dict['diff_keys_search_count']:
                        break_flag = True
                        break
                    else:
                        test_records_list.append(line)
                if break_flag:
                    break
            timestamp_output(f">>> got {len(test_records_list)} lines from etalon samples")
            timestamp_output('<<< determine best diff keys..')

            if len(test_records_list) < 10:
                timestamp_output('(ERROR) to few etalon records for auto keys find. '
                                'use manual keys or add etalons, stopping script')
                sys_exit()
            diff_keys = get_diff_keys(test_records_list)
            if diff_keys:
                settings_dict['diff_keys'] = diff_keys
                timestamp_output(f">>> found diff keys: {diff_keys}")
                timestamp_output('<<< saving new keys in auto_generated_keys.ini')
                save_found_keys(SCRIPT_PATH + '/auto_generated_keys.ini',
                                settings_dict['diff_keys'])
                timestamp_output('>>> saving new keys done')
            else:
                timestamp_output('(ERROR) auto diff keys search failed, stopping script')
                sys_exit()
        #elif (not settings_dict['auto_keys_search_each_run']
        #    and os_path.exists(f"{SCRIPT_PATH}auto_generated_keys.ini")):
        else:
            timestamp_output('<<< applying previously found diff keys '
                                'from auto_generated_keys.ini')

            timestamp_output('(INFO) remove auto_generated_keys.ini file '
                                'if you need another auto diff keys detection!'
                                'Or use auto_keys_search_each_run')
            try:
                config_keys.read(f"{SCRIPT_PATH}auto_generated_keys.ini")
                settings_dict['diff_keys'] = config_keys.get('auto_generated_keys', 'keys')
                timestamp_output(f">>> current diff keys: {list(settings_dict['diff_keys'])}")
            except configparser.MissingSectionHeaderError:
                timestamp_output("(ERROR) cant get keys from auto_generated_keys.ini,"
                                " stopping script")
                sys_exit()
            except configparser.NoSectionError:
                timestamp_output("(ERROR) section 'auto_generated_keys' is not found in "
                                "auto_generated_keys.ini, stopping script")
                sys_exit()
    else:
        timestamp_output(f">>> diff keys are manually set to: {settings_dict['diff_keys']}")

    # compare file pairs and generate field reports
    timestamp_output('>>> starting diff..')

    th1 = Thread(target=stat_out, args=(stats_list, start_time, field_names_list))
    th2 = Thread(target=diff, args=(pairs[0], field_names_list, ))

    # add REPORT_HEADER (colum names) to main report
    cf.write_to_file(f"{settings_dict['diff_out_dir']}"
                  f"/{settings_dict['diff_name']}/diff_result.txt", [REPORT_HEADER])

    # starting diff processing thread

    th2.start()
    timestamp_output('<<< processing diff')
    # delay for log view, also need to prevent /0 for estimated time
    time.sleep(3)

    # starting stat output thread
    th1.start()

    #threads stopper
    while th2.is_alive():
        global thread_break_flag
        global interrupt_flag
        try:
            time.sleep(2)
        except KeyboardInterrupt:
            stats_list[15] = 'stopping script..'
            thread_break_flag = True
            interrupt_flag = True
            th1.join()
            th2.join()
            sys_exit()
    thread_break_flag = True # for proper stat thread stop after diff thread stops


    # output final stat processed in stat_out

    ## End
    # finishing tasks processed in stat_out

if __name__ == "__main__":
    main_f()
