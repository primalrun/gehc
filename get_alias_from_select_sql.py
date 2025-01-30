file_in = r'c:\temp\select.txt'

with open(file_in, 'r') as fr:
    lines_raw = fr.readlines()


def find_non_alphanumeric_string(str_p):
    for i, char in enumerate(str_p):
        if not char.isalnum():
            return i
    return -1


def clean_line(lines_p):
    lines_clean_1 = [x.strip() for x in lines_p if '.' in x]
    lines_clean_2 = [x.replace(',', '') for x in lines_clean_1]
    lines_clean_3 = [x.replace('-', '') for x in lines_clean_2]

    lines_clean_4 = []
    for x in lines_clean_3:
        string_pre_period = x.split('.')[0]

        if string_pre_period.isalnum() is False:
            lines_clean_4.append(string_pre_period[find_non_alphanumeric_string(string_pre_period) + 1:])
        else:
            lines_clean_4.append(string_pre_period)

    unique_alias = set(lines_clean_4)

    return unique_alias


lines_clean = clean_line(lines_raw)
print(lines_clean)



