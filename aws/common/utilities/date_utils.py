from datetime import datetime


def parse_date_multiple_formats(date_string):
    # List of possible date formats to try
    date_formats = [
        '%d.%m.%Y',  # 26.03.2021
        '%d/%m/%Y',  # 26/03/2021
        '%Y-%m-%d',  # 2021-03-26
        '%m/%d/%Y',  # 03/26/2021
        '%Y/%m/%d',  # 2021/03/26
        '%d-%m-%Y',  # 26-03-2021
        '%Y.%m.%d',  # 2021.03.26
        '%d %b %Y',  # 26 Mar 2021
        '%d %B %Y',  # 26 March 2021
        '%Y%m%d',  # 20210326
    ]

    for date_format in date_formats:
        try:
            return datetime.strptime(date_string, date_format)
        except ValueError:
            continue

    # TODO: Temp just for now
    return datetime.now()
    # If none of the formats worked
    # raise ValueError(f"Date string '{date_string}' does not match any of the expected formats")
