import os
import requests


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    filepath = os.path.join(os.path.expanduser('./'), 'titanic.csv')
    with open(filepath, 'w', encoding='utf-8') as f:
        for chunk in response.iter_lines():
            f.write('{}\n'.format(chunk.decode('utf-8')))
    return filepath


def get_number_of_lines(file_path):
    lines = 0
    with open(file_path) as f:
        for line in f:
            if line:
                print(line)
                lines += 1
    return lines


def print_data():
    from csv import reader, DictReader
    with open('./titanic.csv', 'r') as readObj:
        csvReader = reader(readObj)
        for num, row in enumerate(csvReader):
            if row and num != 0:
                row[2] = row[2].replace('\'', "")
                # row variable is a list that represents a row in csv
                print(row)
                    # client.execute("INSERT INTO titanic.data FORMAT CSV", row)


filepath = download_titanic_dataset()
# get_number_of_lines(filepath)
print_data()
