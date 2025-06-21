import argparse
import csv
from tabulate import tabulate

def mean(values):
    if len(values) == 0:
        return None
    curr_sum = 0
    for num in values:
        curr_sum += num
    return curr_sum/len(values)

def try_convert(value):
    try:
        try:
            return float(value)
        except ValueError:
            return int(value)
    except ValueError:
        return value

def parse_filter(filter_str):
    for operator in ['>', '<', '=']:
        if operator in filter_str:
            column, value = filter_str.split(operator)
            return column, operator, try_convert(value)
    raise ValueError("Неверно указана фильтрация")

def parse_order(order_str):
    if '=' not in order_str:
        raise ValueError('Неверно указано сортировка')
    for how in ['asc', 'desc']:
        if (how in order_str):
            column, how = order_str.split('=', maxsplit=1)
            return column, how
    raise ValueError("Неверно указана сортировка")

def sort_by(data, order_arg):
    column, how = parse_order(order_arg)
    how = (how == 'desc')
    if not data:
        return data
    if (column not in next(iter(data))):
        raise ValueError(f'Колонки {column} нет в данных')
    return sorted(data, key=lambda x: try_convert(x[column]), reverse=how)



def apply_filter(data, condition):
    if not condition:
        return data
    column, operator, value = parse_filter(condition)
    filtered_data = []
    for row in data:
        curr_value = try_convert(row[column])
        if column not in row:
            raise ValueError(f'Колонки {column} нет в данных')
        if isinstance(curr_value, (int , float)) and isinstance(value, (int , float)):
            if operator == '>' and curr_value > value:
                filtered_data.append(row)
            elif operator == '<' and curr_value < value:
                filtered_data.append(row)
            elif operator == '=' and curr_value == value:
                filtered_data.append(row)
        elif isinstance(curr_value, str) and isinstance(value, str):
            if operator == '>' and curr_value > value:
                filtered_data.append(row)
            elif operator == '<' and curr_value < value:
                filtered_data.append(row)
            elif operator == '=' and curr_value == value:
                filtered_data.append(row)
        else:
            raise ValueError(f'Невозможно сравнить {row[column]} и {value}\nтипы: {type(curr_value)}, {type(value)}')

    return filtered_data

def aggregate_data(data, column, agg_func):
    values = [try_convert(row[column]) for row in data]

    if not values:
        return None

    if (agg_func not in ['avg', 'min', 'max']):
        raise ValueError('Неизвестная агрегационная функция')

    if (agg_func == 'min'):
        return min(values)
    elif (agg_func == 'max'):
        return max(values)
    elif (agg_func == 'avg' and isinstance(values[0], (int, float))):
        return mean(values)

    else:
        raise ValueError(f"Невозможно применить агрегацию к {type(values[0])}")


def run_pipeline(file_path, where=None, order_by=None, aggregate=None):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data = list(reader)
    if where:
        data = apply_filter(data, where)
    if order_by:
        data = sort_by(data, order_by)
    if aggregate:
        if not data:
            return tabulate([], headers=[aggregate.split('=')[1]])
        agg_column = aggregate.split('=')[0]
        agg_func = aggregate.split('=')[1]
        result = aggregate_data(data=data, column=agg_column, agg_func=agg_func)
        return tabulate([[result]], headers=[agg_func])
    else:
        return tabulate(data, headers="keys")

def main():
    parser = argparse.ArgumentParser(description="Фильтрация файла (.csv), применение функций и сортировка")
    parser.add_argument("--file", required=True, help='Путь к файлу .csv')
    parser.add_argument("--where", help="Фильтрация. Например: \"price>500\"")
    parser.add_argument("--aggregate", help="Агрегация: \"price=avg\", где avg | min | max")
    parser.add_argument("--order-by", help="Сортировка: \"brand=desc\"")
    args = parser.parse_args()

    run_pipeline(args.file, args.where, args.order_by, args.aggregate)

if __name__ == '__main__':
    main()