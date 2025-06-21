import argparse
import csv
from tabulate import tabulate
import re

def mean(values):
    try:
        return sum(values)/len(values)
    except TypeError:
        raise TypeError("Нельзя применить avg к данной колонке")
    except ZeroDivisionError:
        raise ZeroDivisionError("Невозмонжо поделить на ноль")

def try_convert(value):
    if (re.fullmatch(r"\d+.\d+", value)):
        return float(value)
    elif (re.fullmatch(r"\d+", value)):
        return int(value)
    else:
        return value

def parse_filter(filter_str):
    if (re.fullmatch(r"\b[a-zA-Zа-яА-ЯёЁ]+[><=]{1}[0-9a-zA-Zа-яА-ЯёЁ]+\b", filter_str)):
        column, value = re.split(r'[><=]{1}', filter_str, maxsplit=1)
        operator = re.search(r'[><=]{1}', filter_str)[0]
        return column, operator, try_convert(value)
    raise ValueError("Неверно указана фильтрация")


def parse_order(order_str):
    if re.fullmatch(r"\b[a-zA-Zа-яА-ЯёЁ]+={1}(?:asc|desc)\b", order_str):
        column, how = re.split(r"=", order_str, maxsplit=1)
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
    operator_map = {
        '>': lambda x, y: x > y,
        '<': lambda x, y: x < y,
        '=': lambda x, y: x == y
    }
    for row in data:
        curr_value = try_convert(row[column])
        if column not in row:
            raise ValueError(f'Колонки {column} нет в данных')
        if (
            isinstance(curr_value, (int , float)) and isinstance(value, (int , float))
            or
            isinstance(curr_value, str) and isinstance(value, str)
        ):
            if (operator_map[operator](curr_value, value)):
                filtered_data.append(row)
        else:
            raise ValueError(f'Невозможно сравнить {row[column]} и {value}\nтипы: {type(curr_value)}, {type(value)}')

    return filtered_data

def aggregate_data(data, column, agg_func):
    values = [try_convert(row[column]) for row in data]

    if not values:
        return None
    map_agg_funcs = {
        'avg': lambda val : mean(val),
        'min': lambda val : min(val),
        'max': lambda val : max(val)
    }
    if (agg_func not in ['avg', 'min', 'max']):
        raise ValueError('Неизвестная агрегационная функция')
    return map_agg_funcs[agg_func](values)


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