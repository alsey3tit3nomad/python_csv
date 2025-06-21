import pytest
from contextlib import nullcontext as does_not_raise
from app.python_csv import (
    parse_filter,
    apply_filter,
    parse_order,
    sort_by,
    aggregate_data,
    mean,
    try_convert,
    run_pipeline
)
import csv
import tempfile
from tabulate import tabulate

@pytest.mark.parametrize(
    "values, res",
    [
        ([1, 2, 3], 2),
        ([10, 20], 15),
        ([100], 100),
    ]
)
def test_mean(values, res):
    assert mean(values) == res

@pytest.mark.parametrize(
    "value, res",
    [
        ("10", 10.0),
        ("10.5", 10.5),
        ("abc", "abc"),
    ]
)
def test_try_convert(value, res):
    assert try_convert(value) == res

@pytest.mark.parametrize(
    "filter_str, res, raises",
    [
        ("price>100", ("price", ">", 100), does_not_raise()),
        ("name=iphone", ("name", "=", "iphone"), does_not_raise()),
        ("bad-filter", None, pytest.raises(ValueError)),
    ]
)
def test_parse_filter(filter_str, res, raises):
    with raises:
        assert parse_filter(filter_str) == res

@pytest.mark.parametrize(
    "order_str, res, raises",
    [
        ("price=desc", ("price", "desc"), does_not_raise()),
        ("name=asc", ("name", "asc"), does_not_raise()),
        ("invalid", None, pytest.raises(ValueError)),
    ]
)
def test_parse_order(order_str, res, raises):
    with raises:
        assert parse_order(order_str) == res


sample_data = [
    {"name": "A", "price": "100"},
    {"name": "B", "price": "200"},
    {"name": "C", "price": "300"},
]

@pytest.mark.parametrize(
    "data, condition, res_names",
    [
        (sample_data, "price>150", ["B", "C"]),
        (sample_data, "price=100", ["A"]),
        (sample_data, "price<200", ["A"]),
    ]
)
def test_apply_filter(data, condition, res_names):
    filtered = apply_filter(data, condition)
    result_names = [item["name"] for item in filtered]
    assert result_names == res_names

@pytest.mark.parametrize(
    "data, order_str, res_first_name",
    [
        (sample_data, "price=asc", "A"),
        (sample_data, "price=desc", "C"),
    ]
)
def test_order_by(data, order_str, res_first_name):
    ordered = sort_by(data, order_str)
    assert ordered[0]["name"] == res_first_name


@pytest.mark.parametrize(
    "data, column, func, res, raises",
    [
        ([{"price": "10"}, {"price": "20"}], "price", "avg", 15.0, does_not_raise()),
        ([{"price": "10"}, {"price": "20"}], "price", "min", 10, does_not_raise()),
        ([{"price": "10"}, {"price": "20"}], "price", "max", 20, does_not_raise()),
        ([{"price": "10"}], "price", "sum", None, pytest.raises(ValueError)),
        ([{"brand": "apple"}], "brand", "avg", None, pytest.raises(ValueError)),
    ]
)
def test_aggregate_data(data, column, func, res, raises):
    with raises:
        result = aggregate_data(data, column, func)
        assert result == res



def create_temp_csv(rows, headers):
    tmp = tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False, encoding='utf-8')
    writer = csv.DictWriter(tmp, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    tmp.flush()
    return tmp.name

@pytest.mark.parametrize("rows, where, order_by, aggregate, expected", [
    (
        [{"name": "Apple", "price": "100"}, {"name": "Banana", "price": "200"}],
        None, None, None,
        tabulate([{"name": "Apple", "price": "100"}, {"name": "Banana", "price": "200"}], headers="keys")
    ),

    (
        [{"name": "Apple", "price": "100"}, {"name": "Banana", "price": "200"}],
        "price>100", None, None,
        tabulate([{"name": "Banana", "price": "200"}], headers="keys")
    ),

    (
        [{"name": "Banana", "price": "200"}, {"name": "Apple", "price": "100"}],
        None, "name=desc", None,
        tabulate([{"name": "Banana", "price": "200"}, {"name": "Apple", "price": "100"}], headers="keys")
    ),

    (
        [{"name": "A", "price": "100"}, {"name": "B", "price": "300"}],
        None, None, "price=avg",
        tabulate([[200.0]], headers=["avg"])
    ),

    (
        [{"name": "A", "price": "100"}],
        "price>999", None, "price=max",
        tabulate([], headers=["max"])
    ),
])
def test_run_pipeline(rows, where, order_by, aggregate, expected):
    file_path = create_temp_csv(rows, headers=["name", "price"])
    output = run_pipeline(file_path, where, order_by, aggregate)
    assert output.strip() == expected.strip()
