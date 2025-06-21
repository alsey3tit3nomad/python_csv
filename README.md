# CSV Processor

Простой Python-скрипт для обработки CSV-файлов с поддержкой фильтрации и агрегации данных.

### Функционал

Скрипт позволяет:
- **Фильтровать** данные по условию (`column>value`, `column<value`, `column=value`)
- **Агрегировать** данные: вычислять `avg`, `min`, `max` для числовой колонки
- **Сортировать** данные по возрастанию и убыванию
- Поддерживает **разные типы данных**: числа и строки
- Выводит результат в виде красивой таблицы (через `tabulate`)

---

## Как использовать

### Установка зависимостей

```bash
pip install tabulate argparse pytest
```
### Запуск

| name   | price | brand  |
|--------|-------|--------|
| Apple  | 100   | red    |
| Banana | 200   | yellow |
| Cherry | 300   | red    |
```Python
python app/python_csv.py --file data.csv
```
| name   | price | brand  |
|--------|-------|--------|
| Apple  | 100   | red    |
| Banana | 200   | yellow |
| Cherry | 300   | red    |
```Python
python app/python_csv.py --file data.csv --where "price>150"
```
| name   | price | brand  |
|--------|-------|--------|
| Banana | 200   | yellow |
| Cherry | 300   | red    |
```python
python app/python_csv.py --file data.csv --aggregate "price=avg"
```
| avg     |
|---------|
| 200.00  |
```python
python app/python_csv.py --file data.csv --where "brand=red" --aggregate "price=avg"
```
| avg     |
|---------|
| 200.00  |
```python
python app/python_csv.py --file data.csv --order-by "price=desc"
```
| name   | price | brand  |
|--------|-------|--------|
| Cherry | 300   | red    |
| Banana | 200   | yellow |
| Apple  | 100   | red    |