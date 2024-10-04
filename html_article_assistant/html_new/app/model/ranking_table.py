from tabulate import tabulate

from constants.constants import NULL
from util.logger import get_logger

logger = get_logger(__name__)


class RankedItem:
    def __init__(self, name: str, value, ranking: int = None, weighted_value=None, display_only: bool = False):
        self.name = name
        self.value = 0 if value == NULL else value
        self.displayed_value = value
        self.ranking = ranking
        self.weighted_normalized_value = weighted_value
        self.display_only = display_only

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        ranking = 0 if not self.ranking else self.ranking
        weighted_value = 0 if not self.weighted_normalized_value else self.weighted_normalized_value
        if self.display_only:
            string = f"#{ranking:02d} {self.name:<15} ({weighted_value:.02f} weighted/normalized)"
        else:
            value = f"{round(self.displayed_value, 2):.02f}" if type(self.displayed_value) in [int, float] else self.displayed_value
            string = f"#{ranking:02d} | {value} ({weighted_value:.02f} weighted/normalized)"
        return string


class Column:
    def __init__(self, name: str, weight: float, items: list[RankedItem], descending: bool = True):
        self.name = name
        self.weight = weight
        self.descending = descending
        self.items = items

    def rank(self):
        minimum, maximum = min(self.items, key=lambda x: x.value).value, max(self.items, key=lambda x: x.value).value
        lower, upper = 0, 100
        for item in self.items:
            if maximum == minimum:
                normalized_value = 0
            else:
                normalized_value = (item.value - minimum) / (maximum - minimum) * (upper - lower) + lower
            item.weighted_normalized_value = normalized_value * self.weight
            if self.descending:
                item.weighted_normalized_value *= -1
        ranking = sorted(self.items, key=lambda o: o.weighted_normalized_value, reverse=not self.descending)
        for i, item in enumerate(ranking):
            item.ranking = i + 1


class Table:
    def __init__(self, initial_items: list[str]):
        self.items = [RankedItem(item, 0, display_only=True) for item in initial_items]
        self.columns = []

    def rank(self):
        for column in self.columns:
            column.rank()
        table = []
        for i, item in enumerate(self.items):
            # Start row with city name: ["Seattle"]
            row = [item]
            for column in self.columns:
                for y, element in enumerate(column.items):
                    if element.name == item.name:
                        # Get the element in each column corresponding to the city name: {"name": "Seattle", "value": 1}
                        row.append(element)
                        break
            item.weighted_normalized_value = round(sum([e.weighted_normalized_value for e in row[1:]]), 2)
            table.append(row)
        # Sort rows based on weighted value of the city
        table.sort(key=lambda o: o[0].weighted_normalized_value, reverse=True)
        for i, row in enumerate(table):
            row[0].ranking = i + 1
        table_str = tabulate(table, headers=["Items"] + [column.name for column in self.columns])
        logger.info(f"Ranked table: {table_str}")
        for row in range(len(table)):
            for col in range(1, len(table[row])):
                table[row][col] = table[row][col].displayed_value
        for row in table:
            row[0] = f"{row[0].name}"
        return [list(map(str, item)) for item in table]

    def add_column(self, name: str, weight: float, items: list[RankedItem], descending: bool = True):
        self.columns.append(Column(name, weight, items, descending))
        print('done')
