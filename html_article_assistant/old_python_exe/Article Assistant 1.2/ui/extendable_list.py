from PyQt5.QtWidgets import QTableWidget, QComboBox, QHeaderView, QSpinBox, QPushButton, QLineEdit, QCheckBox

from api_library.census_api_client import CensusAPIClient
from api_library.foursquare_api_client import FoursquareAPIClient
from api_library.google_api_client import GoogleMapsAPIClient
from api_library.redfin_api_client import RedfinAPIClient
from api_library.tourist_api_client import TouristAPIClient
from constants.api_list import API_CLIENT_LIST
from constants.census_variable_mapping import CENSUS_VARIABLE_NAMES
from constants.redfin_metric_mapping import REDFIN_METRIC_MAPPING
from model.api import API

ARGS_WIDTH = 300


def _get_api_call_combobox(api_name: str, enabled: bool):
    api_client_module = __import__("constants.api_list", fromlist=[api_name])
    api_client_class = getattr(api_client_module, api_name)
    api_client_instance = api_client_class()
    api_call_list = [api_call_.__name__ for api_call_ in api_client_instance.get_api_calls()]
    api_call_combo_box = QComboBox()
    api_calls = [" ".join(api_call_.split('_')).title() for api_call_ in api_call_list]
    api_call_combo_box.addItems(api_calls)
    api_call_combo_box.setEnabled(enabled)
    return api_call_combo_box


class ExtendableList(QTableWidget):
    def __init__(self, ranked: bool = False):
        super().__init__()
        self.headers = {
            "API Client": QHeaderView.ResizeToContents,
            "API Call": QHeaderView.ResizeToContents,
            "Weight (0 - 100)": QHeaderView.ResizeToContents,
            "Arguments": QHeaderView.ResizeToContents,
            "Order": QHeaderView.ResizeToContents,
            "Remove Row": QHeaderView.ResizeToContents,
        }
        self.ranked = ranked
        if not self.ranked:
            self.headers.pop("Order")
        self.setColumnCount(len(self.headers))
        self.setHorizontalHeaderLabels(self.headers.keys())
        header = self.horizontalHeader()
        for column in range(self.columnCount()):
            header.setSectionResizeMode(column, self.headers[self.horizontalHeaderItem(column).text()])
        self.add_row()

    def add_row(self, api_client: str = None, enabled: bool = True):
        row = self.rowCount()
        if not api_client and row != 0:
            api_client = self.cellWidget(row - 1, 0).currentText()
        self.insertRow(row)

        # Create API Client combobox
        api_name_combo_box = QComboBox()
        api_client_list_copy = API_CLIENT_LIST.copy()
        if not self.ranked:
            api_client_list_copy.remove(GoogleMapsAPIClient)
            api_client_list_copy.remove(FoursquareAPIClient)
        else:
            api_client_list_copy.remove(TouristAPIClient)
        api_client_list = [api_.__name__ for api_ in api_client_list_copy]
        api_name_combo_box.addItems(api_client_list)
        api_name_combo_box.setEnabled(enabled)
        self.setCellWidget(row, 0, api_name_combo_box)

        # Create the API call combobox
        api_name_combo_box.currentIndexChanged.connect(
            lambda: self.update_calls(row, api_name_combo_box.currentText(), enabled)
        )

        # Preset the API client if applicable
        if api_client in api_client_list:
            api_index = api_client_list.index(api_client)
            api_name_combo_box.setCurrentIndex(api_index)
        self.update_calls(row, api_name_combo_box.currentText(), enabled)

        # Create the weight spinbox
        weight_spinbox = QSpinBox()
        weight_spinbox.setMinimum(1)
        weight_spinbox.setMaximum(99)
        self.setCellWidget(row, 2, weight_spinbox)

        # Create the arguments box
        args = QLineEdit()
        args.setDisabled(True)
        args.setFixedWidth(ARGS_WIDTH)
        self.setCellWidget(row, 3, args)

        # Create the order selector
        if self.ranked:
            order_checkbox = QCheckBox()
            order_checkbox.setText("Ascending?")
            self.setCellWidget(row, 4, order_checkbox)

        # Add delete row button
        # sub = 1 if self.ranked else 2
        button = QPushButton("Remove")
        button.clicked.connect(lambda: self.removeRow(self.currentRow()))
        self.setCellWidget(row, len(self.headers) - 1, button)
        self.resizeColumnsToContents()

        # Update calls
        api_name = self.cellWidget(row, 0).currentText()
        self.update_calls(row, api_name, True)

    def update_calls(self, row, api_name, enabled):
        combo_box = _get_api_call_combobox(api_name, enabled)
        self.setCellWidget(row, 1, combo_box)
        if api_name == "FoursquareAPIClient":
            args = QLineEdit()
            args.setFixedWidth(ARGS_WIDTH)
            args.setEnabled(True)
            self.setCellWidget(row, 3, args)
            return
        else:
            args = QLineEdit()
            args.setFixedWidth(ARGS_WIDTH)
            args.setDisabled(True)
            self.setCellWidget(row, 3, args)

        if api_name == "CensusAPIClient":
            args = QComboBox()
            args.setFixedWidth(ARGS_WIDTH)
            args.addItems(CENSUS_VARIABLE_NAMES.keys())
            self.setCellWidget(row, 3, args)
        elif api_name == "RedfinAPIClient":
            args = QComboBox()
            args.setFixedWidth(ARGS_WIDTH)
            args.addItems(REDFIN_METRIC_MAPPING.keys())
            self.setCellWidget(row, 3, args)

    def values(self):
        values = []
        for row in range(self.rowCount()):
            api_name = self.cellWidget(row, 0).currentText()
            api_client_module = __import__("constants.api_list", fromlist=[api_name])
            api_client = getattr(api_client_module, api_name)
            if api_client == FoursquareAPIClient:
                args = self.cellWidget(row, 3).text()
                if not args:
                    args = "restaurants"
            elif api_client == CensusAPIClient:
                args = self.cellWidget(row, 3).currentText()
            elif api_client == RedfinAPIClient:
                args = self.cellWidget(row, 3).currentText()
            else:
                args = ""
            item = API(
                api_client=api_client,
                api_method=self.cellWidget(row, 1).currentText(),
                weight=float(self.cellWidget(row, 2).value()),
                column_name=" ".join(self.cellWidget(row, 1).currentText().split()[1:]),
                ascending=self.cellWidget(row, 4).isChecked(),
                args=args,
            )
            values.append(item)
        return values
