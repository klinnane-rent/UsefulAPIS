from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QDialog, QTableWidget, QTableWidgetItem, QVBoxLayout, QFileDialog, QPushButton
from openpyxl import Workbook

from util.logger import get_logger

logger = get_logger(__name__)


class DataViewerPopupWindow(QDialog):
    def __init__(self, data: list[list], workbook: Workbook):
        logger.debug("Creating pop-up window for search results")
        num_rows = len(data)
        num_cols = len(data[0])
        QDialog.__init__(self)
        self.layout = QVBoxLayout(self)
        self.workbook = workbook

        self.table = QTableWidget()
        self.table.setRowCount(num_rows)
        self.table.setColumnCount(num_cols)

        row_range = range(self.table.rowCount())
        for row in row_range:
            col_range = range(self.table.columnCount())
            for col in col_range:
                item = QTableWidgetItem(str(data[row][col]))
                self.table.setItem(row, col, item)

        self.layout.addWidget(self.table)
        save_button = QPushButton()
        save_button.setText("Save File")
        save_button.clicked.connect(self.save_file)
        self.layout.addWidget(save_button)
        self.setWindowTitle("Raw Article Data")
        self.setWindowFlags(Qt.WindowStaysOnTopHint)
        self.resize(600, 400)
        logger.info(f"Created pop-up window of {num_rows} rows x {num_cols} columns for search results")

    def save_file(self):
        file_dialog = QFileDialog()
        file_dialog.setFileMode(QFileDialog.Directory)
        file_dialog.setAcceptMode(QFileDialog.AcceptSave)
        file, check = file_dialog.getSaveFileName(None, "Save File", "", "Excel Files (*.xlsx)")
        if check:
            logger.info(f"Saving search results to {file}")
            self.workbook.save(file)
            self.close()
