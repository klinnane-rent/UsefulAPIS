from PyQt5 import QtGui
from PyQt5.QtWidgets import (
    QDesktopWidget, QFormLayout, QLineEdit, QComboBox, QVBoxLayout, QPushButton, QLabel, QCheckBox, QSpinBox,
)
from PyQt5.QtWidgets import QWidget
from openpyxl import Workbook

from constants.enums.article_types_enum import ArticleType
from constants.article_builder_registry import ARTICLE_BUILDER_REGISTRY
from ui.extendable_list import ExtendableList
from ui.popup_window import DataViewerPopupWindow
from util.logger import get_logger

logger = get_logger(__name__)


def clear_layout(layout):
    logger.debug("Clearing layout")
    while layout.count():
        child = layout.takeAt(0)
        if child.widget():
            child.widget().deleteLater()


class App(QWidget):
    def __init__(self):
        super().__init__()
        self.layout = QVBoxLayout()
        self.article_type_layout = QFormLayout()
        self.article_type = None

        logger.debug("Adding layout elements to application")
        self.article_type_combo_box = QComboBox()
        self.input_form = QFormLayout()
        self.input_widgets = []
        self.generate_button = QPushButton("Generate Article")
        self.popup_window = None

        self.init_ui()
        self.build()
        self.show()
        logger.info("Application initialization complete")

    def init_ui(self):
        """
        Initializes the UI window
        """
        logger.debug("Setting up window")
        self.setWindowTitle("Article Assistant")
        self.setWindowIcon(QtGui.QIcon("data/icon.ico"))
        self.resize_and_center(600, 350)

    def resize_and_center(self, height, width):
        self.resize(height, width)
        logger.debug("Centering window in screen")
        frame_geometry = self.frameGeometry()
        center_point = QDesktopWidget().availableGeometry().center()
        frame_geometry.moveCenter(center_point)
        self.move(frame_geometry.topLeft())

    def build(self):
        """
        Builds the application layout with article-agnostic widgets
        """
        logger.debug("Creating article type combobox")
        self.article_type_combo_box.addItems(ArticleType.list())
        self.article_type_combo_box.currentIndexChanged.connect(self.on_article_type_selection)
        self.on_article_type_selection()
        self.article_type_layout.addRow('Article Type:', self.article_type_combo_box)

        # add article type combobox, input form, and generate button to layout
        logger.debug("Adding widgets to layout")
        self.layout.addLayout(self.article_type_layout)
        self.layout.addLayout(self.input_form)
        self.layout.addWidget(self.generate_button)
        self.setLayout(self.layout)

    def on_article_type_selection(self):
        """
        Updates the parameter input form with widgets based on the parameters specified in the registry
        """
        clear_layout(self.input_form)
        self.generate_button.setEnabled(False)
        self.input_widgets = []

        self.article_type = ArticleType(self.article_type_combo_box.currentText())
        logger.info(f"Switching to article type: {self.article_type}")
        registry_item = ARTICLE_BUILDER_REGISTRY.get(self.article_type)
        widgets = registry_item.get_input_form_widgets(self.check_parameters_are_filled, self.article_type)
        for label, widget in widgets.items():
            self.input_form.addRow(label, widget)
            self.input_widgets.append(widget)
        if self.article_type in [ArticleType.CUSTOM_ARTICLE, ArticleType.INFORMATION_ABOUT_CITY]:
            add_button = QPushButton("Add Row")
            add_button.clicked.connect(self.input_widgets[-1].add_row)
            self.input_form.addRow("Add Row", add_button)
            self.resize_and_center(1000, 500)
        try:
            self.generate_button.clicked.disconnect()
        except Exception:
            pass
        finally:
            self.generate_button.clicked.connect(
                lambda: self.on_generate_button_clicked(registry_item.builder_function)
            )

    def on_generate_button_clicked(self, builder_function: callable):
        """
        Calls the builder function to create the table of data
        """
        logger.debug("Building parameter dictionary from inputs:")
        parameters = dict()
        for widget in self.input_widgets:
            # get parameter key
            row, col = self.input_form.getWidgetPosition(widget)
            parameter_label = self.input_form.itemAt(row, col - 1).widget()
            if isinstance(parameter_label, QLabel):
                parameter_key = '_'.join(parameter_label.text().lower().split())
            else:
                logger.exception(f"Parameter names must be strings inside of QLabel objects")
                raise ValueError

            # get parameter value
            if isinstance(widget, QLineEdit):
                parameter_value = widget.text()
            elif isinstance(widget, QCheckBox):
                parameter_value = widget.isChecked()
            elif isinstance(widget, QSpinBox):
                parameter_value = widget.value()
            elif isinstance(widget, ExtendableList):
                parameter_value = widget.values()
            else:
                logger.exception(f"Widget of type {widget.__class__.__name__} is not recognized")
                raise NotImplementedError

            logger.debug(f"\t{parameter_key} = {parameter_value}")
            parameters[parameter_key] = parameter_value

        logger.info(f"Calling article builder function {builder_function.__name__} with arguments: {parameters}")
        try:
            result = builder_function(**parameters)
        except Exception as err:
            logger.exception(err)
            raise err

        workbook = Workbook()
        worksheet = workbook.active
        logger.debug("Adding search results to table")
        for entry in result:
            logger.debug(f"\t{entry}")
            worksheet.append(entry)

        self.popup_window = DataViewerPopupWindow(result, workbook)
        self.popup_window.show()

    def check_parameters_are_filled(self):
        """
        Ensures all parameters are filled out based on their type before enabling the generate button
        """
        conditions = []
        for widget in self.input_widgets:
            if isinstance(widget, QLineEdit):
                conditions.append(widget.text() != "")
            elif isinstance(widget, ExtendableList):
                conditions.append(True)
        self.generate_button.setEnabled(all(conditions))
