import inspect
import json
from json import JSONDecodeError

from PyQt5.QtWidgets import QLineEdit, QCheckBox, QSpinBox

from constants.enums.article_types_enum import ArticleType
from model.address import Address
from typing import get_origin

from ui.extendable_list import ExtendableList
from util.logger import get_logger

logger = get_logger(__name__)


class ArticleRegistryItem:
    def __init__(self, builder_function: callable):
        self.builder_function = builder_function
        args = inspect.getfullargspec(self.builder_function)
        if args.defaults:
            pairing = zip(reversed(args.args), reversed(args.defaults))
            self.defaults = {argument: default for argument, default in pairing}
        if self.builder_function.__doc__:
            try:
                self.limits = json.loads(self.builder_function.__doc__.split("Limits:\n")[1])
            except JSONDecodeError as err:
                logger.exception(err)
                raise err

        parameters = self.builder_function.__annotations__
        if "return" in parameters:
            parameters.pop("return")
        else:
            raise NotImplementedError(
                f"Article building function {self.builder_function.__name__} has no return type annotation"
            )

        if parameters:
            self.article_parameters = parameters
        else:
            raise NotImplementedError(
                f"Article building function {self.builder_function.__name__} has no parameter type annotation(s)"
            )

    def get_input_form_widgets(self, validation_function: callable, article_type: ArticleType):
        input_form_widget_dict = {}
        for parameter, parameter_type in self.article_parameters.items():
            if parameter_type == str:
                text_input = QLineEdit()
                text_input.textChanged.connect(validation_function)
                widget = text_input
            elif parameter_type == Address:
                widget = QLineEdit()
                widget.textChanged.connect(validation_function)
            elif parameter_type == bool:
                default_value = self.defaults.get(parameter, False)
                widget = QCheckBox()
                widget.setChecked(default_value)
            elif parameter_type == int:
                try:
                    lower_bound, upper_bound = self.limits[parameter]
                except KeyError as err:
                    logger.error(f"Unable to get limits for integer input {parameter} from {self.limits}")
                    raise ValueError() from err
                widget = QSpinBox()
                widget.setMaximum(upper_bound)
                widget.setMinimum(lower_bound)
                if self.defaults:
                    widget.setValue(self.defaults.get(parameter, lower_bound))
                else:
                    widget.setValue(lower_bound)
            elif get_origin(parameter_type) is not None:
                if get_origin(parameter_type) is list:
                    widget = ExtendableList(ranked=article_type == ArticleType.CUSTOM_ARTICLE)
                    widget.cellChanged.connect(validation_function)
                else:
                    raise NotImplementedError(
                        f"Widgets for parameters of type {parameter_type} in collections are not supported"
                    )
            else:
                raise NotImplementedError(f"Widgets for parameters of type {parameter_type} are not supported")
            input_form_widget_dict[' '.join(parameter.split('_')).title()] = widget
        return input_form_widget_dict
