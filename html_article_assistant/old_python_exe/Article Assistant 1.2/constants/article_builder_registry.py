from article_builders.attraction_in_city import attraction_in_city
from article_builders.custom_article import custom_article
from article_builders.information_about_city import information_about_city

from model.article_registry_item import ArticleRegistryItem
from constants.enums.article_types_enum import ArticleType

"""
The Article Builder Registry is a dictionary of <ArticleType: ArticleRegistryItem> key-value
pairs used to determine how to construct the final CSV table data
"""
ARTICLE_BUILDER_REGISTRY = {
    ArticleType.ATTRACTION_IN_CITY: ArticleRegistryItem(attraction_in_city),
    ArticleType.INFORMATION_ABOUT_CITY: ArticleRegistryItem(information_about_city),
    ArticleType.CUSTOM_ARTICLE: ArticleRegistryItem(custom_article),
}
