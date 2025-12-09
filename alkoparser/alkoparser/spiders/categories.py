import json
import scrapy
from pathlib import Path

from ..items import CategoriesItem


class CategoriesSpider(scrapy.Spider):
    """Паук для сбора категорий по городам"""

    name = "categories"
    allowed_domains = ["alkoteka.com"]

    def __init__(self, *args, **kwargs):
        """Инициализирует паука с путем к файлу городов."""
        super().__init__(*args, **kwargs)
        self.cities_file = Path('cities_uuid.json')

    def start_requests(self):
        """Читаем города из файла и запрашиваем категории для каждого"""
        if not self.cities_file.exists():
            self.logger.error(f"Файл {self.cities_file} не найден!")
            self.logger.info("Сперва выполните парсинг городов")
            return

        try:
            with open(self.cities_file, 'r', encoding='utf-8') as file:
                cities_data = json.load(file)
        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка чтения JSON файла: {e}")
            return
        except Exception as e:
            self.logger.error(f"Ошибка открытия файла: {e}")
            return

        if not isinstance(cities_data, list):
            self.logger.error(f"Файл должен содержать массив UUID, получен: {type(cities_data)}")
            return

        self.logger.info(f"Загружено {len(cities_data)} городов")

        for city in cities_data:
            city_uuid = city.get('uuid')
            city_name = city.get('name')
            url = (
                f"https://alkoteka.com/web-api/v1/category?"
                f"city_uuid={city_uuid}"
            )
            yield scrapy.Request(url, callback=self.parse, meta={'city_uuid': city_uuid})

    def parse(self, response):
        """Обрабатывает ответ с категориями для конкретного города"""
        city_uuid = response.meta['city_uuid']

        try:
            data = response.json()
        except ValueError:
            self.logger.error(f"Invalid JSON response for city {city_uuid}")
            return

        categories = data.get("results", [])
        self.logger.info(f"Город {city_uuid}: {len(categories)} <UNK> <UNK> <UNK>категорий")

        for category in categories:
            try:
                item = CategoriesItem()
                item["name"] = category.get("name", '')
                item["slug"] = category.get("slug", '')
                yield item
            except Exception as e:
                city = city_uuid
                self.logger.error(f"Ошибка при парсинге категорий в городе {city}: {e}")
                continue

