import scrapy

from ..items import CitiesItem


class CitiesSpider(scrapy.Spider):
    """Паук для сбора городов с alkoteka.com."""

    name = "cities"
    allowed_domains = ["alkoteka.com"]

    def start_requests(self):
        """Формируем запрос с первой страницы"""
        page = 1
        url = f"https://alkoteka.com/web-api/v1/city?page={page}"
        yield scrapy.Request(url, callback=self.parse, meta={'page': page})

    def parse(self, response):
        """Обрабатывает ответ API и выполняет пагинацию."""
        page = response.meta['page']

        try:
            data = response.json()
        except ValueError:
            self.logger.error(f"Invalid JSON response on page {page}")
            return

        cities = data.get("results", [])

        for city in cities:
            try:
                item = CitiesItem()
                item["uuid"] = city.get("uuid", '')
                item["name"] = city.get("name", '')
                item["slug"] = city.get("slug", '')
                yield item
            except KeyError as e:
                city_id = city.get('uuid', 'unknown')
                self.logger.error(f"Ошибка при парсинге города {city_id}: {e}")
                continue

        meta = data.get('meta', {})
        if meta.get('has_more_pages', False):
            next_page = page + 1
            next_url = f"https://alkoteka.com/web-api/v1/city?page={next_page}"
            yield scrapy.Request(
                next_url,
                callback=self.parse,
                meta={'page': next_page}
            )


