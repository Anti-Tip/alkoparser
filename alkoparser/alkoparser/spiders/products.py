import scrapy
import time
import re

from ..items import ProductItem


class ProductsSpider(scrapy.Spider):
    """Паук для сбора товаров из категорий alkoteka.com для региона Краснодар."""

    name = "products"
    allowed_domains = ["alkoteka.com"]

    # UUID Краснодара
    CITY_UUID = "4a70f9e0-46ae-11e7-83ff-00155d026416"

    def __init__(self, *args, **kwargs):
        """Инициализация паука.
        """
        super().__init__(*args, **kwargs)

        # Список URL
        self.START_URLS = [
            # "https://alkoteka.com/catalog/slaboalkogolnye-napitki-2",
            # "https://alkoteka.com/catalog/produkty-1",
            # "https://alkoteka.com/catalog/aksessuary-2",
            # "https://alkoteka.com/catalog/skidki",
            "https://alkoteka.com/catalog/krepkiy-alkogol"
        ]

    def start_requests(self):
        """Делает первый запрос для получения total количества товаров."""

        for url in self.START_URLS:
            # Извлекаем slug категории
            parts = url.rstrip('/').split('/')
            category_slug = parts[-1] if parts else ''
            if not category_slug:
                continue

            # Первый запрос: получаем total
            first_url = (
                f"https://alkoteka.com/web-api/v1/product?"
                f"city_uuid={self.CITY_UUID}&"
                f"root_category_slug={category_slug}"
            )

            yield scrapy.Request(
                url=first_url,
                callback=self.get_total_and_request_all,
                meta={
                    'category_url': url,
                    'category_slug': category_slug,
                }
            )

    def get_total_and_request_all(self, response):
        """Получает total (количество товаров в категории) и забирает все товары сразу."""
        category_url = response.meta['category_url']
        category_slug = response.meta['category_slug']

        try:
            data = response.json()
            total = data.get('meta', {}).get('total', 0)

            if total > 0:
                # Второй запрос: забираем все товары
                all_url = (
                    f"https://alkoteka.com/web-api/v1/product?"
                    f"city_uuid={self.CITY_UUID}&"
                    f"root_category_slug={category_slug}&"
                    f"per_page={total}"
                )

                yield scrapy.Request(
                    url=all_url,
                    callback=self.parse_product_list,
                    meta={
                        'category_url': category_url,
                        'category_slug': category_slug,
                        'total_items': total
                    }
                )

            else:
                self.logger.warning(f"Нет товаров в категории {category_slug}")

        except Exception as e:
            self.logger.error(f"Ошибка парсинга: {e}")

    def parse_product_list(self, response):
        """Парсит список всех товаров категории."""
        category_url = response.meta['category_url']
        category_slug = response.meta['category_slug']

        try:
            data = response.json()
            products = data.get('results', [])

            self.logger.info(f"Получено {len(products)} товаров из категории {category_slug}")

            # Для каждого товара делаем запрос на его карточку
            for product in products:
                product_slug = product.get('slug')
                if not product_slug:
                    continue

                # URL карточки товара
                product_url = f"https://alkoteka.com/web-api/v1/product/{product_slug}?city_uuid={self.CITY_UUID}"

                yield scrapy.Request(
                    url=product_url,
                    callback=self.parse_product_page,
                    meta={
                        'category_url': category_url,
                        'category_slug': category_slug,
                        'list_product_data': product  # Базовые данные из списка
                    }
                )

        except Exception as e:
            self.logger.error(f"Ошибка парсинга списка товаров: {e}")

    def parse_product_page(self, response):
        """Парсит полную информацию о товаре с его страницы."""
        category_slug = response.meta['category_slug']
        category_url = response.meta['category_url']
        list_product_data = response.meta.get('list_product_data', {})

        try:
            data = response.json()
            if not data.get('success'):
                self.logger.warning(f"Неуспешный запрос для {response.url}")
                return

            product = data.get('results', {})
            if not product:
                self.logger.warning(f"Нет данных о товаре в {response.url}")
                return

        except Exception as e:
            self.logger.error(f"Ошибка парсинга JSON: {e}")
            return

        try:
            # Формируем ProductItem
            item = ProductItem()

            # 1. timestamp - Unix timestamp в секундах
            item['timestamp'] = int(time.time())

            # 2. RPC - уникальный идентификатор
            item['RPC'] = product.get('uuid', '')

            # 3. url - ссылка на товар
            item['url'] = list_product_data.get('product_url', '')

            # 4. title - с добавлением характеристик если они не указаны в названии
            item['title'] = self._build_title(product)

            # 5. marketing_tags - маркетинговые тэги
            item['marketing_tags'] = self._get_marketing_tags(product)

            # 6. brand - бренд товара
            item['brand'] = self._extract_brand(product)

            # 7. section - иерархия категорий
            item['section'] = [
                product.get('category', {}).get('parent', {}).get('name', ''),
                product.get('category', {}).get('name', '')
            ]

            # 8. price_data - информация о цене
            item['price_data'] = {
                'current': float(product.get('price')) if product.get('price') is not None else 0.0,
                'original': float(product.get('prev_price', product.get('price', 0))) if product.get(
                    'prev_price') is not None else 0.0,
                'sale_tag': f"Скидка {round((1 - product['price'] / product.get('prev_price', product['price'])) * 100, 1)}%" if product.get(
                    'prev_price') and product.get('prev_price') > product.get('price') else ""
            }

            # 9. stock - информация о наличии
            item['stock'] = self._get_stock_info(product)

            # 10. assets - изображения
            item['assets'] = self._get_assets(product)

            # 11. metadata - все характеристики товара
            item['metadata'] = self._get_metadata(product, category_url, category_slug)

            # 12. variants - количество вариантов
            item['variants'] = self._count_variants(product)

            yield item

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге продукта {product.get('uuid', 'unknown')}: {e}")
            self.logger.error(f"Данные продукта: {product}")

    def _build_title(self, product: dict) -> str:
        """Строит заголовок товара с добавлением характеристик если их нет в названии."""
        base_title = product.get('name', '').strip()

        # Получаем характеристики из товара
        product_volume = None
        product_weight = None
        product_color = None

        # 1. Ищем объем и вес в filter_labels
        filter_labels = product.get('filter_labels')
        if filter_labels:
            for label in filter_labels:
                filter_code = label.get('filter', '')
                title = label.get('title', '')

                if filter_code == 'obem' and title:
                    product_volume = title
                    # Форматируем объем
                    if 'Л' not in product_volume and any(char.isdigit() for char in product_volume):
                        product_volume = f"{product_volume} Л"
                elif filter_code == 'ves' and title:
                    product_weight = title
                elif filter_code == 'cvet' and title:
                    product_color = label.get('title', '')

        # 2. Ищем в description_blocks если не нашли в filter_labels
        description_blocks = product.get('description_blocks')
        if description_blocks:
            for block in description_blocks:
                block_code = block.get('code', '')
                block_type = block.get('type', '')
                unit = block.get('unit', '')

                if block_code == 'obem' and not product_volume:
                    if block_type == 'range':
                        min_val = block.get('min')
                        if min_val is not None:
                            product_volume = f"{min_val} {unit}".replace('  ', ' ').strip()

                elif block_code == 'ves' and not product_weight:
                    if block_type == 'range':
                        min_val = block.get('min')
                        if min_val is not None:
                            product_weight = f"{min_val} {unit}".replace('  ', ' ').strip()
                    # Также проверяем values для select типа
                    elif block_type == 'select':
                        values = block.get('values', [])
                        for value in values:
                            if value.get('enabled', True):
                                name = value.get('name', '')
                                if name and any(char.isdigit() for char in name):
                                    product_weight = name
                                    break

        # 3. Проверяем, есть ли характеристики в названии
        base_lower = base_title.lower()

        # Проверка веса
        has_weight = False
        if product_weight:
            # Извлекаем числовую часть веса
            weight_match = re.search(r'(\d+[\.,]?\d*)', product_weight)
            if weight_match:
                weight_num = weight_match.group(1)
                # Проверяем есть ли число в названии
                if weight_num in base_title:
                    has_weight = True
                # Проверяем индикаторы веса
                elif any(indicator in base_lower for indicator in ['г', 'гр', 'грамм', 'кг', 'g', 'gr']):
                    has_weight = True
                # Проверяем полное совпадение веса
                elif product_weight.lower() in base_lower:
                    has_weight = True

        # Проверка объема
        has_volume = False
        if product_volume:
            # Извлекаем числовую часть объема
            volume_match = re.search(r'(\d+[\.,]?\d*)', product_volume)
            if volume_match:
                volume_num = volume_match.group(1)
                # Проверяем есть ли число в названии
                if volume_num in base_title:
                    has_volume = True
                # Проверяем индикаторы объема
                elif any(indicator in base_lower for indicator in ['л', 'литр', 'ml', 'мл']):
                    has_volume = True
                # Проверяем полное совпадение объема
                elif product_volume.lower() in base_lower:
                    has_volume = True

        # Проверка цвета
        has_color = False
        if product_color:
            color_lower = product_color.lower()
            # Проверяем частичное совпадение
            color_indicators = {
                'светлое': ['светл', 'светное'],
                'темное': ['темн', 'темное'],
                'красное': ['красн', 'красное'],
                'белое': ['бел', 'белое'],
                'розовое': ['розов', 'розовое'],
                'черное': ['черн', 'черное'],
            }

            if color_lower in base_lower:
                has_color = True
            else:
                # Проверяем частичные совпадения
                for color, indicators in color_indicators.items():
                    if color_lower in color:
                        if any(indicator in base_lower for indicator in indicators):
                            has_color = True
                            break

        # 4. Определяем категорию товара
        category_name = product.get('category', {}).get('name', '').lower()
        parent_category = product.get('category', {}).get('parent', {}).get('name', '').lower()

        is_food = any(food_indicator in category_name or food_indicator in parent_category
                      for food_indicator in ['продукты', 'снеки', 'шоколад', 'сыр', 'мясо',
                                             'закуски', 'бакалея', 'консервация', 'конфеты'])

        is_drink = any(drink_indicator in category_name or drink_indicator in parent_category
                       for drink_indicator in ['напитки', 'пиво', 'вино', 'сидр', 'медовуха',
                                               'алкоголь', 'водка', 'коньяк', 'виски'])

        # 5. Добавляем недостающую характеристику в заголовок
        result_title = base_title

        # Приоритет: для продуктов - вес, для напитков - объем/цвет
        if is_food:
            if product_weight and not has_weight:
                result_title = f"{result_title}, {product_weight}"
            elif product_volume and not has_volume:
                result_title = f"{result_title}, {product_volume}"
            elif product_color and not has_color:
                result_title = f"{result_title}, {product_color}"
        elif is_drink:
            if product_volume and not has_volume:
                result_title = f"{result_title}, {product_volume}"
            elif product_color and not has_color:
                result_title = f"{result_title}, {product_color}"
            elif product_weight and not has_weight:
                result_title = f"{result_title}, {product_weight}"
        else:
            # Общая логика для остальных категорий
            if product_weight and not has_weight:
                result_title = f"{result_title}, {product_weight}"
            elif product_volume and not has_volume:
                result_title = f"{result_title}, {product_volume}"
            elif product_color and not has_color:
                result_title = f"{result_title}, {product_color}"

        return result_title

    def _get_marketing_tags(self, product: dict) -> list:
        """Извлекает маркетинговые тэги из данных товара."""
        tags = []

        # Тэги из action_labels
        for action in product.get('action_labels', []):
            title = action.get('title', '')
            if title:
                tags.append(title)

        # Новинка
        if product.get('new'):
            tags.append('Новинка')

        # Рекомендуемое
        if product.get('recomended'):
            tags.append('Рекомендуемое')

        # Онлайн цена
        if product.get('has_online_price'):
            tags.append('Выгодно онлайн')

        # Товары со скидкой из filter_labels
        for label in product.get('filter_labels', []):
            if label.get('filter') == 'tovary-so-skidkoi' and label.get('title'):
                tags.append(label.get('title'))

        # Ценовые акции из price_details
        price_details = product.get('price_details')
        if price_details:  # Проверяем что не None
            for detail in price_details:
                title = detail.get('title', '')
                if title and title not in tags:
                    tags.append(title)

        # Скидка если есть разница в ценах
        price = product.get('price')
        prev_price = product.get('prev_price')
        if prev_price and price and prev_price > price > 0:
            tags.append('Скидка')

        return list(set(tags))

    def _get_stock_info(self, product: dict) -> dict:
        """Формирует информацию о наличии."""
        quantity_total = product.get('quantity_total', 0)
        available = product.get('available', False)
        warning = product.get('warning', '')
        availability_title = product.get('availability_title', '')

        # Проверяем наличие в магазинах
        has_stores = False
        availability = product.get('availability', {})
        if availability:
            stores = availability.get('stores', [])
            has_stores = len(stores) > 0

        # Определяем есть ли товар в наличии
        in_stock = False

        if quantity_total > 0:
            in_stock = True
        elif available:
            in_stock = True
        elif has_stores:
            in_stock = True

        # Проверяем явные признаки отсутствия
        if warning and any(phrase in warning.lower() for phrase in ['нет в наличии', 'недоступен', 'отсутствует']):
            in_stock = False

        if availability_title and any(
                phrase in availability_title.lower() for phrase in ['нет в наличии', 'недоступен', 'отсутствует']):
            in_stock = False

        # Если quantity_total = 0 и нет магазинов, то товара точно нет
        if quantity_total == 0 and not has_stores:
            in_stock = False

        return {
            'in_stock': in_stock,
            'count': quantity_total
        }

    def _get_assets(self, product: dict) -> dict:
        """Формирует информацию об изображениях и видео."""
        main_image = product.get('image_url', '')

        return {
            'main_image': main_image,
            'set_images': [main_image] if main_image else [],
            'view360': [],
            'video': []
        }

    def _get_metadata(self, product: dict, category_url: str, category_slug: str) -> dict:
        """Собирает все характеристики товара."""
        description_parts = []

        # Добавляем subname если есть
        subname = product.get('subname', '')
        if subname:
            description_parts.append(subname)

        # Ищем описание в text_blocks
        text_blocks = product.get('text_blocks', [])
        for block in text_blocks:
            if block.get('title') == 'Описание' and block.get('content'):
                content = block.get('content', '')
                # Очищаем HTML теги
                clean_content = re.sub(r'<[^>]+>', '', content)
                # Заменяем переносы строк на пробелы
                clean_content = clean_content.replace('\n', ' ').replace('\r', ' ')
                # Убираем лишние пробелы
                clean_content = ' '.join(clean_content.split())
                description_parts.append(clean_content)
                break

        # Объединяем все части описания
        description = ' '.join(description_parts).strip()

        metadata = {
            '__description': description if description else '',
        }

        # Основные характеристики
        basic_info = {
            'Артикул': product.get('vendor_code'),
            'Код товара': product.get('uuid'),
            'Страна': product.get('country_name'),
            'Код страны': product.get('country_code'),
            'Доступное количество': product.get('quantity_total'),
            'Категория URL': category_url,
            'Категория slug': category_slug,
            'Новинка': 'Да' if product.get('new') else 'Нет',
            'Рекомендуемое': 'Да' if product.get('recomended') else 'Нет',
            'Енограмма': 'Да' if product.get('enogram') else 'Нет',
            'Аксиома': 'Да' if product.get('axioma') else 'Нет',
            'Подарочная упаковка': 'Да' if product.get('gift_package') else 'Нет',
            'Цена оффлайн': product.get('offline_price'),
            'Избранное': 'Да' if product.get('favorite') else 'Нет',
            'Количество в наличии': product.get('quantity'),
            'Предупреждение': product.get('warning'),
            'Статус': product.get('status', 'active'),
            'Заголовок наличия': product.get('availability_title'),
        }

        for key, value in basic_info.items():
            if value is not None and value != '' and value != 'Нет':
                metadata[key] = value

        # Характеристики из filter_labels
        filter_labels = product.get('filter_labels')
        if filter_labels:
            for label in filter_labels:
                filter_name = label.get('filter', '')
                title = label.get('title', '')
                filter_type = label.get('type', '')

                if filter_name and title:
                    filter_mapping = {
                        'categories': 'Категория',
                        'strana': 'Страна',
                        'brend': 'Бренд',
                        'proizvoditel': 'Производитель',
                        'vid-upakovki': 'Вид упаковки',
                        'dopolnitelno': 'Дополнительно',
                        'cvet': 'Цвет',
                        'obem': 'Объем',
                        'ves': 'Вес',
                        'tovary-so-skidkoi': 'Товары со скидкой',
                        'cena': 'Цена',
                        'v-nalicii': 'В наличии',
                        'tip-piva': 'Тип пива',
                        'sort-piva': 'Сорт пива',
                        'podarocnaya-upakovka': 'Подарочная упаковка',
                        'sort-vina': 'Сорт вина',
                        'tip-vina': 'Тип вина',
                        'vkus': 'Вкус',
                        'osobennosti': 'Особенности',
                        's-ostavom': 'С составом',
                        'bez-sostava': 'Без состава',
                        'soderzanie-saxara': 'Содержание сахара',
                        'vid': 'Вид',
                        'sortovoi-sostav': 'Сортовой состав',
                        'region': 'Регион',
                        'emkost-vyderzki': 'Емкость выдержки',
                        'temperatura-podaci': 'Температура подачи',
                    }
                    display_name = filter_mapping.get(filter_name, filter_name)

                    if filter_type == 'range' and label.get('values'):
                        values = label.get('values', {})
                        if 'min' in values and 'max' in values:
                            if values['min'] == values['max']:
                                metadata[display_name] = f"{values['min']}"
                            else:
                                metadata[display_name] = f"{values['min']} - {values['max']}"
                    elif title.lower() != 'да' and title.lower() != 'нет':
                        metadata[display_name] = title

        # Информация о категории
        category = product.get('category', {})
        if category:
            metadata['Категория товара'] = category.get('name', '')
            metadata['Категория UUID'] = category.get('uuid', '')
            metadata['Категория slug'] = category.get('slug', '')

            if category.get('background_color'):
                metadata['Цвет фона категории'] = category.get('background_color')

            parent = category.get('parent', {})
            if parent:
                metadata['Родительская категория'] = parent.get('name', '')
                metadata['Родительская категория UUID'] = parent.get('uuid', '')
                metadata['Родительская категория slug'] = parent.get('slug', '')

        # Характеристики из description_blocks
        description_blocks = product.get('description_blocks')
        if description_blocks:
            for block in description_blocks:
                block_code = block.get('code', '')
                block_title = block.get('title', '')
                block_type = block.get('type', '')
                unit = block.get('unit', '')

                if block_code and block_title:
                    # Для всех блоков собираем данные
                    values = block.get('values', [])
                    if values:
                        # Для select типа
                        if block_type == 'select':
                            enabled_values = [v.get('name', '') for v in values if v.get('enabled', True)]
                            if enabled_values:
                                metadata[block_title] = ', '.join(enabled_values)
                        # Для range типа
                        elif block_type == 'range':
                            min_val = block.get('min')
                            max_val = block.get('max')
                            if min_val is not None and max_val is not None:
                                if min_val == max_val:
                                    metadata[block_title] = f"{min_val}{unit}"
                                else:
                                    metadata[block_title] = f"{min_val} - {max_val}{unit}"
                    elif block_type == 'range':
                        min_val = block.get('min')
                        max_val = block.get('max')
                        if min_val is not None:
                            if max_val is not None and min_val != max_val:
                                metadata[block_title] = f"{min_val} - {max_val}{unit}"
                            else:
                                metadata[block_title] = f"{min_val}{unit}"

        # Информация об акциях
        action_labels = product.get('action_labels')
        if action_labels:
            action_titles = [action.get('title', '') for action in action_labels if action.get('title')]
            if action_titles:
                metadata['Акции'] = ', '.join(action_titles)

        # Информация о ценах из price_details
        price_details = product.get('price_details')
        if price_details:
            price_info = []
            for detail in price_details:
                prev_price = detail.get('prev_price')
                price = detail.get('price')
                title = detail.get('title', '')

                if title:
                    price_info.append(title)
                elif prev_price and price:
                    discount = round((1 - price / prev_price) * 100, 1)
                    price_info.append(f"Скидка {discount}% (было {prev_price}, стало {price})")

            if price_info:
                metadata['Детали цен'] = '; '.join(price_info)

        # Информация о наличии в магазинах
        availability = product.get('availability', {})
        if availability:
            stores = availability.get('stores', [])
            if stores:
                metadata['Количество магазинов'] = len(stores)

                # Считаем общее количество товара по магазинах
                total_in_stores = 0
                for store in stores:
                    quantity_str = store.get('quantity', '0 шт')
                    match = re.search(r'(\d+)', quantity_str)
                    if match:
                        total_in_stores += int(match.group(1))

                if total_in_stores > 0:
                    metadata['Количество во всех магазинах'] = total_in_stores

        # Гастрономические сочетания
        gastronomics = product.get('gastronomics', {})
        if gastronomics:
            for category_name, items in gastronomics.items():
                if items and isinstance(items, list):
                    item_titles = [item.get('title', '') for item in items if item.get('title')]
                    if item_titles:
                        # Преобразуем название категории
                        category_display = {
                            'poultry': 'С птицей',
                            'meat': 'С мясом',
                            'fish': 'С рыбой',
                            'cheese': 'С сыром',
                            'dessert': 'С десертом',
                        }.get(category_name, category_name)

                        metadata[f'Гастрономические сочетания ({category_display})'] = ', '.join(item_titles)

        return metadata

    def _count_variants(self, product: dict) -> int:
        """Подсчитывает количество вариантов товара."""
        variants = 1

        # Ищем разные объемы в description_blocks
        description_blocks = product.get('description_blocks')
        if description_blocks:
            volumes = set()
            for block in description_blocks:
                if block.get('code') == 'obem':
                    min_val = block.get('min')
                    max_val = block.get('max')
                    if min_val is not None:
                        if max_val is not None and min_val != max_val:
                            # Диапазон объемов
                            variants = max(2, variants)  # Минимум 2 варианта если есть диапазон
                        else:
                            volumes.add(str(min_val))

            # Если найдены разные объемы
            if len(volumes) > 1:
                variants = len(volumes)

        # Ищем в filter_labels
        filter_labels = product.get('filter_labels')
        if filter_labels:
            colors = set()
            for label in product.get('filter_labels', []):
                if label.get('filter') == 'cvet' and label.get('title'):
                    colors.add(label.get('title'))

            if len(colors) > 1:
                variants = max(variants, len(colors))

        return variants

    def _extract_brand(self, product: dict) -> str:
        """Извлекает бренд товара."""
        brand = ''

        # Сначала ищем в description_blocks
        description_blocks = product.get('description_blocks')
        if description_blocks:
            for block in description_blocks:
                if block.get('code') == 'brend' and block.get('values'):
                    for value in block['values']:
                        if value.get('enabled', True):
                            brand = value.get('name', '')
                            break
                    if brand:
                        break

        # Если не нашли бренд, ищем производителя
        if not brand:
            if description_blocks:
                for block in description_blocks:
                    if block.get('code') == 'proizvoditel' and block.get('values'):
                        for value in block['values']:
                            if value.get('enabled', True):
                                brand = value.get('name', '')
                                break
                        if brand:
                            break

        # Затем в filter_labels
        if not brand:
            filter_labels = product.get('filter_labels')
            if filter_labels:
                for label in filter_labels:
                    if label.get('filter') == 'brend':
                        brand = label.get('title', '')
                        break

        return brand
