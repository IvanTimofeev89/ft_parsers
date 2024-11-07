import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv
from googleapiclient.discovery import Resource
from telethon import TelegramClient, functions
from telethon.tl.types import MessageService

# Загружаем переменные окружения
load_dotenv()

# Инициализируем логгер
logger = logging.getLogger("VK_TG_PARSER")
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(name)s:%(levelname)s] --> %(message)s"
)


def get_links_from_google(google_service: Resource) -> Tuple[List, List]:
    """Функция для получения ссылок по которым будет парситься информация из Google таблицы"""
    all_links_spreadsheet_id = os.getenv("ALL_LINKS_SPREADSHEET_ID")
    posts_parsing_links = []
    channels_parsing_links = []
    try:
        response = (
            google_service.spreadsheets()
            .values()
            .get(spreadsheetId=all_links_spreadsheet_id, range="Лист1!A2:B")
            .execute()
        )
        data = response.get("values")
        for elem in data:
            if len(elem) == 1:
                posts_parsing_links.append(elem[0])
            elif len(elem) == 2:
                if elem[0]:
                    posts_parsing_links.append(elem[0])
                channels_parsing_links.append(elem[1])
        return posts_parsing_links, channels_parsing_links
    except Exception as error:
        logger.error(f"Ошибка при получении существующих данных из Google Sheets: {error}")


class BaseParser:
    """
    Базовый класс для парсеров постов и каналов, работающих с Google Sheets.
    Содержит общие методы для обработки и записи данных в Google Sheets.
    """

    column_letters = {
        1: "A",
        2: "B",
        3: "C",
        4: "D",
        5: "E",
        6: "F",
        7: "G",
        8: "H",
        9: "I",
        10: "J",
        11: "K",
        12: "L",
        13: "M",
        14: "N",
    }

    @staticmethod
    def _get_row_from_post(post: dict, parser_flag: str) -> List[str]:
        """
        Преобразует информацию о посте в список данных для записи в таблицу.

        Args:
            post (dict): Словарь с данными поста.
            parser_flag (str): Флаг парсера, определяющий формат поста.

        Returns:
            List[str]: Список данных, соответствующих структуре поста.
        """
        match parser_flag:
            case "vk_posts_parser":
                return [
                    post["Группа"],
                    post["Пост"],
                    post["Текст оригинального поста"],
                    post["Дата-время"],
                    post["Лайки"],
                    post["Репосты"],
                    post["Комментарии"],
                    post["Просмотры"],
                    post["Текст репоста"],
                    post["Ссылка на оригинальный пост"],
                    post["Ссылка в посте"],
                ]
            case "tg_posts_parser":
                return [
                    post["Канал"],
                    post["Пост"],
                    post["Текст поста"],
                    post["Дата-время"],
                    post["Реакции"],
                    post["Комментарии"],
                    post["Просмотры"],
                    post["Количество репостов"],
                    post["Ссылка на оригинальный пост"],
                    post["Ссылка в посте"],
                ]
            case "vk_channel_parser":
                return [
                    post["Канал"],
                    post["Дата-время"],
                    post["Количество подписчиков"],
                ]
            case "tg_channel_parser":
                return [
                    post["Канал"],
                    post["Дата-время"],
                    post["Количество подписчиков"],
                ]

    def _make_sheet_ready_data(self, posts_info_list: List[dict], parser_flag: str) -> List[List]:
        """
        Преобразует список постов в формат, пригодный для записи в Google Sheets.

        Args:
            posts_info_list (List[dict]): Список словарей с информацией о постах.
            parser_flag (str): Флаг парсера для выбора структуры данных поста.

        Returns:
            List[List]: Список списков, готовых для записи в таблицу.
        """
        rebuild_data = []
        # Преобразуем посты в формат списков
        for post in posts_info_list:

            row = self._get_row_from_post(post, parser_flag)

            rebuild_data.append(row)
        return rebuild_data

    def _get_existing_posts(self, spreadsheet_id: str) -> List[dict]:
        """
        Получает список существующих постов из Google Sheets.

        Args:
            spreadsheet_id (str): Идентификатор таблицы Google Sheets.

        Returns:
            List[dict]: Список существующих постов в виде словарей.
        """
        # Считываем все данные из столбца 'Пост' для проверки на дубликаты
        try:
            response = (
                self.google_service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range="Лист1!B2:B")
                .execute()
            )
            existing_posts = response.get("values", [])
            return [item[0] for item in existing_posts] if existing_posts else []
        except Exception as error:
            logger.error(f"Ошибка при получении существующих данных из Google Sheets: {error}")
            return []

    def _update_existing_rows(
        self, spreadsheet_id: str, rows_to_update: List[dict], parser_flag: str
    ) -> None:
        """
        Обновляет существующие строки в Google Sheets на основе полученных данных.

        Args:
            spreadsheet_id (str): Идентификатор таблицы Google Sheets.
            rows_to_update (List[dict]): Список данных для обновления.
            parser_flag (str): Флаг парсера для выбора структуры данных поста.

        Returns:
            None
        """
        # Вычисляем столбец в Google Sheets для обновления данных
        column_letter = self.column_letters[len(rows_to_update[0])]
        # Получаем данные для обновления в формате списков
        rebuild_data = self._make_sheet_ready_data(rows_to_update, parser_flag)

        # Находим индексы строк для обновления (исходя из ссылок на посты)
        existing_posts = self._get_existing_posts(spreadsheet_id)
        update_requests = []

        for row in rebuild_data:
            post_link = row[1]  # Индекс 1 — это столбец "Пост" (ссылка на пост)
            if post_link in existing_posts:
                row_index = (
                    existing_posts.index(post_link) + 2
                )  # Индекс строки для обновления в Google Sheets
                update_range = f"Лист1!A{row_index}:{column_letter}{row_index}"

                # Формируем запрос на обновление
                update_requests.append(
                    {"range": update_range, "majorDimension": "ROWS", "values": [row]}
                )

        if update_requests:
            body = {"valueInputOption": "USER_ENTERED", "data": update_requests}
            self.google_service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            logger.info(f"Успешно обновлены строки: {len(update_requests)}")

    def _insert_new_rows(
        self, spreadsheet_id: str, rows_to_insert: List[dict], parser_flag: str
    ) -> None:
        """
        Вставляет новые строки в Google Sheets.

        Args:
            spreadsheet_id (str): Идентификатор таблицы Google Sheets.
            rows_to_insert (List[dict]): Список данных для вставки.
            parser_flag (str): Флаг парсера для выбора структуры данных поста.

        Returns:
            None
        """
        # Получаем данные для вставки в формате списков
        rebuild_data = self._make_sheet_ready_data(rows_to_insert, parser_flag)

        # Получаем количество строк в таблице для вычисления следующей доступной строки
        response = (
            self.google_service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range="Лист1!A:A")
            .execute()
        )
        last_row = len(response.get("values", [])) + 1  # Последняя заполненная строка

        # Вставляем новые данные начиная с первой пустой строки
        number_sheet = f"Лист1!A{last_row}"

        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {
                    "range": number_sheet,
                    "majorDimension": "ROWS",  # Вставляем по строкам
                    "values": rebuild_data,
                }
            ],
        }
        self.google_service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()
        logger.info(f"Успешно добавлены новые строки: {len(rows_to_insert)}")

    def run_google_upsert(
        self, spreadsheet_id: str, posts_info_list: List[dict], parser_flag: str
    ) -> None:
        """
        Выполняет операцию upsert (обновление существующих записей или вставка новых)
        для постов в Google Sheets.

        Args:
            spreadsheet_id (str): Идентификатор таблицы Google Sheets.
            posts_info_list (List[dict]): Список данных о постах.
            parser_flag (str): Флаг парсера для выбора структуры данных поста.

        Returns:
            None
        """

        # Получаем существующие посты из Google Таблицы
        existing_posts = self._get_existing_posts(spreadsheet_id)

        rows_to_update = []
        rows_to_insert = []

        # Разделяем посты для вставки и обновление
        for post in posts_info_list:
            if post["Пост"] in existing_posts:
                rows_to_update.append(post)
            else:
                rows_to_insert.append(post)

        logger.info("Выгружаем данные в Google Sheets")

        # Выполняем обновление существующих строк
        if rows_to_update:
            self._update_existing_rows(spreadsheet_id, rows_to_update, parser_flag)

        # Выполняем вставку новых строк
        if rows_to_insert:
            self._insert_new_rows(spreadsheet_id, rows_to_insert, parser_flag)


class TgPostsParser(BaseParser):
    """
    Парсер для постов в Telegram каналах.
    Получает посты из указанных Telegram каналов и записывает их в Google Sheets.
    """

    def __init__(
        self, channel_links: List[str], tg_client: TelegramClient, google_service: Resource
    ) -> None:
        """
        Инициализирует парсер с ссылками на каналы и клиентом Telegram.

        Args:
            channel_links (List[str]): Список ссылок на Telegram каналы.
            tg_client (TelegramClient): Telegram клиент для взаимодействия с API.
        """
        self.channel_links = channel_links
        self.tg_links_pattern = re.compile(r"(?<=\]\()(?:https?://\S+)(?=\))|https?://\S+")
        self.tg_client = tg_client
        self.parser_flag = "tg_posts_parser"
        self.google_service = google_service

    @property
    async def tg_posts_links(self):
        """
        Возвращает список ссылок на посты в Telegram каналах.

        Returns:
            List[str]: Список ссылок на посты.
        """
        return [post["Пост"] for post in self.tg_posts_info_list]

    @property
    async def tg_posts_info_list(self) -> List[dict[str, str | int | Any]]:
        """
        Возвращает список с информацией о постах в Telegram каналах.

        Returns:
            List[dict]: Список словарей с данными о постах.
        """
        tg_posts_info_list = []

        tg_groups_counter = len(self.channel_links)

        for channel_link in self.channel_links:
            time.sleep(1)  # Пауза между запросами
            logger.info(
                f"Получение постов из канале: {channel_link}."
                f"Осталось {tg_groups_counter - 1} канал(-а, -ов)."
            )

            try:
                # Получаем информацию о канале
                channel = await self.tg_client.get_entity(channel_link)
                messages = await self.tg_client.get_messages(channel, limit=100)

                for message in messages:
                    if isinstance(message, MessageService) or message.grouped_id:
                        continue
                    reactions = message.reactions
                    replies = message.replies

                    original_channel_link = None
                    # Если пост является репостом
                    if message.forward:
                        if message.forward.is_channel:
                            channel_id = message.forward.from_id.channel_id
                            message_id = message.forward.channel_post
                            channel = await self.tg_client.get_entity(channel_id)
                            original_channel_link = f"https://t.me/{channel.username}/{message_id}"
                        elif message.forward.from_name:
                            original_channel_link = f"Репост от {message.forward.from_name}"

                    # Поиск ссылок в тексте поста
                    links_in_post = None
                    if message.text:
                        _ = re.findall(self.tg_links_pattern, message.text)
                        if _:
                            links_in_post = ", ".join(_)

                    message_info = {
                        "Канал": channel_link,
                        "Пост": f"{channel_link}/{message.id}",
                        "Текст поста": message.text,
                        "Дата-время": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                        "Реакции": (
                            sum([emoji.count for emoji in reactions.results]) if reactions else 0
                        ),
                        "Комментарии": replies.replies if replies else 0,
                        "Просмотры": message.views,
                        "Количество репостов": message.forwards,
                        "Ссылка на оригинальный пост": (
                            original_channel_link if original_channel_link else ""
                        ),
                        "Ссылка в посте": links_in_post,
                    }

                    tg_posts_info_list.append(message_info)

            except Exception as e:
                print(f"Произошла ошибка при обработке канала {channel_link}: {e}")

            tg_groups_counter -= 1

        return tg_posts_info_list

    async def run_tg_posts_parser(self, tg_spreadsheet_id):
        """
        Запускает парсер для обработки постов в Telegram и записи их в Google Sheets.

        Args:
            tg_spreadsheet_id (str): Идентификатор таблицы Google Sheets.

        Returns:
            None
        """
        # Дождаться формирования списка постов
        posts_info_list = await self.tg_posts_info_list
        self.run_google_upsert(
            spreadsheet_id=tg_spreadsheet_id,
            posts_info_list=posts_info_list,
            parser_flag=self.parser_flag,
        )


class VkPostsParser(BaseParser):
    """
    Парсер для постов в группах ВКонтакте.
    Получает посты из указанных групп ВКонтакте и записывает их в Google Sheets.
    """

    def __init__(self, links_list: List[str], google_service: Resource) -> None:
        """
        Инициализирует парсер со ссылками на группы ВКонтакте.

        Args:
            links_list (List[str]): Список ссылок на группы ВКонтакте.
        """
        self.links = links_list
        self.vk_domain_pattern = re.compile(r".*vk\.com/(.*)/?\Z")
        self.link_pattern = re.compile(r"(https?://\S+|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6}\S*)")
        self.url = "https://api.vk.com/method/wall.get"
        self.token = os.getenv("VK_TOKEN")
        self.parser_flag = "vk_posts_parser"
        self.params = {
            "access_token": self.token,
            "v": "5.95",
            "count": "100",
            "filter": "all",
        }
        self.google_service = google_service

    def _get_vk_wall(self, domain: str) -> List[dict]:
        """
        Получает посты со стены группы ВКонтакте.

        Args:
            domain (str): Домен группы ВКонтакте (название группы).

        Returns:
            List[dict]: Список постов в виде словарей.
        """
        try:
            self.params["domain"] = domain
            response = requests.get(self.url, params=self.params)
            if response.status_code != 200:
                sys.exit(f"Статус код не равен 200. Получен статус: {response.status_code}")
            data = response.json()["response"]["items"]
            return data
        except Exception as error:
            logger.error(f"Ошибка при получении постов: {error}")

    @property
    def vk_posts_info_list(self) -> List[dict]:
        """
        Возвращает список с информацией о постах в группах ВКонтакте.

        Returns:
            List[dict]: Список словарей с данными о постах.
        """
        vk_posts_info_list = []
        vk_groups_counter = len(self.links)

        for link in self.links:

            domain = self.vk_domain_pattern.search(link).group(1)

            logger.info(
                f"Получение постов из группы {domain}."
                f"Осталось {vk_groups_counter - 1} групп(-ы, -а)."
            )

            data = self._get_vk_wall(domain)

            vk_groups_counter -= 1

            for post in data:
                # Переменные для сохранения данных оригинально поста при репосте
                original_post_text = None
                original_post_url = None
                repost_text = None

                # Если пост является репостом
                if post.get("copy_history"):
                    original_post = post["copy_history"][-1]
                    original_post_text = original_post.get("text")
                    repost_text = post.get("text")
                    original_post_url = (
                        f'vk.com/wall{original_post["owner_id"]}_{original_post["id"]}'
                    )

                # Если пост является клипом, то сохраняем текст из описания
                post_text = post.get("text")
                if not post_text and post.get("attachments"):
                    if post.get("attachments")[0].get("type") == "video":
                        post_text = post.get("attachments")[0]["video"].get("description")

                # Поиск ссылок в тексте поста
                links_in_post = None
                if post_text or original_post_text or repost_text:
                    links_collector = []
                    if post_text:
                        _ = re.findall(self.link_pattern, post_text)
                        links_collector.extend(_)
                    if original_post_text:
                        _ = re.findall(self.link_pattern, original_post_text)
                        links_collector.extend(_)
                    if repost_text:
                        _ = re.findall(self.link_pattern, repost_text)
                        links_collector.extend(_)
                    if links_collector:
                        links_in_post = ", ".join(links_collector)

                try:
                    post_info = {
                        "Группа": link,
                        "Пост": f'vk.com/wall{post["owner_id"]}_{post["id"]}',
                        "Текст оригинального поста": f"'{(original_post_text if original_post_text else post_text)}",
                        "Дата-время": datetime.fromtimestamp(post["date"]).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "Лайки": post["likes"].get("count") if post.get("likes") else 0,
                        "Репосты": post["reposts"].get("count") if post.get("reposts") else 0,
                        "Комментарии": post["comments"].get("count") if post.get("comments") else 0,
                        "Просмотры": post["views"].get("count") if post.get("views") else 0,
                        "Текст репоста": repost_text,
                        "Ссылка на оригинальный пост": original_post_url,
                        "Ссылка в посте": links_in_post,
                    }
                    vk_posts_info_list.append(post_info)

                except Exception as error:
                    logger.error(
                        f"Ошибка при получении постов: {error},\n"
                        f'Проблемный пост: vk.com/wall{post["owner_id"]}_{post["id"]}'
                    )

        return vk_posts_info_list

    @property
    def vk_posts_links(self) -> List[str]:
        """
        Возвращает список ссылок на посты в группах ВКонтакте.

        Returns:
            List[str]: Список ссылок на посты.
        """
        return [post["Пост"] for post in self.vk_posts_info_list]


class VkChannelParser(BaseParser):
    """
    Парсер для получения информации о каналах ВКонтакте (количество подписчиков).
    """

    def __init__(self, links_list: List[str], google_service: Resource) -> None:
        """
        Инициализирует парсер со ссылками на каналы ВКонтакте.

        Args:
            links_list (List[str]): Список ссылок на каналы ВКонтакте.
        """
        self.channel_links = links_list
        self.vk_domain_pattern = re.compile(r".*vk\.com/(.*)/?\Z")
        self.url = "https://api.vk.com/method/groups.getMembers"
        self.token = os.getenv("VK_TOKEN")
        self.parser_flag = "vk_channel_parser"
        self.params = {
            "access_token": self.token,
            "v": "5.199",
        }
        self.google_service = google_service

    def get_channel_info(self, domain: str) -> Dict:
        """
        Получает информацию о канале ВКонтакте (например, количество подписчиков).

        Args:
            domain (str): Домен канала ВКонтакте.

        Returns:
            Dict: Информация о канале в виде словаря.
        """
        try:
            self.params["group_id"] = domain
            response = requests.get(self.url, params=self.params)
            if response.status_code != 200:
                sys.exit(f"Статус код не равен 200. Получен статус: {response.status_code}")
            data = response.json()
            return data
        except Exception as error:
            logger.error(f"Ошибка при получении информации о канале: {error}")

    @property
    def vk_channel_info_list(self) -> List[dict]:
        """
        Возвращает список с информацией о каналах ВКонтакте.

        Returns:
            List[dict]: Список словарей с данными о каналах.
        """
        vk_channel_info_list = []
        vk_channels_counter = len(self.channel_links)

        for link in self.channel_links:
            domain = self.vk_domain_pattern.match(link).group(1)

            logger.info(
                f"Получение подписчиков канала: {domain}. "
                f"Осталось {vk_channels_counter - 1} канал(-а, -ов)."
            )

            channel_data = self.get_channel_info(domain)
            if channel_data.get("error"):
                error_description = channel_data.get("error").get("error_msg")
                if error_description == "Access denied: group hide members":
                    logger.info(f"Канал {domain} скрыл количество подписчиков.")
                    vk_channels_counter -= 1
                    continue

            api_response = channel_data.get("response")

            data = {
                "Канал": link,
                "Дата-время": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Количество подписчиков": api_response.get("count", 0) if api_response else 0,
            }
            vk_channel_info_list.append(data)

            vk_channels_counter -= 1
        return vk_channel_info_list


class TgChannelParser(BaseParser):
    """
    Парсер для получения информации о Telegram каналах (количество подписчиков).
    """

    def __init__(
        self, channel_links: List[str], tg_client: TelegramClient, google_service: Resource
    ) -> None:
        """
        Инициализирует парсер с ссылками на каналы и клиентом Telegram.

        Args:
            channel_links (List[str]): Список ссылок на Telegram каналы.
            tg_client (TelegramClient): Telegram клиент для взаимодействия с API.
        """
        self.channel_links = channel_links
        self.tg_client = tg_client
        self.parser_flag = "tg_channel_parser"
        self.google_service = google_service

    @property
    async def tg_channels_info_list(self) -> List[Dict]:
        """
        Возвращает список с информацией о Telegram каналах.

        Returns:
            List[Dict]: Список словарей с данными о каналах.
        """
        tg_channels_info_list = []

        tg_groups_counter = len(self.channel_links)

        for channel_link in self.channel_links:
            time.sleep(1)  # Пауза между запросами
            logger.info(
                f"Получение подписчиков канала: {channel_link}."
                f"Осталось {tg_groups_counter - 1} канал(-а, -ов)."
            )

            try:

                result = await self.tg_client(
                    functions.channels.GetFullChannelRequest(channel_link)
                )

            except Exception as e:
                print(f"Произошла ошибка при обработке канала {channel_link}: {e}")
                tg_groups_counter -= 1
                continue

            subscribers_count = result.full_chat.participants_count

            channel_info = {
                "Канал": channel_link,
                "Дата-время": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Количество подписчиков": subscribers_count,
            }

            tg_channels_info_list.append(channel_info)

            tg_groups_counter -= 1

        return tg_channels_info_list

    async def run_tg_channels_parser(self, tg_channels_spreadsheet_id):
        """
        Запускает парсер для обработки информации о Telegram каналах и записи её в Google Sheets.

        Args:
            tg_channels_spreadsheet_id (str): Идентификатор таблицы Google Sheets.

        Returns:
            None
        """

        channels_info_list = await self.tg_channels_info_list
        self._insert_new_rows(
            spreadsheet_id=tg_channels_spreadsheet_id,
            rows_to_insert=channels_info_list,
            parser_flag=self.parser_flag,
        )
