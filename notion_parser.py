import logging
import os
from typing import Dict, List

import apiclient.discovery
import httplib2
import requests
from dotenv import load_dotenv
from googleapiclient.discovery import Resource
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

# Инициализируем логгер
logger = logging.getLogger("NOTION_PARSER")
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(name)s:%(levelname)s] --> %(message)s"
)


class BaseNotionParser:

    @property
    def notion_api_url(self) -> str:
        """
        Возвращает URL для запроса к базе данных Notion.

        Returns:
        --------
        str: URL для обращения к API Notion.
        """
        return f"https://api.notion.com/v1/databases/{self.NOTION_DB_ID}/query"

    @property
    def headers(self) -> Dict[str, str]:
        """
        Возвращает заголовки для запросов к API Notion.

        Returns:
        --------
        dict: Заголовки для авторизации и указания версии API Notion.
        """
        return {
            "Authorization": f"Bearer {self.NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }

    @property
    def proxies(self) -> Dict[str, str]:
        """
        Возвращает конфигурацию для использования прокси-сервера.

        Returns:
        --------
        dict: Настройки прокси для запросов.
        """
        return {
            "https": f"http://{self.PROXY_LOGIN}:{self.PROXY_PASS}@"
            f"{self.PROXY_IP}:{self.PROXY_PORT}"
        }

    def get_records_by_batch(self) -> Dict:
        """
        Получает записи из базы данных Notion партиями с помощью пагинации.

        Yields:
        -------
        dict: Каждая запись базы данных Notion.
        """
        next_cursor = None
        has_more = True

        counter = 0

        logger.info(f"Получение записей из базы {self.db_name}")

        while has_more:
            # Формирование тела запроса с учетом пагинации
            payload = {}
            if next_cursor:
                payload["start_cursor"] = next_cursor

            # Выполнение запроса
            response = requests.post(
                self.notion_api_url, proxies=self.proxies, headers=self.headers, json=payload
            )
            data = response.json()

            # Обработка текущей партии данных
            for result in data["results"]:
                yield result

            counter += len(data["results"])

            # Проверяем, есть ли ещё данные для запроса
            has_more = data.get("has_more", False)
            next_cursor = data.get("next_cursor")
        logger.info(f"Обработанно {counter} записей")

    def _get_row_from_table(self, row: Dict) -> List[str]:
        """
        Преобразует запись из базы данных Notion в строку для Google таблицы.

        Parameters:
        -----------
        row : dict
            Запись из Notion.

        Returns:
        --------
        list of str: Строка, готовая для вставки в Google таблицу.
        """
        match self.parser_flag:
            case "tasks":
                return [
                    row["Задача"],
                    row["Дедлайн [Р]"],
                    row["Назначен"],
                    row["Статус [Р]"],
                    row["Публикация (автоматическое поле)"],
                    row["UTM"],
                    row["organic"],
                    row["Гибкая публикация"],
                    row["Дедлайн [К-П]"],
                    row["Статус [К-П]"],
                    row["Срочность [Дизайн]"],
                    row["Статус [Дайджест]"],
                    row["Статус [Д]"],
                    row["Сегмент"],
                    row["Дедлайн[Д]"],
                    row["Группа"],
                    row["Не использовать 1"],
                    row["Не использовать 2"],
                    row["Не использовать 3"],
                    # row["URL Database"],
                    row["Компания"],
                ]
            case "urls":
                return [
                    row[""],
                    row["URL"],
                    row["URL&UTM"],
                    row["Группа"],
                    row["utm_medium"],
                    row["utm_campaign"],
                    row["utm_tern"],
                    row["Database ID"],
                    row["Инфоповод"],
                    row["Направления"],
                    row["Owner"],
                    row["Сегмент"],
                    row["Основная задача"],
                ]

    def _make_sheet_ready_data(self, table_info_list) -> List[List[str]]:
        """
        Преобразует список записей Notion в формат, готовый для записи в Google таблицу.

        Parameters:
        -----------
        table_info_list : list of dict
            Список записей из базы данных Notion.

        Returns:
        --------
        list of list of str: Преобразованный список записей для вставки в таблицу.
        """
        rebuild_data = []
        for elem in table_info_list:
            row = self._get_row_from_table(elem)
            rebuild_data.append(row)
        return rebuild_data

    @property
    def google_service(self) -> Resource:
        """
        Создает объект службы Google Sheets API для выполнения операций с таблицами.
        Кеширует подключение для последующих вызовов.

        Returns: Resource
            Объект подключения к Google Sheets API.
        """
        if self._google_service is None:
            credentials_google_file = os.getenv("CREDENTIALS_GOOGLE_FILE")
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                credentials_google_file,
                [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            httpAuth = credentials.authorize(httplib2.Http())
            self._google_service = apiclient.discovery.build("sheets", "v4", http=httpAuth)
        return self._google_service

    def _get_table_headers(self) -> List[List[str]]:
        """
        Возвращает заголовки таблицы.

        Returns:
        --------
        list of str: Заголовки таблицы.
        """
        match self.parser_flag:
            case "tasks":
                return [
                    [
                        "Задача",
                        "Дедлайн [Р]",
                        "Назначен",
                        "Статус [Р]",
                        "Публикация (автоматическое поле)",
                        "UTM",
                        "organic",
                        "Гибкая публикация",
                        "Дедлайн [К-П]",
                        "Статус [К-П]",
                        "Срочность [Дизайн]",
                        "Статус [Дайджест]",
                        "Статус [Д]",
                        "Сегмент",
                        "Дедлайн[Д]",
                        "Группа",
                        "Не использовать 1",
                        "Не использовать 2",
                        "Не использовать 3",
                        # "URL Database",
                        "Компания",
                    ]
                ]
            case "urls":
                return [
                    [
                        "",
                        "URL",
                        "URL&UTM",
                        "Группа",
                        "utm_medium",
                        "utm_campaign",
                        "utm_tern",
                        "Database ID",
                        "Инфоповод",
                        "Направления",
                        "Owner",
                        "Сегмент",
                        "Основная задача",
                    ]
                ]

    def _google_sheet_handling(self) -> None:
        """
        Очищает лист в Google таблице и загружает новые данные.

        Очищает все существующие данные на листе и записывает новые данные из базы данных Notion.

        Returns: None
        """
        # Подготавливаем данные для вставки
        rebuild_data = self._make_sheet_ready_data(self.table_info_list)

        # Формируем данные для записи (начинаем с первой строки)
        ready_data = self._get_table_headers()
        ready_data.extend(rebuild_data)

        # Очищаем все данные на листе перед вставкой новых
        clear_range = "Лист1"
        self.google_service.spreadsheets().values().clear(
            spreadsheetId=self.NOTION_SPREADSHEET_ID, range=clear_range
        ).execute()

        # Вставляем данные в таблицу, начиная с первой строки
        number_sheet = "Лист1!A1"
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {
                    "range": number_sheet,
                    "majorDimension": "ROWS",
                    "values": ready_data,
                }
            ],
        }

        # Выполняем вставку данных
        self.google_service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.NOTION_SPREADSHEET_ID, body=body
        ).execute()

        self._google_service.close()

        logger.info(f"Запись в таблицу завершена. Количество записей: {len(rebuild_data)}")

    def run(self) -> None:
        """
        Запускает процесс извлечения данных из Notion и их загрузки в Google таблицу.

        Returns: None
        """
        self._google_sheet_handling()


class TasksDbParser(BaseNotionParser):
    """
    Класс для извлечения данных из базы данных Notion (Tasks DB) и записи их в Google таблицу.

    Атрибуты:
    ----------
     _google_service: Resource
        Переменная для хранения подключения к Google Sheets API.
    NOTION_TOKEN : str
        Токен доступа к Notion API.
    NOTION_DB_ID : str
        Идентификатор базы данных Notion.
    PROXY_LOGIN : str
        Логин для прокси-сервера.
    PROXY_PASS : str
        Пароль для прокси-сервера.
    PROXY_IP : str
        IP адрес прокси-сервера.
    PROXY_PORT : str
        Порт прокси-сервера.
    NOTION_SPREADSHEET_ID : str
        Идентификатор Google таблицы для записи данных.
    parser_flag : str
        Признак парсера, используемый для корректной подготовки данных для записи в Google таблицу.
    db_name: str
        Название базы данных, используемое для логирования.
    """

    _google_service: Resource = None

    NOTION_TOKEN: str = os.getenv("NOTION_TOKEN")
    NOTION_DB_ID: str = os.getenv("NOTION_TASKS_DB_ID")

    PROXY_LOGIN: str = os.getenv("PROXY_LOGIN")
    PROXY_PASS: str = os.getenv("PROXY_PASS")
    PROXY_IP: str = os.getenv("PROXY_IP")
    PROXY_PORT: str = os.getenv("PROXY_PORT")

    NOTION_SPREADSHEET_ID: str = os.getenv("NOTION_TASKS_SPREADSHEET_ID")

    parser_flag: str = "tasks"
    db_name = "Tasks Database"

    @property
    def table_info_list(self) -> List[Dict]:
        """
        Формирует список записей из базы данных Notion для дальнейшей обработки.

        Returns:
        --------
        list of dict: Список словарей с необходимыми полями из Notion.
        """
        table_info_list = []

        for record in self.get_records_by_batch():
            properties = record["properties"]

            # Колонка "Задача"
            task = None
            if properties["Задача"].get("title"):
                task = properties["Задача"].get("title")[0].get("plain_text")

            # Колонка "Дедлайн [Р]"
            p_deadline = None
            if properties["Дедлайн [Р]"].get("date"):
                p_deadline = properties["Дедлайн [Р]"].get("date").get("start")

            # Колонка "Назначен"
            executor = None
            if properties["Назначен"].get("people"):
                persons_list = []
                for person in properties["Назначен"].get("people"):
                    persons_list.append(person.get("name"))
                executor = ", ".join([_ for _ in persons_list if _ is not None])

            # Колонка "Статус [Р]"
            p_status = None
            if properties["Статус [Р]"].get("select"):
                p_status = properties["Статус [Р]"].get("select").get("name")

            # Колонка "Публикация"
            auto_publication_date = None
            if properties["Публикация (автоматическое поле)"].get("date"):
                auto_publication_date = (
                    properties["Публикация (автоматическое поле)"].get("date").get("start")
                )

            # Колонка "UTM"
            utm = None
            if properties["UTM"].get("formula"):
                utm = properties["UTM"].get("formula").get("string")

            # Колонка "organic"
            organic = properties["organic"].get("unique_id").get("number")

            # Колонка "Гибкая публикация"
            flexible_publication = None
            match properties["Гибкая публикация"].get("checkbox"):
                case True:
                    flexible_publication = "Да"
                case False:
                    flexible_publication = "Нет"

            # Колонка "Дедлайн [К-П]"
            k_p_deadline = None
            if properties["Дедлайн [К-П]"].get("date"):
                k_p_deadline = properties["Дедлайн [К-П]"].get("date").get("start")

            # Колонка "Статус [К-П]"
            k_p_status = None
            if properties["Статус [К-П]"].get("select"):
                k_p_status = properties["Статус [К-П]"].get("select").get("name")

            # Колонка "Срочность [Дизайн]"
            design_priority = None
            if properties["Срочность [Дизайн]"].get("select"):
                design_priority = properties["Срочность [Дизайн]"].get("select").get("name")

            # Колонка "Статус [Дайджест]"
            digest_status = None
            if properties["Статус [Дайджест]"].get("select"):
                digest_status = properties["Статус [Дайджест]"].get("select").get("name")

            # Колонка "Статус [Д]"
            d_status = None
            if properties["Статус [Д]"].get("select"):
                d_status = properties["Статус [Д]"].get("select").get("name")

            # Колонка "Сегмент"
            segment = None
            if properties["Сегмент"].get("multi_select"):
                segments_list = []
                for segment in properties["Сегмент"].get("multi_select"):
                    segments_list.append(segment.get("name"))
                segment = ", ".join([_ for _ in segments_list if _ is not None])

            # Колонка "Дедлайн[Д]"
            d_deadline = None
            if properties["Дедлайн[Д]"].get("date"):
                d_deadline = properties["Дедлайн[Д]"].get("date").get("start")

            # Колонка "Группа"
            group = None
            if properties["Группа"].get("multi_select"):
                groups_list = []
                for group in properties["Группа"].get("multi_select"):
                    groups_list.append(group.get("name"))
                group = ", ".join([_ for _ in groups_list if _ is not None])

            # Колонка "Не использовать 1"
            not_for_use_1 = None
            if properties["Не использовать 1"].get("people"):
                not_for_use_1_list = []
                for elem in properties["Не использовать 1"].get("people"):
                    not_for_use_1_list.append(elem.get("name"))
                not_for_use_1 = ", ".join(not_for_use_1_list)

            # Колонка "Не использовать 2"
            not_for_use_2 = None
            if properties["Не использовать 2"].get("people"):
                not_for_use_2_list = []
                for elem in properties["Не использовать 2"].get("people"):
                    not_for_use_2_list.append(elem.get("name"))
                not_for_use_2 = ", ".join(not_for_use_2_list)

            # Колонка "Не использовать 3"
            not_for_use_3 = None
            if properties["Не использовать 3"].get("people"):
                not_for_use_3_list = []
                for elem in properties["Не использовать 3"].get("people"):
                    not_for_use_3_list.append(elem.get("name"))
                not_for_use_3 = ", ".join(not_for_use_3_list)

            # # Колонка "URL Database"
            # url_database = None
            # if properties["URL Database"].get("relation"):
            #     url_database = properties["URL Database"].get("url")

            # Колонка "Компания"
            company = None
            if properties["Компания"].get("multi_select"):
                company_list = []
                for elem in properties["Компания"].get("multi_select"):
                    company_list.append(elem.get("name"))
                company = ", ".join([_ for _ in company_list if _ is not None])

            data = {
                "Задача": task,
                "Дедлайн [Р]": p_deadline,
                "Назначен": executor,
                "Статус [Р]": p_status,
                "Публикация (автоматическое поле)": auto_publication_date,
                "UTM": utm,
                "organic": organic,
                "Гибкая публикация": flexible_publication,
                "Дедлайн [К-П]": k_p_deadline,
                "Статус [К-П]": k_p_status,
                "Срочность [Дизайн]": design_priority,
                "Статус [Дайджест]": digest_status,
                "Статус [Д]": d_status,
                "Сегмент": segment,
                "Дедлайн[Д]": d_deadline,
                "Группа": group,
                "Не использовать 1": not_for_use_1,
                "Не использовать 2": not_for_use_2,
                "Не использовать 3": not_for_use_3,
                # "URL Database": url_database,
                "Компания": company,
            }

            table_info_list.append(data)

        return table_info_list


class UrlDbParser(BaseNotionParser):
    _google_service: Resource = None

    NOTION_TOKEN: str = os.getenv("NOTION_TOKEN")
    NOTION_DB_ID: str = os.getenv("NOTION_URL_DB_ID")

    PROXY_LOGIN: str = os.getenv("PROXY_LOGIN")
    PROXY_PASS: str = os.getenv("PROXY_PASS")
    PROXY_IP: str = os.getenv("PROXY_IP")
    PROXY_PORT: str = os.getenv("PROXY_PORT")

    NOTION_SPREADSHEET_ID: str = os.getenv("NOTION_URL_DB_SPREADSHEET_ID")

    parser_flag: str = "urls"
    db_name = "URL Database"

    @property
    def table_info_list(self) -> List[Dict]:
        """
        Формирует список записей из базы данных Notion для дальнейшей обработки.

        Returns:
        --------
        list of dict: Список словарей с необходимыми полями из Notion.
        """
        table_info_list = []

        for record in self.get_records_by_batch():
            properties = record["properties"]

            # Колонка ""
            title = None
            if properties[""].get("title"):
                title = properties[""].get("title")[0].get("plain_text")

            # Колонка "URL"
            url = None
            if properties["URL"].get("url"):
                url = properties["URL"].get("url")

            # Колонка "URL&UTM"
            url_utm = None
            if properties["URL&UTM"].get("formula"):
                url_utm = properties["URL&UTM"].get("formula").get("string")

            # Колонка "Группа"
            group = None
            if properties["Группа"].get("rollup").get("array"):
                group_list = []
                for elem in properties["Группа"].get("rollup").get("array"):
                    for _ in elem["multi_select"]:
                        group_list.append(_.get("name"))
                group = ", ".join([_ for _ in group_list if _ is not None])

            # Колонка "utm_medium"
            utm_medium = None
            if properties["utm_medium"].get("formula"):
                utm_medium = properties["utm_medium"].get("formula").get("string")

            # Колонка "utm_campaign"
            utm_campaign = None
            if properties["utm_campaign"].get("formula"):
                utm_campaign = properties["utm_campaign"].get("formula").get("string")

            # Колонка "utm_tern"
            utm_tern = None
            if properties["utm_tern"].get("rollup").get("array"):
                utm_tern_list = []
                for elem in properties["utm_tern"].get("rollup").get("array"):
                    utm_tern_list.append(elem.get("date").get("start"))
                utm_tern = ", ".join([_ for _ in utm_tern_list if _ is not None])

            # Колонка "Database ID"
            database_id = None
            if properties["Database ID"].get("rollup").get("array"):
                database_id_list = []
                for elem in properties["Database ID"].get("rollup").get("array"):
                    database_id_list.append(elem.get("unique_id").get("number"))
                database_id = ", ".join([str(_) for _ in database_id_list if _ is not None])

            # Колонка "Инфоповод"
            info_reason = None
            if properties["Инфоповод"].get("select"):
                info_reason = properties["Инфоповод"].get("select").get("name")

            # Колонка "Направления"
            courses = None
            if properties["Направления"].get("select"):
                courses = properties["Направления"].get("select").get("name")

            # Колонка "Owner"
            owner = None
            if properties["Owner"].get("people"):
                owner_list = []
                for person in properties["Owner"].get("people"):
                    owner_list.append(person.get("name"))
                owner = ", ".join([_ for _ in owner_list if _ is not None])

            # Колонка "Сегмент"
            segment = None
            if properties["Сегмент"].get("select"):
                segment = properties["Сегмент"].get("select").get("name")

            # Колонка "Основная задача"
            main_task = None
            if properties["Основная задача"].get("relation"):
                main_task_list = []
                for elem in properties["Основная задача"].get("relation"):
                    task_id = elem.get("id")
                    main_task_list.append(task_id)
                main_task = ", ".join([_ for _ in main_task_list if _ is not None])

            data = {
                "": title,
                "URL": url,
                "URL&UTM": url_utm,
                "Группа": group,
                "utm_medium": utm_medium,
                "utm_campaign": utm_campaign,
                "utm_tern": utm_tern,
                "Database ID": database_id,
                "Инфоповод": info_reason,
                "Направления": courses,
                "Owner": owner,
                "Сегмент": segment,
                "Основная задача": main_task,
            }

            table_info_list.append(data)

        return table_info_list
