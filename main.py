import os

from telethon import TelegramClient

from connections import get_google_connection
from notion_parser import TasksDbParser, UrlDbParser
from vk_tg_parsers import (
    TgChannelParser,
    TgPostsParser,
    VkChannelParser,
    VkPostsParser,
    get_links_from_google,
    logger,
)

if __name__ == "__main__":
    logger.info("Парсеры запущены")

    # Запуск TG, VK парсеров

    # Переменные окружения
    API_ID = int(os.getenv("TG_API_ID"))
    API_HASH = os.getenv("TG_API_HASH")
    vk_posts_spreadsheet_id = os.getenv("VK_POSTS_SPREADSHEET_ID")
    tg_posts_spreadsheet_id = os.getenv("TG_POSTS_SPREADSHEET_ID")
    vk_channels_spreadsheet_id = os.getenv("VK_CHANNELS_SPREADSHEET_ID")
    tg_channels_spreadsheet_id = os.getenv("TG_CHANNELS_SPREADSHEET_ID")

    # Подключаемся к Google Sheets API
    google_service = get_google_connection()

    # Получаем списки ссылок для парсинга из Google Sheets
    posts_parsing_links, channels_parsing_links = get_links_from_google(google_service)

    vk_posts_parsing_links = None
    tg_posts_parsing_links = None
    if posts_parsing_links:
        vk_posts_parsing_links = [elem for elem in posts_parsing_links if "vk.com" in elem]
        tg_posts_parsing_links = [elem for elem in posts_parsing_links if "t.me" in elem]

    vk_channels_parsing_links = None
    tg_channels_parsing_links = None
    if channels_parsing_links:
        vk_channels_parsing_links = [elem for elem in channels_parsing_links if "vk.com" in elem]
        tg_channels_parsing_links = [elem for elem in channels_parsing_links if "t.me" in elem]

    # Блок обработки ВК каналов и постов
    if vk_posts_parsing_links:
        logger.info(f"Парсим посты из ВК каналов. Общее количество: {len(vk_posts_parsing_links)}")
        vk_posts_parser = VkPostsParser(
            links_list=vk_posts_parsing_links, google_service=google_service
        )
        vk_posts_parser.run_google_upsert(
            spreadsheet_id=vk_posts_spreadsheet_id,
            posts_info_list=vk_posts_parser.vk_posts_info_list,
            parser_flag=vk_posts_parser.parser_flag,
        )

    if vk_channels_parsing_links:
        logger.info(
            f"Парсим подписчиков ВК каналов. Общее количество: {len(vk_channels_parsing_links)}"
        )
        vk_channels_parser = VkChannelParser(
            links_list=vk_channels_parsing_links, google_service=google_service
        )
        vk_channels_parser._insert_new_rows(
            spreadsheet_id=vk_channels_spreadsheet_id,
            rows_to_insert=vk_channels_parser.vk_channel_info_list,
            parser_flag=vk_channels_parser.parser_flag,
        )

    # Блок обработки ТГ каналов и постов
    if tg_posts_parsing_links or tg_channels_parsing_links:
        tg_client = TelegramClient("my_session", API_ID, API_HASH)
        tg_client.start()

        with tg_client:
            if tg_posts_parsing_links:
                logger.info(f"Парсим посты из ТГ. Общее количество: {len(tg_posts_parsing_links)}")
                tg_posts_parser = TgPostsParser(
                    channel_links=tg_posts_parsing_links,
                    tg_client=tg_client,
                    google_service=google_service,
                )
                tg_client.loop.run_until_complete(
                    tg_posts_parser.run_tg_posts_parser(tg_posts_spreadsheet_id)
                )

            if tg_channels_parsing_links:
                logger.info(
                    f"Парсим подписчиков ТГ каналов. "
                    f"Общее количество: {len(tg_channels_parsing_links)}"
                )
                tg_channels_parser = TgChannelParser(
                    channel_links=tg_channels_parsing_links,
                    tg_client=tg_client,
                    google_service=google_service,
                )
                tg_client.loop.run_until_complete(
                    tg_channels_parser.run_tg_channels_parser(tg_channels_spreadsheet_id)
                )
    google_service.close()

    # Запуск Notion парсеров
    logger.info("Начинаем парсинг Notion")
    notion_tasks = TasksDbParser()
    notion_tasks.run()

    notion_url = UrlDbParser()
    notion_url.run()

    logger.info("Все парсеры завершили работу")
