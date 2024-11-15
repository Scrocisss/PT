import unittest
from unittest.mock import patch, MagicMock
import sqlite3
from INT14 import (
    setup_database, save_links_to_db, mark_as_processed, fetch_links, extract_links, crawl_links, save_queue
)
from urllib.parse import unquote

class TestSetupDatabase(unittest.TestCase):
    def setUp(self):
        setup_database()

    def test_setup_database(self):
        conn = sqlite3.connect("links.db")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='links';")
        table = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(table, "Таблица links должна быть создана")


class TestSaveLinksToDb(unittest.TestCase):
    def setUp(self):
        setup_database()
        save_queue.queue.clear()

    def test_save_links_to_db(self):
        links = {
            "https://ru.wikipedia.org/wiki/Московская_горная_академия",
            "https://ru.wikipedia.org/wiki/Губкин,_Иван_Михайлович",
            "https://ru.wikipedia.org/wiki/Индустриализация_в_СССР"
        }
        save_links_to_db(links, 1)
        self.assertFalse(save_queue.empty(), "Очередь для сохранения не должна быть пустой")


class TestMarkAsProcessed(unittest.TestCase):
    def setUp(self):
        setup_database()
        save_links_to_db({"https://ru.wikipedia.org/wiki/Московская_горная_академия"}, 1)

    def test_mark_as_processed(self):
        url = "https://ru.wikipedia.org/wiki/Московская_горная_академия"
        mark_as_processed(url)
        conn = sqlite3.connect("links.db")
        cursor = conn.cursor()
        cursor.execute("SELECT processed FROM links WHERE url = ?", (url,))
        row = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(row, "Запись должна существовать в базе данных")
        self.assertEqual(row[0], 1, "Ссылка должна быть помечена как обработанная")


class TestFetchLinks(unittest.TestCase):
    @patch("INT14.urlopen")
    def test_fetch_links(self, mock_urlopen):
        mock_html = '''
            <html><body>
            <a href="/wiki/Московская_горная_академия">Московская горная академия</a>
            <a href="/wiki/Губкин,_Иван_Михайлович">Губкин, Иван Михайлович</a>
            <a href="/wiki/Индустриализация_в_СССР">Индустриализация в СССР</a>
            </body></html>
        '''
        mock_urlopen.return_value.__enter__.return_value.read.return_value = mock_html.encode("utf-8")

        url = "https://ru.wikipedia.org/wiki/Российский_государственный_университет_нефти_и_газа"
        result = fetch_links(url)
        expected_links = {
            "https://ru.wikipedia.org/wiki/Московская_горная_академия",
            "https://ru.wikipedia.org/wiki/Губкин,_Иван_Михайлович",
            "https://ru.wikipedia.org/wiki/Индустриализация_в_СССР"
        }
        self.assertTrue(expected_links.issubset(result), "Должны быть возвращены корректные ссылки")


class TestCrawlLinks(unittest.TestCase):
    @patch("INT14.fetch_links")
    def test_crawl_links(self, mock_fetch_links):
        mock_fetch_links.side_effect = lambda url: {
            "https://ru.wikipedia.org/wiki/Московская_горная_академия",
            "https://ru.wikipedia.org/wiki/Губкин,_Иван_Михайлович",
            "https://ru.wikipedia.org/wiki/Индустриализация_в_СССР"
        }
        setup_database()
        start_url = "https://ru.wikipedia.org/wiki/Российский_государственный_университет_нефти_и_газа"
        crawl_links(start_url, max_depth=3)
        self.assertTrue(mock_fetch_links.called, "fetch_links должна быть вызвана")


if __name__ == "__main__":
    unittest.main()
