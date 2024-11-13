import sqlite3
from urllib.parse import urljoin, urlparse, unquote, quote
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re
import threading

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'
]

EXCLUDED_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar')
EXCLUDE_PARAMS_REGEX = re.compile(r"[?&](action|veaction)=")

db_lock = threading.Lock()

def setup_database():
    conn = sqlite3.connect("links.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS links (
            url TEXT PRIMARY KEY,
            level INTEGER,
            processed INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def save_links_to_db(links, level):
    with db_lock:
        conn = sqlite3.connect("links.db")
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR IGNORE INTO links (url, level, processed)
            VALUES (?, ?, 0)
        ''', [(unquote(url), level) for url in links])
        conn.commit()
        conn.close()

def mark_as_processed(url):
    with db_lock:
        conn = sqlite3.connect("links.db")
        cursor = conn.cursor()
        cursor.execute('UPDATE links SET processed = 1 WHERE url = ?', (url,))
        conn.commit()
        conn.close()

def fetch_links(url):
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    try:
        parsed_url = urlparse(url)
        encoded_path = quote(parsed_url.path)
        encoded_url = parsed_url._replace(path=encoded_path).geturl()

        req = Request(encoded_url, headers=headers)
        with urlopen(req) as response:
            html = response.read().decode('utf-8')
            return extract_links(url, html)
    except Exception as e:
        print(f"Ошибка при загрузке {url}: {e}")
        return set()


def extract_links(base_url, html):
    links = set()
    href_pattern = re.compile(r'href=["\'](.*?)["\']', re.IGNORECASE)

    # Список префиксов и ключевых слов для исключения
    excluded_prefixes = ("Служебная:", "Википедия:", "Обсуждение:", "Шаблон:", "Категория:", "Википедия:", "Справка:", "Портал:")

    for match in href_pattern.findall(html):
        href = match
        if not href.startswith('#') and not EXCLUDE_PARAMS_REGEX.search(href):
            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)

            # Извлекаем часть после /wiki/ и декодируем её
            path_after_wiki = unquote(parsed_url.path.split("/wiki/")[-1])

            # Проверка на наличие исключенных префиксов в части пути после /wiki/
            if ("wikipedia.org" in parsed_url.netloc and
                    not parsed_url.netloc.startswith("web.archive") and
                    not any(full_url.endswith(ext) for ext in EXCLUDED_EXTENSIONS) and
                    "action=edit" not in full_url and
                    "&amp" not in full_url and
                    not any(path_after_wiki.startswith(prefix) for prefix in excluded_prefixes)):
                links.add(full_url)
    return links


def crawl_links(start_url, max_depth):
    setup_database()
    save_links_to_db([start_url], 0)  # Сохраняем начальную ссылку как уровень 0

    def process_level(level):
        with db_lock:
            conn = sqlite3.connect("links.db")
            cursor = conn.cursor()
            cursor.execute('SELECT url FROM links WHERE level = ? AND processed = 0', (level,))
            links_to_check = [row[0] for row in cursor.fetchall()]
            conn.close()

        if not links_to_check:
            return

        new_links = set()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_links, url): url for url in links_to_check}
            for future in as_completed(futures):
                url = futures[future]
                print(f"Обрабатываем ссылку: {url} (Уровень: {level})")
                try:
                    found_links = future.result()
                    new_links.update(found_links)
                    mark_as_processed(url)
                except Exception as e:
                    print(f"Ошибка при обработке {url}: {e}")

        if new_links:
            save_links_to_db(new_links, level + 1)

    for level in range(0, max_depth):
        process_level(level)

if __name__ == "__main__":
    start_url = "https://ru.wikipedia.org/wiki/4_января"
    max_depth = 6

    crawl_links(start_url, max_depth)
    print("Результаты сохранены в базе данных.")
