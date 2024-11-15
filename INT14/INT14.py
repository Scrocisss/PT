import sqlite3
from urllib.parse import urljoin, urlparse, unquote, quote
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re
import threading
import time
import queue
from typing import Set, List, Tuple

USER_AGENTS: List[str] = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
]

EXCLUDED_EXTENSIONS: Tuple[str, ...] = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar')
EXCLUDE_PARAMS_REGEX: re.Pattern = re.compile(r"[?&](action|veaction)=")

db_lock = threading.Lock()
visited_topics: Set[str] = set()
BATCH_SIZE: int = 1000  # Размер батча для массового сохранения ссылок
save_queue: queue.Queue[Tuple[str, int]] = queue.Queue()  # Очередь для ссылок, ожидающих сохранения
save_thread_stop_event = threading.Event()  # Событие для завершения потока сохранения

def setup_database() -> None:
    """Создает базу данных и таблицу, если они не существуют."""
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

def save_links_worker() -> None:
    """Фоновый поток для асинхронного сохранения ссылок в базу данных."""
    while not save_thread_stop_event.is_set() or not save_queue.empty():
        batch: List[Tuple[str, int]] = []
        while len(batch) < BATCH_SIZE and not save_queue.empty():
            try:
                batch.append(save_queue.get(timeout=5))
            except queue.Empty:
                break
        if batch:
            with db_lock:
                conn = sqlite3.connect("links.db")
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT OR IGNORE INTO links (url, level, processed)
                    VALUES (?, ?, 0)
                ''', batch)
                conn.commit()
                conn.close()

def save_links_to_db(links: Set[str], level: int) -> None:
    """Добавляет ссылки в очередь на сохранение."""
    for url in links:
        save_queue.put((unquote(url), level))

def mark_as_processed(url: str) -> None:
    """Отмечает ссылку как обработанную в базе данных."""
    with db_lock:
        conn = sqlite3.connect("links.db")
        cursor = conn.cursor()
        cursor.execute('UPDATE links SET processed = 1 WHERE url = ?', (url,))
        conn.commit()
        conn.close()

def fetch_links(url: str) -> Set[str]:
    """Получает ссылки со страницы по URL."""
    time.sleep(random.uniform(0.5, 1.5))
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

def extract_links(base_url: str, html: str) -> Set[str]:
    """Извлекает ссылки из HTML-страницы."""
    links: Set[str] = set()
    href_pattern = re.compile(r'href=["\'](.*?)["\']', re.IGNORECASE)

    for match in href_pattern.findall(html):
        href = match
        if not href.startswith('#') and not EXCLUDE_PARAMS_REGEX.search(href):
            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)

            if "wikipedia.org" not in parsed_url.netloc:
                continue

            path_after_wiki = unquote(parsed_url.path.split("/wiki/")[-1])

            if ':' in path_after_wiki:
                continue

            topic = path_after_wiki
            if topic in visited_topics:
                continue
            visited_topics.add(topic)

            if (not any(full_url.endswith(ext) for ext in EXCLUDED_EXTENSIONS) and
                "action=edit" not in full_url and "&amp" not in full_url):
                links.add(full_url)
    return links

def crawl_links(start_url: str, max_depth: int) -> None:
    """Начинает процесс обхода ссылок с заданной глубиной."""
    setup_database()
    save_links_to_db({start_url}, 0)

    # Запускаем поток для асинхронного сохранения в базу данных
    save_thread = threading.Thread(target=save_links_worker, daemon=True)
    save_thread.start()

    def process_level(level: int) -> None:
        with db_lock:
            conn = sqlite3.connect("links.db")
            cursor = conn.cursor()
            cursor.execute('SELECT url FROM links WHERE level = ? AND processed = 0', (level,))
            links_to_check: List[str] = [row[0] for row in cursor.fetchall()]
            conn.close()

        if not links_to_check:
            return

        new_links: Set[str] = set()
        max_workers = max(5, 20 - level * 2)  # Уменьшаем количество потоков на следующих уровнях
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_links, url): url for url in links_to_check}
            for future in as_completed(futures):
                url = futures[future]
                try:
                    found_links = future.result()
                    new_links.update(found_links)
                    mark_as_processed(url)
                except Exception as e:
                    print(f"Ошибка при обработке {url}: {e}")

        if new_links:
            save_links_to_db(new_links, level + 1)
            print(f"Сохранено {len(new_links)} новых ссылок на уровне {level + 1}")

    for level in range(0, max_depth):
        print(f"Начинаем обработку уровня {level}")
        process_level(level)
        print(f"Завершена обработка уровня {level}")

    # Устанавливаем событие завершения для потока сохранения
    save_thread_stop_event.set()
    save_thread.join()
    print("Все данные сохранены в базе данных.")

if __name__ == "__main__":
    start_url: str = "https://ru.wikipedia.org/wiki/Российский_государственный_университет_нефти_и_газа"
    max_depth: int = 6

    crawl_links(start_url, max_depth)
    print("Результаты сохранены в базе данных.")
