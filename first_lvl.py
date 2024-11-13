from html.parser import HTMLParser
from urllib.request import urlopen
from urllib.parse import quote, urlparse, urlunparse, unquote
from urllib.error import URLError, HTTPError

class LinkCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.in_target_div = False  # Флаг, указывающий, что мы находимся в нужном <div>
        self.div_stack = 0  # Стек для отслеживания уровня вложенности целевого <div>

    def handle_starttag(self, tag, attrs):
        # Проверка на начальный тег <div> с нужным классом
        if tag == 'div' and ('class', 'mw-content-ltr mw-parser-output') in attrs:
            self.in_target_div = True
            self.div_stack += 1  # Увеличиваем уровень вложенности при нахождении целевого <div>

        # Если мы находимся внутри нужного <div>, ищем ссылки <a>
        elif self.in_target_div and tag == 'a':
            for attr, value in attrs:
                if attr == 'href' and not value.startswith('#'):  # Исключаем ссылки, начинающиеся с #
                    self.links.append(value)

        # Увеличиваем уровень вложенности для всех вложенных <div>, если уже находимся в нужном блоке
        elif tag == 'div' and self.in_target_div:
            self.div_stack += 1

    def handle_endtag(self, tag):
        # Когда встречаем закрывающий тег </div> внутри нужного блока, уменьшаем уровень вложенности
        if tag == 'div' and self.in_target_div:
            self.div_stack -= 1
            if self.div_stack == 0:
                self.in_target_div = False  # Выходим из нужного <div> только при возврате на начальный уровень

def fetch_links(url):
    parsed_url = urlparse(url)
    encoded_path = quote(parsed_url.path)
    encoded_url = urlunparse((parsed_url.scheme, parsed_url.netloc, encoded_path, '', '', ''))

    try:
        with urlopen(encoded_url) as response:
            html = response.read().decode('utf-8')
    except HTTPError as e:
        print(f"HTTP Error: {e.code} for URL: {encoded_url}")
        return []
    except URLError as e:
        print(f"URL Error: {e.reason} for URL: {encoded_url}")
        return []

    parser = LinkCollector()
    parser.feed(html)
    return parser.links

def save_links_to_file(links, filename="links.txt"):
    base_url = "https://ru.wikipedia.org"  # Базовый URL
    with open(filename, "w", encoding="utf-8") as file:
        for link in links:
            # Проверяем, является ли ссылка абсолютной
            if link.startswith("http://") or link.startswith("https://"):
                full_url = unquote(link)  # Используем как есть, если это полный URL
            else:
                full_url = base_url + unquote(link)  # Добавляем базовый URL для относительных ссылок
            file.write(full_url + "\n")

# Основная часть программы
if __name__ == "__main__":
    # url = input("Введите URL страницы: ")
    links = fetch_links('https://ru.wikipedia.org/wiki/Политические_репрессии_в_СССР')
    filename = "links.txt"  # Указываем имя файла здесь
    save_links_to_file(links, filename)  # Сохраняем ссылки в файл
    print(f"Найденные ссылки сохранены в {filename}")
