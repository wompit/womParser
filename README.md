# womScraper (//ww Bizin.eu)

Инструмент для парсинга сайтов `*.bizin.eu`.  
Извлекает название компании, сайт, e-mail (включая скрытые Cloudflare), страну и сохраняет результат в CSV.  
Работает через **Selenium** + **undetected-chromedriver**.

---

## 🔧 Возможности

- Сбор данных с `xx.bizin.eu` (xx = код страны).
- Извлечение:
  - название компании;
  - e-mail (в т.ч. Cloudflare email obfuscation);
  - сайт компании (внешние ссылки, исключая соцсети);
  - страна (по поддомену).
- Автосохранение прогресса.
- Автоматический перезапуск браузера.
- Поддержка `headless`-режима и отключения картинок.

---

## 📦 Установка

```bash
git clone https://github.com/yourusername/bizin-scraper.git
cd bizin-scraper
pip install -r requirements.txt
Если файла requirements.txt нет, установите напрямую:
pip install pandas selenium undetected-chromedriver

🚀 Запуск

python scraper.py --input input.csv --output result.csv [опции]
Аргументы
Аргумент	Описание	По умолчанию
--input	обяз. CSV со ссылками (колонка url)	—
--output	обяз. CSV для результата	—
--profile-dir	Папка для профиля Chrome (сохраняет куки/сессии)	""
--chrome-binary	Путь к chrome.exe	""
--headless	Запуск без интерфейса	False
--disable-images	Отключить загрузку картинок	False
--page-timeout	Таймаут загрузки страницы (сек)	60
--retries	Кол-во повторных попыток	2
--flush-every	Каждые N записей сохранять результат	200
--batch-size	Сколько URL обрабатывать за один цикл	1200
--restart-every	Перезапуск драйвера каждые N запросов	350
--manual-cf	Ручное прохождение Cloudflare Turnstile (чекбокс)	False

📝 Примеры

Запуск в headless-режиме, без картинок, сохранение каждые 100 строк:
python scraper.py --input companies.csv --output result.csv --headless --disable-images --flush-every 100
С профилем Chrome:
python scraper.py --input companies.csv --output result.csv --profile-dir chrome_profile

📊 Форматы данных

Вход (input.csv)
url
https://de.bizin.eu/firm123
https://fr.bizin.eu/firm456
https://pl.bizin.eu/firm789
Выход (result.csv)
url,name,website,email,country
https://de.bizin.eu/firm123,Example GmbH,https://example.com,info@example.com,Germany

⚠️ Важно

При частых Cloudflare-челленджах используйте --manual-cf и проходите капчу вручную.
Скрипт автоматически делает паузы и перезапуски, чтобы снизить риск блокировки.
