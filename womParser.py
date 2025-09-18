// main womParser

import argparse, time, random, re, sys
from pathlib import Path
import pandas as pd

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui   import WebDriverWait
from selenium.webdriver.support      import expected_conditions as EC
from selenium.common.exceptions      import TimeoutException, WebDriverException, SessionNotCreatedException

SOCIAL = ("facebook.com","fb.com","instagram.com","t.me","twitter.com","x.com",
          "linkedin.com","youtube.com","vk.com","ok.ru","wa.me","pinterest.com")
CC2COUNTRY = {
 "be":"Belgium","es":"Spain","dk":"Denmark","pl":"Poland","gb":"United Kingdom",
 "ie":"Ireland","se":"Sweden","at":"Austria","de":"Germany","fr":"France",
 "it":"Italy","nl":"Netherlands",
}
UA_LIST = [
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
]

def extract_country_from_url(url):
    m = re.search(r"https?://([a-z]{2})\.bizin\.eu", url)
    if not m: return ""
    return CC2COUNTRY.get(m.group(1), m.group(1).upper())

def same_domain(url): return "bizin.eu" in url
def looks_social(url): return any(s in url for s in SOCIAL)

def decode_cfemail(cfhex):
    try:
        r = int(cfhex[:2], 16)
        return "".join(chr(int(cfhex[i:i+2],16) ^ r) for i in range(2, len(cfhex), 2))
    except: return ""

def build_driver(profile_dir, headless, disable_images, page_timeout, chrome_binary=None, user_agent=None):
    opts = uc.ChromeOptions()
    if chrome_binary and Path(chrome_binary).exists():
        opts.binary_location = chrome_binary
    if profile_dir:
        Path(profile_dir).mkdir(parents=True, exist_ok=True)
        opts.add_argument(f"--user-data-dir={profile_dir}")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-features=Translate,AutomationControlled,MediaRouter,InterestFeedContent")
    opts.add_argument("--start-maximized")
    opts.add_argument(f"--user-agent={user_agent or random.choice(UA_LIST)}")
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1200,2000")
    if disable_images:
        prefs = {"profile.managed_default_content_settings.images": 2}
        opts.add_experimental_option("prefs", prefs)

    drv = uc.Chrome(options=opts, use_subprocess=True)
    drv.set_page_load_timeout(page_timeout)
    drv.set_script_timeout(page_timeout)
    return drv

def robust_get(driver, url, timeout, retries=2, manual_cf=False):
    cf_hit = False
    for attempt in range(1, retries+1):
        try:
            driver.get(url)
            WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState")=="complete")
            html = driver.page_source
            if ("cf-turnstile" in html) or ("challenges.cloudflare" in html) or ('/cdn-cgi/challenge-platform' in html):
                cf_hit = True
                if manual_cf:
                    print(f"   [CF] Обнаружен Turnstile — отметьте чекбокс в окне ({url})")
                    for _ in range(60):
                        time.sleep(1)
                        if "cf-turnstile" not in driver.page_source: break
                else:
                    time.sleep(20)
            return True, cf_hit
        except TimeoutException:
            if attempt==retries: return False, cf_hit
            time.sleep(2*attempt)
        except WebDriverException:
            if attempt==retries: return False, cf_hit
            time.sleep(2*attempt)
    return False, cf_hit

def grab_text_safe(el):
    try: return el.text.strip()
    except: return ""

def parse_company(driver, url):
    country = extract_country_from_url(url)
    name, site, email = "","",""

    for sel in ["h1","h1.title","h1.page-title",".company-title",".title h1",".title"]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            t = grab_text_safe(el)
            if len(t)>=2: name=t; break
        except: pass
    if not name:
        try:
            og = driver.find_element(By.CSS_SELECTOR, "meta[property='og:title']")
            name = og.get_attribute("content") or ""
        except: pass

    try:
        for n in driver.find_elements(By.CSS_SELECTOR, "a.__cf_email__,[data-cfemail]"):
            hx = n.get_attribute("data-cfemail")
            if hx:
                email = decode_cfemail(hx)
                if "@" in email: break
    except: pass
    if not email:
        try:
            for a in driver.find_elements(By.CSS_SELECTOR, "a[href^='mailto:']"):
                href = a.get_attribute("href") or ""
                m = re.search(r"mailto:([^?]+)", href, re.I)
                if m:
                    email = m.group(1).strip()
                    if "@" in email: break
        except: pass
    if not email:
        try:
            txt = driver.find_element(By.TAG_NAME, "body").text
            m = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", txt, re.I)
            if m: email = m.group(0)
        except: pass

    try:
        links = driver.find_elements(By.CSS_SELECTOR, "a[href^='http']")
        cand = []
        for a in links:
            href = (a.get_attribute("href") or "").strip()
            if not href: continue
            if looks_social(href): continue
            if same_domain(href): continue
            cand.append(href)
        if cand:
            site = cand[0]
            for h in cand:
                if re.search(r"//(www\.)?[^/]+\.[a-z]{2,}", h, re.I):
                    site = h; break
    except: pass

    return {"url":url, "name":name, "website":site, "email":email, "country":country}

def cycle_process(urls, out_path, profile_dir, chrome_binary, headless, disable_images,
                  page_timeout, retries, flush_every, restart_every, manual_cf):
    if out_path.exists():
        res = pd.read_csv(out_path)
        have = set(res.get("url", pd.Series([], dtype=str)).astype(str))
    else:
        res = pd.DataFrame(columns=["url","name","website","email","country"])
        have = set()

    todo = [u for u in urls if u not in have]
    print(f"▶ Уже в выходном: {len(have)} | К обработке: {len(todo)}")
    if not todo:
        print("▶ Нечего делать."); return

    buffer = []
    processed = 0
    cf_recent = 0
    consecutive_driver_fail = 0

    driver = None

    def ensure_driver(force_new_ua=False):
        nonlocal driver, consecutive_driver_fail
        ua = random.choice(UA_LIST) if force_new_ua else None
        while True:
            try:
                driver = build_driver(profile_dir, headless, disable_images, page_timeout, chrome_binary, ua)
                consecutive_driver_fail = 0
                print("▶ Chrome поднялся.")
                return
            except SessionNotCreatedException:
                consecutive_driver_fail += 1
                wait = min(60, 5 * consecutive_driver_fail)
                print(f"   ⚠ Не поднялся Chrome (SessionNotCreated). Жду {wait}s и пробую снова…")
                time.sleep(wait)
            except WebDriverException as e:
                consecutive_driver_fail += 1
                wait = min(60, 5 * consecutive_driver_fail)
                print(f"   ⚠ WebDriverException: {e}. Жду {wait}s и пробую снова…")
                time.sleep(wait)

    def flush():
        nonlocal buffer, res
        if not buffer: return
        res = pd.concat([res, pd.DataFrame(buffer)], ignore_index=True)
        res.drop_duplicates(subset=["url"], keep="last", inplace=True)
        res.to_csv(out_path, index=False)
        buffer.clear()

    ensure_driver()

    for i, url in enumerate(todo, 1):
        try:
            if (i % restart_every) == 0:
                try:
                    driver.quit()
                except: pass
                driver = None
                ensure_driver(force_new_ua=True)
                print("▶ Перезапущен Chrome (профилактика).")

            ok, cf_hit = robust_get(driver, url, page_timeout, retries=retries, manual_cf=manual_cf)
            if cf_hit:
                cf_recent += 1
            else:
                cf_recent = max(0, cf_recent-1)

            if cf_recent >= 8:
                cooldown = random.randint(40, 70)
                print(f"   [CF] частые челленджи → пауза {cooldown}s и смена сессии")
                try:
                    driver.quit()
                except: pass
                driver = None
                time.sleep(cooldown)
                ensure_driver(force_new_ua=True)
                cf_recent = 0

            if not ok:
                print(f"[{i}/{len(todo)}] ✖ не открылось: {url}")
                row = {"url":url, "name":"", "website":"", "email":"", "country":extract_country_from_url(url)}
            else:
                row = parse_company(driver, url)

            buffer.append(row)
            processed += 1

            if (processed % 50)==0:
                print(f"[{processed}/{len(todo)}] ✅ промежуточно")
            if (processed % flush_every)==0:
                flush()
                print(f"[{processed}] ✅ сохранено")

            time.sleep(random.uniform(0.9, 1.6))

        except KeyboardInterrupt:
            print("⛔ Остановлено пользователем.")
            break
        except Exception as e:
            print(f"[{i}] Ошибка цикла: {e.__class__.__name__}: {e}")
            time.sleep(2)

    flush()
    try:
        if driver: driver.quit()
    except: pass
    print(f"▶ Цикл окончен. Текущий размер файла: {len(res)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV со ссылками (колонка url)")
    ap.add_argument("--output", required=True, help="CSV для результата")
    ap.add_argument("--profile-dir", default="", help="Папка профиля Chrome")
    ap.add_argument("--chrome-binary", default="", help="Путь к chrome.exe (опционально)")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--disable-images", action="store_true")
    ap.add_argument("--page-timeout", type=int, default=60)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--flush-every", type=int, default=200)
    ap.add_argument("--batch-size", type=int, default=1200, help="Сколько URL обрабатываем за один цикл сессий")
    ap.add_argument("--restart-every", type=int, default=350, help="Периодический перезапуск драйвера внутри цикла")
    ap.add_argument("--manual-cf", action="store_true")
    args = ap.parse_args()

    print("▶ Проверка пакетов…")
    print("  pandas:", pd.__version__)
    print("  undetected_chromedriver:", uc.__version__)

    df = pd.read_csv(args.input)
    if "url" not in df.columns:
        print("Входной CSV должен содержать колонку 'url'", file=sys.stderr); sys.exit(1)

    urls = df["url"].astype(str).tolist()
    total = len(urls)
    print(f"▶ Всего входных ссылок: {total}")

    out_path = Path(args.output)
    if out_path.exists():
        done = set(pd.read_csv(out_path).get("url", pd.Series([], dtype=str)).astype(str))
        urls = [u for u in urls if u not in done]
        print(f"▶ Уже в выходном: {len(done)} | Осталось: {len(urls)}")

    while urls:
        batch = urls[:args.batch_size]  
        urls  = urls[args.batch_size:]    
        print(f"\n=== Новый цикл: {len(batch)} URL (осталось {len(urls)}) ===")
        cycle_process(
            batch, out_path,
            profile_dir=args.profile_dir,
            chrome_binary=args.chrome_binary,
            headless=args.headless,
            disable_images=args.disable_images,
            page_timeout=args.page_timeout,
            retries=args.retries,
            flush_every=args.flush_every,
            restart_every=args.restart_every,
            manual_cf=args.manual_cf,
        )
        sleep_s = random.randint(10, 25)
        print(f"▶ Пауза между циклами {sleep_s}s…")
        time.sleep(sleep_s)

    print("✔ Готово: все URL обработаны.")

if __name__ == "__main__":
    main()


