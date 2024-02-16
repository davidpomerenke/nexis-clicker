import asyncio
import json
import re
import shutil
from calendar import monthrange
from datetime import date, datetime, timedelta
from os import environ
from pathlib import Path
from zipfile import ZipFile

import dateparser
import pandas as pd
from tqdm.auto import tqdm
from dotenv import load_dotenv
from munch import Munch
from playwright.async_api import Browser, BrowserContext, Page, async_playwright
from striprtf.striprtf import rtf_to_text

load_dotenv()

# config:
cookie_path = Path("cookies.json")
query = """(climate) NEAR/10 (protest* OR demo OR rally OR campaign OR "social movement" OR occup* OR strike OR petition OR riot OR unrest OR uprising OR boycott OR riot OR activis* OR resistance OR mobilization OR "citizens' initiative" OR march OR parade OR picket OR block* OR sit-in OR vigil OR "hunger strike" OR rebel* OR "civil disobedience")"""
data_path = Path("data") / "climate-protests"
data_path.mkdir(parents=True, exist_ok=True)


async def main():
    await clickthrough(query, headless=False, backward=False)


def process_downloads():
    for path in sorted((data_path / "zip").glob("**/*.zip")):
        process_dowload(path)


async def clickthrough(
    query=None, headless=True, start=2008, end=2024, backward=False
) -> pd.DataFrame | None:
    try:
        page, browser, context = await setup(headless=headless)
        page, browser, context = await login(page, browser, context)
        page, browser, context = await search(
            query, page, browser, context, backward=backward
        )
        cookies = await context.cookies()
        cookie_path.write_text(json.dumps(cookies))
        months_and_years = [
            (month, year) for year in range(start, end) for month in range(1, 13)
        ]
        q = tqdm(months_and_years, miniters=1, mininterval=0.1)
        for month, year in q:
            q.set_description(f"{year}-{month:02d}")
            res = await search_by_month(
                year, month, page, browser, context, backward=backward
            )
            if res is None:
                continue
            page, browser, context = res
            page, browser, context = await download(
                year, month, page, browser, context, backward=backward, q=q
            )
    except Exception as e:
        print(e)
        # wait here for a longer time for debugging
        await page.wait_for_timeout(5_000)
    finally:
        await browser.close()


async def setup(headless=True) -> tuple[Page, Browser, BrowserContext]:
    path = data_path / "tmp"
    path.mkdir(parents=True, exist_ok=True)
    p = await async_playwright().start()
    browser = await p.chromium.launch(
        timeout=10_000, downloads_path=path, headless=headless
    )
    context = await browser.new_context()
    page = await context.new_page()
    return page, browser, context


async def click(page: Page, selector: str, n: int = 0, timeout=5_000) -> Page:
    await page.wait_for_selector(selector, timeout=timeout)
    els = await page.query_selector_all(selector)
    await els[n].click()
    return page


async def login(
    page: Page, browser: Browser, context: BrowserContext
) -> tuple[Page, Browser, BrowserContext]:
    if cookie_path.exists():
        last_mod = datetime.fromtimestamp(cookie_path.stat().st_mtime)
        time_passed = datetime.now() - last_mod
        if time_passed < timedelta(hours=1):
            await context.add_cookies(json.loads(cookie_path.read_text()))
            return page, browser, context
    print("Logging in ...")
    await page.goto(environ["NEXIS_URL"])
    await page.wait_for_timeout(5_000)
    await page.fill('input[id="user"]', environ["NEXIS_USER"])
    await page.fill('input[id="pass"]', environ["NEXIS_PASSWORD"])
    await click(page, 'input[type="submit"]')
    await page.wait_for_timeout(20_000)
    cookies = await context.cookies()
    cookie_path.write_text(json.dumps(cookies))
    return page, browser, context


async def search(
    query: str,
    page: Page,
    browser: Browser,
    context: BrowserContext,
    backward: bool = False,
) -> tuple[Page, Browser, BrowserContext]:
    print("Searching", end=" ... ")
    await page.goto(environ["NEXIS_URL"])
    await page.fill("lng-expanding-textarea", query)
    await click(page, "lng-search-button")
    try:
        await click(page, 'span[class="filter-text"]', timeout=15_000)
        await page.wait_for_timeout(5_000)
    except Exception:
        pass
    await click(page, 'button[data-filtertype="source"]')
    await page.wait_for_timeout(1_000)
    await click(page, 'button[data-action="moreless"]')
    await page.wait_for_timeout(5_000)
    el = await page.query_selector('input[data-value="Agence France Presse - English"]')
    await el.dispatch_event("click")
    await page.wait_for_timeout(5_000)
    await click(page, 'span[id="sortbymenulabel"]')
    await page.wait_for_timeout(1_000)
    order = "descending" if backward else "ascending"
    await click(page, f'button[data-value="date{order}"]')
    await page.wait_for_timeout(5_000)
    print("✅")
    return page, browser, context


async def search_by_month(
    year: int,
    month: int,
    page: Page,
    browser: Browser,
    context: BrowserContext,
    backward: bool = False,
) -> tuple[Page, Browser, BrowserContext] | None:
    existing_files = list((data_path / "zip").glob(f"{year}-{month:02d}/*.zip"))
    if any([not a.name.endswith("00.zip") for a in existing_files]):
        # then we already have all files for this month
        return None
    if any([a.name.endswith("000.zip") for a in existing_files]):
        # can't download more than 1000 files per query in a straightforward way
        # but backwards one can download another 1000, which is sufficient for my case
        if not backward:
            return None
    try:
        await click(page, 'span[class="filter-text"]', n=1)
    except Exception:
        pass
    await page.wait_for_timeout(5_000)
    try:
        await click(
            page, 'button[data-filtertype="datestr-news"][data-action="expand"]'
        )
    except Exception:
        pass
    await page.wait_for_timeout(3_000)
    await page.fill('input[class="min-val"]', f"01/{month}/{year}")
    await page.wait_for_timeout(2_000)
    day = monthrange(year, month)[1]
    await page.fill('input[class="max-val"]', f"{day}/{month}/{year}")
    await page.wait_for_timeout(2_000)
    await click(page, 'div[class="date-form"]')
    await page.wait_for_timeout(1_000)
    await click(page, 'button[class="save btn secondary"]')
    await page.wait_for_timeout(10_000)
    return page, browser, context


async def visit_permalink(
    permalink: str, page: Page, browser: Browser, context: BrowserContext
) -> tuple[Page, Browser, BrowserContext]:
    await page.goto(permalink)
    await click(page, 'input[data-action="viewlink"]')
    await page.wait_for_timeout(10_000)
    return page, browser, context


def _print(q, *args, end="\n"):
    text = " ".join([str(a) for a in args])
    q.write(text, end=end)


async def download(
    year,
    month,
    page: Page,
    browser: Browser,
    context: BrowserContext,
    q: tqdm,
    n: int = 1000,
    backward: bool = False,
) -> tuple[Page, Browser, BrowserContext]:
    el = await page.query_selector('header[class="resultsHeader"]')
    n_results = int(
        re.search(r"\(((\d|\.)+)\)", await el.inner_text()).group(1).replace(".", "")
    )
    if not backward:
        r = range(0, min(n_results, n), 100)
    if backward:
        r = range(0, min(n_results - n, n), 100)
    for i in r:
        if not backward:
            x, y = i + 1, min(i + 100, n_results)
        if backward:
            x, y = i + 1, min(i + 100, n_results - 1000)
        range_ = f"{x}-{y}" if x != y else f"{x}"
        b = "B" if backward else ""
        dest_path = data_path / f"zip/{year}-{month:02d}/{b}{range_}.zip"
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if dest_path.exists():
            continue
        _print(
            q,
            datetime.now().strftime("%H:%M:%S"),
            "Downloading",
            dest_path,
            end=" ... ",
        )
        el = await page.query_selector('span[class="icon la-Download"]')
        await el.dispatch_event("click")
        await page.wait_for_timeout(2_000)
        await page.click('a[id="tab-FormattingOptions"]')
        checkboxes_1 = await page.query_selector_all(
            'fieldset[class="IncludeOptions"] input[type="checkbox"]'
        )
        checkboxes_2 = await page.query_selector_all(
            'fieldset[class="styling"] input[type="checkbox"]'
        )
        for checkbox in checkboxes_1 + checkboxes_2:
            if await checkbox.bounding_box():
                await checkbox.set_checked(False)
                await page.wait_for_timeout(200)
        await page.click('a[id="tab-BasicOptions"]')
        await page.wait_for_timeout(500)
        checkbox = await page.query_selector('input[id="SeparateFiles"]')
        await checkbox.set_checked(True)
        checkbox = await page.query_selector('input[id="Rtf"]')
        await checkbox.set_checked(True)
        await page.wait_for_timeout(1_000)
        await page.fill('input[id="SelectedRange"]', range_)
        async with page.expect_download(timeout=120_000) as download_info:
            await click(page, 'button[data-action="download"]')
        download = await download_info.value
        tmp_path = await download.path()
        shutil.move(tmp_path, dest_path)
        _print(q, "✅")
        process_dowload(dest_path)
        # await page.wait_for_timeout(120_000)
        await page.wait_for_timeout(5_000)
    return page, browser, context


def unpack(path: Path) -> list[tuple[str, str]]:
    with ZipFile(path) as zipObj:
        plaintexts = []
        for file in zipObj.filelist:
            if "_doclist" in file.filename:
                continue
            rtf = zipObj.read(file).decode(encoding="latin-1")
            plaintext = rtf_to_text(rtf, encoding="latin-1").strip()
            plaintext = plaintext.replace("\xa0", " ")
            plaintexts.append((file.filename, plaintext))
    return plaintexts


def parse(plaintext: str) -> dict:
    title, rest = plaintext.split("\n", 1)
    feed, rest = rest.split("\n", 1)
    date, rest = rest.split("\n", 1)
    date = dateparser.parse(date.strip(), languages=["en"])
    location = re.findall(r"Dateline:\s?(.+),[^,]+\n", rest)
    location = location[0] if len(location) > 0 else None
    if ", " in location:
        location, country = location.rsplit(", ", 1)
    else:
        country = None
    meta, rest = rest.split("Body", 1)
    if "Graphic" in rest:
        text, _ = rest.split("Graphic", 1)
    else:
        text, _ = rest.split("Load-Date", 1)
    return Munch(
        date=date.strftime("%Y-%m-%d") if date is not None else None,
        country=country,
        location=location,
        source=feed.strip(),
        title=title.strip(),
        text=text.strip(),
    )


def process_dowload(path: Path):
    texts = unpack(path)
    for fn, text in texts:
        item = parse(text)
        if not item.date:
            print(f"No date for {fn}, {item.title}")
            continue
        datestr = date.strftime(dateparser.parse(item.date), "%Y-%m-%d")
        jpath = data_path / "json" / datestr / f"{fn}.json"
        jpath.parent.mkdir(parents=True, exist_ok=True)
        jpath.write_text(json.dumps(item, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
