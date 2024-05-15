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


def process_downloads():
    """This function is not part of the main workflow
    you can run it after downloading everything to make sure all downloads are processed
    (some downloads might not be processed when the script is interrupted)
    """
    for path in sorted((data_path / "zip").glob("**/*.zip")):
        process_download(path)


async def clickthrough(
    query=None, headless=True, start=2018, end=2024, backward=False
) -> pd.DataFrame | None:
    """The main workflow function

    Args:
        query (_type_, optional): _description_. Defaults to None.
        headless (bool, optional): _description_. Defaults to True.
        start (int, optional): _description_. Defaults to 2008.
        end (int, optional): _description_. Defaults to 2024.
        backward (bool, optional): _description_. Defaults to False.

    Returns:
        pd.DataFrame | None: _description_
    """
    try:
        page, browser, context = await setup(headless=headless)  # open the browser
        page, browser, context = await login(page, browser, context)  # login
        page, browser, context = await search(
            query, page, browser, context, backward=backward
        )  # search and sort by date

        # now iterate through the results by month
        # (this is because only 1000 results can be downloaded at once,
        # so we need to split up the results in some way)
        months_and_years = [
            (month, year) for year in range(start, end) for month in range(1, 13)
        ]
        q = tqdm(months_and_years, miniters=1, mininterval=0.1)
        
        for month, year in q:
            q.set_description(f"{year}-{month:02d}")
            
            # narrow down the results to the given month
            res = await search_by_month(
                year, month, page, browser, context, backward=backward
            )  
            if res is None:
                # continue if there are no results or the results are already downloaded
                continue
            page, browser, context = res
            
            # trigger the download and the conversion of the results
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
    """Setup the browser

    Args:
        headless (bool, optional): _description_. Defaults to True.

    Returns:
        tuple[Page, Browser, BrowserContext]: _description_
    """
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
    """Wait and click once available

    Args:
        page (Page): _description_
        selector (str): _description_
        n (int, optional): _description_. Defaults to 0.
        timeout (_type_, optional): _description_. Defaults to 5_000.

    Returns:
        Page: _description_
    """
    await page.wait_for_selector(selector, timeout=timeout)
    els = await page.query_selector_all(selector)
    await els[n].click()
    return page


async def login(
    page: Page, browser: Browser, context: BrowserContext
) -> tuple[Page, Browser, BrowserContext]:
    """login to nexis
    ⚠️ the login is specific to your institution, so you may need to adjust this
    if logged in within the previous hour, reuse that session

    Args:
        page (Page): _description_
        browser (Browser): _description_
        context (BrowserContext): _description_

    Returns:
        tuple[Page, Browser, BrowserContext]: _description_
    """
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
    """search and filter the results

    Args:
        query (str): _description_
        page (Page): _description_
        browser (Browser): _description_
        context (BrowserContext): _description_
        backward (bool, optional): _description_. Defaults to False.

    Returns:
        tuple[Page, Browser, BrowserContext]: _description_
    """
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
    """Narrow down the search results to a specific month

    Args:
        year (int): _description_
        month (int): _description_
        page (Page): _description_
        browser (Browser): _description_
        context (BrowserContext): _description_
        backward (bool, optional): _description_. Defaults to False.

    Returns:
        tuple[Page, Browser, BrowserContext] | None: _description_
    """
    existing_files = list((data_path / "zip").glob(f"{year}-{month:02d}/*.zip"))
    if any([not a.name.endswith("00.zip") for a in existing_files]):
        # then we already have all files for this month
        return None
    if any([a.name.endswith("000.zip") for a in existing_files]):
        # can't download more than 1000 files per query in a straightforward way
        # but backwards one can download another 1000, so you can double the amount
        # alternatively use smaller date ranges such as weeks
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


def _print(q, *args, end="\n"):
    """print while a tqdm progress bar is running

    Args:
        q (_type_): _description_
        end (str, optional): _description_. Defaults to "\n".
    """
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
    """Navigate through the download dialog and trigger the download

    Args:
        year (_type_): _description_
        month (_type_): _description_
        page (Page): _description_
        browser (Browser): _description_
        context (BrowserContext): _description_
        q (tqdm): _description_
        n (int, optional): _description_. Defaults to 1000.
        backward (bool, optional): _description_. Defaults to False.

    Returns:
        tuple[Page, Browser, BrowserContext]: _description_
    """
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
        process_download(dest_path)
        # await page.wait_for_timeout(120_000)
        await page.wait_for_timeout(5_000)
    return page, browser, context


def unpack(path: Path) -> list[tuple[str, str]]:
    """Unpack zip to rtfs and convert to plaintexts

    Args:
        path (Path): _description_

    Returns:
        list[tuple[str, str]]: _description_
    """
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
    """Parse metadata and text from plaintext
    this is partially specific to Agence France Presse's format,
    so you may need to adjust this
    also consider using https://github.com/JBGruber/LexisNexisTools for this part ❤️

    Args:
        plaintext (str): _description_

    Returns:
        dict: _description_
    """
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


def process_download(path: Path):
    """Unpack and parse and store in the right directory and filename

    Args:
        path (Path): _description_
    """
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
    asyncio.run(clickthrough(query, headless=True, backward=False))
