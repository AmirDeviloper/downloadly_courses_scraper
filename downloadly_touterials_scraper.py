import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def to_mb(txt):
    if txt is None or txt == '':
        return 0

    match = re.search(r"(\d+(?:\.\d+)?)", txt)
    number = float(match.group(1)) if match else 0

    return number * 1024 if ("گیگابایت" in txt) else number

def fetch_course_details(url):
    print('getting course details')
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"error while reading {url}: {e}")
        return {"duration": 0, "file_size_mb": 0, "rate": 0, "votes_count": 0}

    soup = BeautifulSoup(resp.text, "html.parser")

    duration = None
    li_tag = soup.find("li", string=re.compile("مدت زمان آموزش"))
    if li_tag:
        time_values = re.findall(r"\d+", li_tag.get_text(strip=True))

        duration = f'{time_values[0]}:{time_values[1]}' if len(time_values) > 1 else f'0:{time_values}'

    file_size = None
    container = soup.select_one("div.w-post-elm.post_content")
    p_tag = container.find_all("p")[-1]
    if p_tag:
        file_size = to_mb(p_tag.get_text(strip=True))

    rate, votes_count = None, None
    rating_div = soup.select_one("div.kksr-legend")

    if rating_div:
        rating = rating_div.get_text(strip=True)
        rating_values = re.findall(r"\d+(?:\.\d+)?", rating)
        rate, votes_count = (0, 0) if not rating_values else (rating_values[0], rating_values[2])

    return {
        "duration": duration,
        "file_size_mb": file_size,
        "rate": rate,
        'votes_count': votes_count
    }


def fetch_courses(url):

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"error while reading {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    selector = (
        "div.w-grid-list article .w-grid-item-h "
        "div.w-vwrapper.usg_vwrapper_1.align_none.valign_top"
    )
    return [div for div in soup.select(selector) if div.find("h2")]

def get_course_by_div(course_div):

    h2 = course_div.find("h2")
    a_tag = h2.find("a") if h2 else None
    title = a_tag.get_text(strip=True) if a_tag else None
    link = a_tag["href"] if a_tag and a_tag.has_attr("href") else None

    time_tag = course_div.find("time")
    updated_date = None
    if time_tag and time_tag.has_attr("datetime"):
        dt = datetime.fromisoformat(time_tag["datetime"].replace("Z", "+00:00"))
        updated_date = dt.strftime("%Y-%m-%d %H:%M:%S")

    comments_count_obj = course_div.select_one("a.smooth-scroll")
    comments_count = int(
        comments_count_obj.get_text(" ", strip=True).split()[0]
        if comments_count_obj else "0"
    )

    result = {
        "title": title,
        "link": link,
        "updated": updated_date,
        "comments": comments_count,
    }

    details = fetch_course_details(link)
    result.update(details)

    return result
    

def process_page(page):
    url = f"https://downloadly.ir/download/elearning/video-tutorials/page/{page}/"
    course_divs = fetch_courses(url)
    return [get_course_by_div(div) for div in course_divs]

if __name__ == "__main__":
    FINAL_PAGE = 954
    MAX_WORKERS = 20
    SAVE_PER_PAGE = 100

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_page, page): page for page in range(1, FINAL_PAGE)}

        for i, future in enumerate(as_completed(futures), start=1):
            page = futures[future]
            try:
                page_results = future.result()
                results.extend(page_results)
                print(f"Page {page} / {FINAL_PAGE} Complete. (Total so far: {len(results)})")
            except Exception as e:
                print(f"error while reading {page}: {e}")

            if i % SAVE_PER_PAGE == 0:
                df = pd.DataFrame(results)
                df.to_csv(f"result_{i - SAVE_PER_PAGE - 1}_{i}.csv", index=False, encoding="utf-8-sig")
                results = []

    if results:
        df = pd.DataFrame(results)
        df.to_csv("result_final.csv", index=False, encoding="utf-8-sig")

