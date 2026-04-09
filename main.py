import asyncio
import datetime
import functools
import os.path
import time

import aiofiles
import aiohttp
from aiohttp.client_exceptions import (
    ClientConnectorError,
    NonHttpUrlRedirectClientError,
    ClientResponseError,
    InvalidUrlRedirectClientError,
    ClientOSError,
)
from bs4 import BeautifulSoup
import fake_useragent

user = fake_useragent.UserAgent().random
header = {"user-agent": user}

all_urls = []
count = 0
errors = {
    "ClientConnectorError": 0,
    "TimeoutError": 0,
    "UnicodeDecodeError": 0,
    "NonHttpUrlRedirectClientError": 0,
    "ClientResponseError": 0,
    "InvalidUrlRedirectClientError": 0,
    "ClientOSError": 0,
}


def timer(foo):
    @functools.wraps(foo)
    def wrapper(*args, **kwargs):
        start = time.time()
        foo(*args, **kwargs)
        time_passed = round(time.time() - start, 2)
        len_all_links = len(all_urls) + errors["TimeoutError"]
        print(
            f"Асинхронный краулер. Заданная глубина: {args[1]}. Достигнутая глубина: {count}."
            f" Найдено ссылок: {len_all_links}. TimeoutError: {errors["TimeoutError"]}. Затрачено времени: {time_passed}"
        )

    return wrapper


async def link_parser(url, level, url_base, log_name, client, mode):
    global count
    urls = []
    tasks = []

    try:
        async with client.get(url, headers=header, timeout=5) as response:
            res = await response.text()
            if count == level and level != 0:
                return

            soup = BeautifulSoup(res, "lxml")
            result = soup.find_all("a")
            for value in result:
                if mode == "external" and "nofollow" not in value.get("rel", []):
                    continue
                elif mode == "internal" and "nofollow" in value.get("rel", []):
                    continue

                value = value.get("href")
                if value:
                    if (
                        value.startswith("https://") or value.startswith("http://")
                    ) and url_base not in value:
                        urls.append(value)
            list(set(urls)).sort()
            urls_to_write = "\n".join(urls)
            string = f"Уровень: {count + 1}, {url=} urls:\n {urls_to_write}"
            count += 1
            await write_log(string, log_name)
            for value in urls:
                if url_base not in value:
                    if value not in all_urls:
                        all_urls.append(value)
                        tasks.append(
                            link_parser(value, level, url_base, log_name, client, mode)
                        )
            await asyncio.gather(*tasks)
    except TimeoutError:
        string = f"TimeoutError: {url}"
        await write_log(string, log_name)
        errors["TimeoutError"] += 1
    except ClientConnectorError:
        errors["ClientConnectorError"] += 1
    except UnicodeDecodeError:
        errors["UnicodeDecodeError"] += 1
    except NonHttpUrlRedirectClientError:
        errors["NonHttpUrlRedirectClientError"] += 1
    except ClientResponseError:
        errors["ClientResponseError"] += 1
    except InvalidUrlRedirectClientError:
        errors["InvalidUrlRedirectClientError"] += 1
    except ClientOSError:
        errors["ClientOSError"] += 1
    finally:
        if count == level and level != 0:
            return


async def write_log(string, name):
    async with aiofiles.open(name, "a", encoding="utf-8") as obj:
        await obj.write(string + "\n\n")


async def get_external_link(url, level, mode):
    data = datetime.datetime.now().strftime(format="%d.%m.%Y %H.%M.%S")
    if not os.path.exists("logs"):
        os.makedirs("logs")
    name = url.split("/")[-1]
    log_name = ".".join(["logs/", name, data, mode or "all", "txt"])
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(3)) as client:
        await link_parser(
            url, level=level, url_base=url, log_name=log_name, client=client, mode=mode
        )


@timer
def main(url, level, mode):
    print("Старт асинхронного краулера.")
    asyncio.run(get_external_link(url, level, mode))
    print(errors, end="\n\n")


def async_crawler(url, level=0, mode=""):
    main(url, level, mode)


if __name__ == "__main__":
    url = "https://ru.wikipedia.org/wiki/Человек,_который_упал_на_Землю_(фильм)"
    # url = "https://github.com"
    async_crawler(url, level=10, mode="external") # internal external all

# Асинхронный краулер. Заданная глубина: 10. Достигнутая глубина: 10. Найдено ссылок: 691. TimeoutError: 299. Затрачено времени: 24.02 all
# Асинхронный краулер. Заданная глубина: 10. Достигнутая глубина: 10. Найдено ссылок: 207. TimeoutError: 69. Затрачено времени: 11.66 internal
# Асинхронный краулер. Заданная глубина: 10. Достигнутая глубина: 10. Найдено ссылок: 137. TimeoutError: 41. Затрачено времени: 13.09 external