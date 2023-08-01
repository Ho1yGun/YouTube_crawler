import asyncio
import aiohttp
import random
import sqlite3

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from pytube import YouTube
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from youtube_transcript_api import YouTubeTranscriptApi


# Функция для создания базы данных и таблицы
def create_database():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS videos_with_subtitles 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  title TEXT, 
                  author TEXT, 
                  description TEXT,
                  views INTEGER,
                  publish_date DATE,
                  subtitles TEXT)''')
    conn.commit()
    conn.close()


# Функция для сохранения информации о видео и субтитрах в базе данных
def save_video_with_subtitles(video_info, subtitles):
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()
    subtitles_text = '\n'.join([subtitle['text'] for subtitle in subtitles])
    data = video_info + (subtitles_text,)
    c.execute('''INSERT INTO videos_with_subtitles (
    title, author, description, views, publish_date, subtitles
    ) 
                 VALUES (?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()


# Функция для получения содержимого страницы с помощью библиотеки aiohttp
async def get_page_content(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.text()
                return content
    except Exception as e:
        return None


# Функция для получения содержимого страницы с помощью библиотеки Selenium
async def get_page_content_with_selenium(url):
    # необходимо указать правильный путь к chromedriver.exe
    chrome_driver_path = 'chromedriver.exe'
    chrome_service = Service(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(service=chrome_service)

    try:
        driver.get(url)
        await asyncio.sleep(5)
        page_content = driver.page_source
        driver.quit()
        return page_content
    except Exception as e:
        driver.quit()
        return None


# Функция для получения случайного видео с главной страницы YouTube
async def get_random_video_url():
    url = "https://www.youtube.com/"
    page_content = await get_page_content_with_selenium(url)
    if page_content:
        soup = BeautifulSoup(page_content, "html.parser")
        video_links = [
            a["href"] for a in soup.find_all(
                "a", href=True
            ) if "/watch?v=" in a["href"]]
        if video_links:
            return random.choice(video_links)
    return None


# Функция для обработки видео и сохранения информации о видео и субтитрах в БД
def process_video(url):
    try:
        yt = YouTube("https://www.youtube.com" + url)
        video_title = yt.title
        video_author = yt.author
        video_description = yt.description
        video_views = yt.views
        video_publish_date = yt.publish_date
        video_id = url.split("?v=")[1]
        subtitles = YouTubeTranscriptApi.get_transcript(video_id)
        video_info = (
            video_title,
            video_author,
            video_description,
            video_views,
            video_publish_date
        )
        save_video_with_subtitles(video_info, subtitles)
    except Exception as e:
        print(f"Error processing video {url}: {e}")


# Основная функция для асинхронного выполнения переходов по страницам
async def crawl_youtube_pages():
    global visited_videos

    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        while True:
            start_video = await get_random_video_url()
            if start_video and start_video not in visited_videos:
                loop.run_in_executor(executor, process_video, start_video)
                visited_videos.add(start_video)
            else:
                print("No video links found. Restarting crawling...")

# Создаем базу данных и таблицу
create_database()

# Множество для отслеживания посещенных видео
visited_videos = set()

# Запускаем основную функцию в асинхронном режиме
asyncio.run(crawl_youtube_pages())
