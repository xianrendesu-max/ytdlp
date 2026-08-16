import asyncio
import time
from fastapi import FastAPI, HTTPException, Query
import aiohttp
import yt_dlp

app = FastAPI()

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text"
]

TEST_URL = "https://www.gstatic.com/generate_204"

cached_proxies = []

async def fetch_proxies_from_source(session: aiohttp.ClientSession, url: str) -> set:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                text = await resp.text()
                return set(line.strip() for line in text.splitlines() if line.strip())
    except Exception:
        pass
    return set()

async def check_proxy(session: aiohttp.ClientSession, proxy: str, timeout_sec: float = 2.0):
    proxy_url = f"http://{proxy}"
    start_time = time.time()
    try:
        async with session.get(TEST_URL, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
            if resp.status in (200, 204):
                latency = round((time.time() - start_time) * 1000, 2)
                return {"proxy": proxy_url, "latency_ms": latency}
    except Exception:
        pass
    return None

async def refresh_proxy_pool():
    global cached_proxies
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_proxies_from_source(session, url) for url in PROXY_SOURCES]
        results = await asyncio.gather(*tasks)
        all_proxies = list(set().union(*results))[:150]
        if not all_proxies:
            return []

        check_tasks = [check_proxy(session, p) for p in all_proxies]
        checked_results = await asyncio.gather(*check_tasks)
        valid_proxies = [p for p in checked_results if p is not None]
        valid_proxies.sort(key=lambda x: x["latency_ms"])

        cached_proxies = valid_proxies
        return valid_proxies

async def get_best_proxy():
    global cached_proxies
    if cached_proxies:
        return cached_proxies[0]["proxy"]
    
    refreshed = await refresh_proxy_pool()
    if refreshed:
        return refreshed[0]["proxy"]
    return None

@app.get("/api/info")
async def get_video_info(url: str = Query(...)):
    proxy = await get_best_proxy()
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    if proxy:
        ydl_opts["proxy"] = proxy

    loop = asyncio.get_event_loop()
    try:
        def extract():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)

        info = await loop.run_in_executor(None, extract)
        return {
            "status": "success",
            "proxy_used": proxy,
            "title": info.get("title"),
            "uploader": info.get("uploader"),
            "duration": info.get("duration"),
            "thumbnail": info.get("thumbnail"),
            "formats": [
                {
                    "format_id": f.get("format_id"),
                    "ext": f.get("ext"),
                    "resolution": f.get("resolution"),
                    "url": f.get("url")
                }
                for f in info.get("formats", []) if f.get("url")
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cron")
async def cron_handler():
    proxies = await refresh_proxy_pool()
    return {
        "status": "ok",
        "total_active": len(proxies),
        "top_proxy": proxies[0]["proxy"] if proxies else None
    }

@app.get("/api/proxies/status")
async def get_proxy_status():
    global cached_proxies
    return {
        "total_active": len(cached_proxies),
        "proxies": cached_proxies[:10]
    }
