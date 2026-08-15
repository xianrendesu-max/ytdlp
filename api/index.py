import asyncio
import ipaddress
import logging
import os
import random
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO")
)

logger = logging.getLogger("youtube-stream-api")


app = FastAPI(
    title="YouTube Combined Stream API",
    version="1.0.0",
    description=(
        "FastAPI and yt-dlp API for extracting "
        "YouTube single-file video/audio streams."
    ),
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


MAX_CACHE_SIZE = int(
    os.getenv("MAX_CACHE_SIZE", "100")
)

CACHE_TTL = int(
    os.getenv("CACHE_TTL", "240")
)

MAX_PROXIES = int(
    os.getenv("MAX_PROXIES", "60")
)

PROXY_TIMEOUT = float(
    os.getenv("PROXY_TIMEOUT", "2.5")
)

PROXY_REFRESH_SECONDS = int(
    os.getenv("PROXY_REFRESH_SECONDS", "600")
)

MAX_CONCURRENT = int(
    os.getenv("MAX_CONCURRENT", "1")
)

YT_DLP_TIMEOUT = int(
    os.getenv("YT_DLP_TIMEOUT", "12")
)

MAX_PROXY_ATTEMPTS = int(
    os.getenv("MAX_PROXY_ATTEMPTS", "4")
)

COOKIES_PATH = os.getenv(
    "COOKIES_PATH",
    os.getenv("COOKIE_FILE", "cookies.txt")
)


class APIError(Exception):
    pass


@dataclass
class CacheItem:
    value: Any
    expires_at: float


class MemoryCache:
    def __init__(
        self,
        max_size: int,
        ttl: int,
    ):
        self.max_size = max_size
        self.ttl = ttl
        self.data: OrderedDict[str, CacheItem] = OrderedDict()

    def get(
        self,
        key: str,
    ) -> Optional[Any]:

        item = self.data.get(key)

        if item is None:
            return None

        if item.expires_at <= time.monotonic():
            self.data.pop(key, None)
            return None

        self.data.move_to_end(key)

        return item.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ):

        expiration = time.monotonic() + (
            self.ttl
            if ttl is None
            else ttl
        )

        self.data[key] = CacheItem(
            value=value,
            expires_at=expiration,
        )

        self.data.move_to_end(key)

        while len(self.data) > self.max_size:
            self.data.popitem(last=False)

    def delete(
        self,
        key: str,
    ):

        self.data.pop(key, None)

    def clear(self):

        self.data.clear()

    def size(self) -> int:

        return len(self.data)


class InFlight:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}

    async def get_or_create(
        self,
        key: str,
        factory,
    ):

        existing = self.tasks.get(key)

        if existing is not None:
            return await existing

        task = asyncio.create_task(
            factory()
        )

        self.tasks[key] = task

        try:
            return await task
        finally:
            current = self.tasks.get(key)

            if current is task:
                self.tasks.pop(key, None)


@dataclass
class ProxyInfo:
    url: str
    latency: float = 9999.0
    failures: int = 0
    successes: int = 0
    checked_at: float = 0.0


class ProxyManager:

    SOURCES = [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    ]

    def __init__(self):

        self.proxies: List[ProxyInfo] = []

        self.last_refresh = 0.0

        self.refresh_lock = asyncio.Lock()

    @staticmethod
    def is_public_ip(
        host: str,
    ) -> bool:

        try:
            ip = ipaddress.ip_address(host)

        except ValueError:
            return False

        if ip.is_private:
            return False

        if ip.is_loopback:
            return False

        if ip.is_link_local:
            return False

        if ip.is_multicast:
            return False

        if ip.is_reserved:
            return False

        if ip.is_unspecified:
            return False

        return True

    @classmethod
    def normalize_proxy(
        cls,
        value: str,
    ) -> Optional[str]:

        value = value.strip()

        if not value:
            return None

        if value.startswith(
            "http://"
        ):
            value = value[7:]

        elif value.startswith(
            "https://"
        ):
            value = value[8:]

        elif "://" in value:
            return None

        if "@" in value:
            return None

        parts = value.rsplit(
            ":",
            1,
        )

        if len(parts) != 2:
            return None

        host = parts[0].strip()

        port_text = parts[1].strip()

        if not host:
            return None

        try:
            port = int(port_text)

        except ValueError:
            return None

        if port < 1 or port > 65535:
            return None

        if not cls.is_public_ip(host):
            return None

        return (
            "http://"
            + host
            + ":"
            + str(port)
        )

    async def fetch_source(
        self,
        client: httpx.AsyncClient,
        source: str,
    ) -> List[str]:

        try:

            response = await client.get(
                source,
                timeout=8.0,
                follow_redirects=True,
            )

            response.raise_for_status()

            proxies = []

            for line in response.text.splitlines():

                proxy = self.normalize_proxy(
                    line
                )

                if proxy:
                    proxies.append(proxy)

            return proxies

        except Exception as exc:

            logger.warning(
                "Proxy source failed: %s %s",
                source,
                exc,
            )

            return []

    async def refresh(
        self,
        force: bool = False,
    ) -> int:

        now = time.monotonic()

        if (
            not force
            and self.proxies
            and (
                now - self.last_refresh
                < PROXY_REFRESH_SECONDS
            )
        ):
            return len(self.proxies)

        async with self.refresh_lock:

            now = time.monotonic()

            if (
                not force
                and self.proxies
                and (
                    now - self.last_refresh
                    < PROXY_REFRESH_SECONDS
                )
            ):
                return len(self.proxies)

            timeout = httpx.Timeout(
                connect=5.0,
                read=8.0,
                write=8.0,
                pool=5.0,
            )

            limits = httpx.Limits(
                max_connections=8,
                max_keepalive_connections=4,
            )

            async with httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 "
                        "(compatible; "
                        "YouTubeStreamAPI/1.0)"
                    )
                },
            ) as client:

                results = await asyncio.gather(
                    *[
                        self.fetch_source(
                            client,
                            source,
                        )
                        for source in self.SOURCES
                    ],
                    return_exceptions=True,
                )

            unique = set()

            for result in results:

                if isinstance(
                    result,
                    list,
                ):
                    unique.update(result)

            values = list(unique)

            random.shuffle(values)

            values = values[
                :MAX_PROXIES
            ]

            self.proxies = [
                ProxyInfo(
                    url=value
                )
                for value in values
            ]

            self.last_refresh = time.monotonic()

            logger.info(
                "Loaded %d proxy candidates",
                len(self.proxies),
            )

            return len(self.proxies)

    async def verify(
        self,
        proxy: ProxyInfo,
    ) -> bool:

        started = time.monotonic()

        timeout = httpx.Timeout(
            connect=PROXY_TIMEOUT,
            read=PROXY_TIMEOUT,
            write=PROXY_TIMEOUT,
            pool=PROXY_TIMEOUT,
        )

        try:

            async with httpx.AsyncClient(
                proxy=proxy.url,
                timeout=timeout,
                follow_redirects=False,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 "
                        "(compatible; "
                        "YouTubeStreamAPI/1.0)"
                    )
                },
            ) as client:

                response = await client.get(
                    "https://www.youtube.com/robots.txt"
                )

            elapsed = (
                time.monotonic()
                - started
            )

            if (
                200
                <= response.status_code
                < 500
            ):

                proxy.latency = elapsed

                proxy.failures = 0

                proxy.successes += 1

                proxy.checked_at = (
                    time.monotonic()
                )

                return True

        except Exception:
            pass

        proxy.failures += 1

        proxy.checked_at = (
            time.monotonic()
        )

        return False

    async def get_proxy(
        self,
        attempts: int = MAX_PROXY_ATTEMPTS,
    ) -> Optional[str]:

        await self.refresh()

        candidates = [
            proxy
            for proxy in self.proxies
            if proxy.failures < 3
        ]

        candidates.sort(
            key=lambda proxy: (
                proxy.failures,
                proxy.latency,
            )
        )

        if not candidates:

            await self.refresh(
                force=True
            )

            candidates = list(
                self.proxies
            )

        random.shuffle(
            candidates
        )

        checked = 0

        for proxy in candidates:

            if checked >= attempts:
                break

            checked += 1

            if await self.verify(
                proxy
            ):
                return proxy.url

        return None

    def mark_failure(
        self,
        proxy_url: str,
    ):

        for proxy in self.proxies:

            if proxy.url == proxy_url:

                proxy.failures += 1

                break

    def mark_success(
        self,
        proxy_url: str,
    ):

        for proxy in self.proxies:

            if proxy.url == proxy_url:

                proxy.failures = 0

                proxy.successes += 1

                break

    def stats(self) -> Dict[str, Any]:

        usable = sum(
            1
            for proxy in self.proxies
            if proxy.failures < 3
        )

        return {
            "total": len(
                self.proxies
            ),
            "usable": usable,
            "last_refresh": (
                self.last_refresh
                if self.last_refresh
                else None
            ),
        }


cache = MemoryCache(
    max_size=MAX_CACHE_SIZE,
    ttl=CACHE_TTL,
)


inflight = InFlight()


proxy_manager = ProxyManager()


semaphore = asyncio.Semaphore(
    MAX_CONCURRENT
)


YOUTUBE_HOSTNAMES = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtu.be",
    "www.youtu.be",
}


VIDEO_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9_-]{6,20}$"
)


def normalize_youtube_url(
    url: str,
) -> str:

    if not isinstance(
        url,
        str,
    ):
        raise APIError(
            "Invalid URL"
        )

    url = url.strip()

    if len(url) > 2048:
        raise APIError(
            "URL is too long"
        )

    parsed = urlparse(url)

    if parsed.scheme.lower() not in {
        "http",
        "https",
    }:
        raise APIError(
            "Only HTTP and HTTPS URLs are supported"
        )

    hostname = (
        parsed.hostname or ""
    ).lower()

    if hostname not in YOUTUBE_HOSTNAMES:
        raise APIError(
            "Only YouTube URLs are supported"
        )

    video_id = None

    if hostname in {
        "youtu.be",
        "www.youtu.be",
    }:

        video_id = (
            parsed.path
            .strip("/")
            .split("/", 1)[0]
        )

    elif parsed.path == "/watch":

        video_id = parse_qs(
            parsed.query
        ).get(
            "v",
            [None],
        )[0]

    elif parsed.path.startswith(
        "/shorts/"
    ):

        video_id = (
            parsed.path
            .split(
                "/shorts/",
                1,
            )[1]
            .split(
                "/",
                1,
            )[0]
        )

    elif parsed.path.startswith(
        "/embed/"
    ):

        video_id = (
            parsed.path
            .split(
                "/embed/",
                1,
            )[1]
            .split(
                "/",
                1,
            )[0]
        )

    elif parsed.path.startswith(
        "/live/"
    ):

        video_id = (
            parsed.path
            .split(
                "/live/",
                1,
            )[1]
            .split(
                "/",
                1,
            )[0]
        )

    if not video_id:
        raise APIError(
            "Could not find YouTube video ID"
        )

    if not VIDEO_ID_PATTERN.match(
        video_id
    ):
        raise APIError(
            "Invalid YouTube video ID"
        )

    return (
        "https://www.youtube.com/watch?v="
        + video_id
    )


def is_combined_format(
    fmt: Dict[str, Any],
) -> bool:

    vcodec = fmt.get(
        "vcodec"
    )

    acodec = fmt.get(
        "acodec"
    )

    if (
        not vcodec
        or vcodec == "none"
    ):
        return False

    if (
        not acodec
        or acodec == "none"
    ):
        return False

    return True


def select_combined_format(
    formats: List[Dict[str, Any]],
    max_height: Optional[int] = None,
) -> Optional[Dict[str, Any]]:

    candidates = []

    for fmt in formats:

        if not isinstance(
            fmt,
            dict,
        ):
            continue

        if not is_combined_format(
            fmt
        ):
            continue

        stream_url = fmt.get(
            "url"
        )

        if not stream_url:
            continue

        protocol = fmt.get(
            "protocol"
        )

        if protocol not in {
            "http",
            "https",
        }:
            continue

        height = fmt.get(
            "height"
        )

        if (
            max_height is not None
            and height is not None
            and height > max_height
        ):
            continue

        candidates.append(
            fmt
        )

    if not candidates:
        return None

    def score(
        fmt: Dict[str, Any]
    ):

        height = (
            fmt.get("height")
            or 0
        )

        fps = (
            fmt.get("fps")
            or 0
        )

        tbr = (
            fmt.get("tbr")
            or 0
        )

        abr = (
            fmt.get("abr")
            or 0
        )

        ext = fmt.get(
            "ext"
        )

        mp4 = (
            1
            if ext == "mp4"
            else 0
        )

        return (
            mp4,
            height,
            fps,
            tbr,
            abr,
        )

    candidates.sort(
        key=score,
        reverse=True,
    )

    return candidates[0]


def extract_info_sync(
    url: str,
    proxy: Optional[str],
) -> Dict[str, Any]:

    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "format": "best",
        "socket_timeout": YT_DLP_TIMEOUT,
        "retries": 1,
        "fragment_retries": 1,
        "extractor_retries": 1,
        "cachedir": False,
        "nocheckcertificate": False,
        "geo_bypass": False,
        "check_formats": False,
    }

    if os.path.exists(COOKIES_PATH):
        options["cookiefile"] = COOKIES_PATH

    if proxy:
        options["proxy"] = proxy

    with yt_dlp.YoutubeDL(
        options
    ) as ydl:

        return ydl.extract_info(
            url,
            download=False,
        )


async def extract_stream(
    normalized_url: str,
    max_height: Optional[int],
    use_proxy: bool,
) -> Dict[str, Any]:

    proxy_candidates = [
        None
    ]

    if use_proxy:

        proxy = await proxy_manager.get_proxy()

        if proxy:
            proxy_candidates = [
                proxy,
                None,
            ]

    last_error = None

    for proxy in proxy_candidates:

        try:

            started = time.monotonic()

            info = await asyncio.to_thread(
                extract_info_sync,
                normalized_url,
                proxy,
            )

            formats = info.get(
                "formats",
                [],
            )

            selected = (
                select_combined_format(
                    formats,
                    max_height,
                )
            )

            if selected is None:
                raise APIError(
                    "No combined video/audio format is available"
                )

            stream_url = selected.get(
                "url"
            )

            if not stream_url:
                raise APIError(
                    "The selected format does not contain a stream URL"
                )

            result = {
                "success": True,
                "video_id": info.get(
                    "id"
                ),
                "title": info.get(
                    "title"
                ),
                "channel": (
                    info.get("channel")
                    or info.get("uploader")
                ),
                "uploader": info.get(
                    "uploader"
                ),
                "duration": info.get(
                    "duration"
                ),
                "thumbnail": info.get(
                    "thumbnail"
                ),
                "webpage_url": info.get(
                    "webpage_url"
                ),
                "stream_url": stream_url,
                "format_id": selected.get(
                    "format_id"
                ),
                "ext": selected.get(
                    "ext"
                ),
                "protocol": selected.get(
                    "protocol"
                ),
                "width": selected.get(
                    "width"
                ),
                "height": selected.get(
                    "height"
                ),
                "fps": selected.get(
                    "fps"
                ),
                "vcodec": selected.get(
                    "vcodec"
                ),
                "acodec": selected.get(
                    "acodec"
                ),
                "abr": selected.get(
                    "abr"
                ),
                "tbr": selected.get(
                    "tbr"
                ),
                "filesize": selected.get(
                    "filesize"
                ),
                "filesize_approx": selected.get(
                    "filesize_approx"
                ),
                "has_video": True,
                "has_audio": True,
                "combined": True,
                "proxy_used": bool(
                    proxy
                ),
                "processing_ms": round(
                    (
                        time.monotonic()
                        - started
                    )
                    * 1000,
                    2,
                ),
            }

            if proxy:
                proxy_manager.mark_success(
                    proxy
                )

            return result

        except Exception as exc:

            last_error = exc

            if proxy:
                proxy_manager.mark_failure(
                    proxy
                )

            logger.warning(
                "Extraction failed proxy=%s error=%s",
                bool(proxy),
                exc,
            )

    raise APIError(
        str(last_error)
        if last_error
        else "Stream extraction failed"
    )


async def get_stream(
    normalized_url: str,
    max_height: Optional[int],
    use_proxy: bool,
) -> Dict[str, Any]:

    cache_key = (
        normalized_url
        + "|"
        + str(max_height)
        + "|"
        + str(use_proxy)
    )

    cached = cache.get(
        cache_key
    )

    if cached is not None:

        return {
            **cached,
            "cached": True,
        }

    async with semaphore:

        cached = cache.get(
            cache_key
        )

        if cached is not None:

            return {
                **cached,
                "cached": True,
            }

        async def factory():

            return await extract_stream(
                normalized_url,
                max_height,
                use_proxy,
            )

        result = await inflight.get_or_create(
            cache_key,
            factory,
        )

        cache.set(
            cache_key,
            result,
        )

        return {
            **result,
            "cached": False,
        }


@app.get("/")
async def root():

    return {
        "success": True,
        "name": "YouTube Combined Stream API",
        "version": "1.0.0",
        "platform": "Vercel",
        "status": "ok",
        "docs": "/docs",
        "endpoints": {
            "stream": "/api/stream",
            "formats": "/api/formats",
            "health": "/api/health",
            "proxy_status": "/api/proxy/status",
            "proxy_refresh": "/api/proxy/refresh",
        },
    }


@app.get("/api/health")
async def health():

    return {
        "success": True,
        "status": "ok",
        "platform": "vercel",
        "cache_size": cache.size(),
        "cache_max_size": MAX_CACHE_SIZE,
        "cache_ttl": CACHE_TTL,
        "max_concurrent": MAX_CONCURRENT,
        "proxy": proxy_manager.stats(),
    }


@app.get("/api/stream")
async def stream(
    url: str = Query(
        ...,
        min_length=1,
        max_length=2048,
    ),
    max_height: Optional[int] = Query(
        default=None,
        ge=144,
        le=1080,
    ),
    proxy: bool = Query(
        default=True,
    ),
):

    try:

        normalized_url = (
            normalize_youtube_url(
                url
            )
        )

        return await get_stream(
            normalized_url,
            max_height,
            proxy,
        )

    except APIError as exc:

        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": str(exc),
            },
        )

    except Exception as exc:

        logger.exception(
            "Unexpected stream error"
        )

        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": "Internal server error",
            },
        )


@app.get("/api/formats")
async def formats(
    url: str = Query(
        ...,
        min_length=1,
        max_length=2048,
    ),
    max_height: Optional[int] = Query(
        default=None,
        ge=144,
        le=1080,
    ),
    proxy: bool = Query(
        default=True,
    ),
):

    try:

        normalized_url = (
            normalize_youtube_url(
                url
            )
        )

        proxy_url = None

        if proxy:
            proxy_url = (
                await proxy_manager.get_proxy()
            )

        info = await asyncio.to_thread(
            extract_info_sync,
            normalized_url,
            proxy_url,
        )

        result = []

        for fmt in info.get(
            "formats",
            [],
        ):

            if not is_combined_format(
                fmt
            ):
                continue

            if not fmt.get(
                "url"
            ):
                continue

            protocol = fmt.get(
                "protocol"
            )

            if protocol not in {
                "http",
                "https",
            }:
                continue

            height = fmt.get(
                "height"
            )

            if (
                max_height is not None
                and height is not None
                and height > max_height
            ):
                continue

            result.append(
                {
                    "format_id": fmt.get(
                        "format_id"
                    ),
                    "ext": fmt.get(
                        "ext"
                    ),
                    "protocol": fmt.get(
                        "protocol"
                    ),
                    "width": fmt.get(
                        "width"
                    ),
                    "height": fmt.get(
                        "height"
                    ),
                    "fps": fmt.get(
                        "fps"
                    ),
                    "vcodec": fmt.get(
                        "vcodec"
                    ),
                    "acodec": fmt.get(
                        "acodec"
                    ),
                    "abr": fmt.get(
                        "abr"
                    ),
                    "tbr": fmt.get(
                        "tbr"
                    ),
                    "filesize": fmt.get(
                        "filesize"
                    ),
                    "filesize_approx": fmt.get(
                        "filesize_approx"
                    ),
                    "has_video": True,
                    "has_audio": True,
                    "combined": True,
                }
            )

        result.sort(
            key=lambda item: (
                item.get(
                    "height"
                )
                or 0,
                item.get(
                    "tbr"
                )
                or 0,
            ),
            reverse=True,
        )

        return {
            "success": True,
            "video_id": info.get(
                "id"
            ),
            "title": info.get(
                "title"
            ),
            "proxy_used": bool(
                proxy_url
            ),
            "count": len(
                result
            ),
            "formats": result,
        }

    except APIError as exc:

        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": str(exc),
            },
        )

    except Exception as exc:

        logger.exception(
            "Format extraction failed"
        )

        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(exc),
            },
        )


@app.get("/api/proxy/status")
async def proxy_status():

    return {
        "success": True,
        "proxy": proxy_manager.stats(),
    }


@app.get("/api/proxy/refresh")
async def proxy_refresh():

    count = await proxy_manager.refresh(
        force=True
    )

    return {
        "success": True,
        "candidates": count,
    }
