import asyncio
import aiohttp
import time
import json
import os

TIMEOUT_LIMIT = 2.0  
MAX_CONCURRENT_TASKS = 60  
TARGET_COUNTRIES = ["CN", "HK", "TW", "US", "JP", "KR"]  
STREAMS_API = "https://iptv-org.github.io/api/streams.json"
OUTPUT_DIR = "output"

async def test_single_stream(session, semaphore, stream):
    url = stream.get("url")
    ua = stream.get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    headers = {"User-Agent": ua}
    if stream.get("referrer"):
        headers["Referer"] = stream["referrer"]

    async with semaphore:
        start_time = time.time()
        try:
            async with session.get(url, headers=headers, timeout=TIMEOUT_LIMIT) as response:
                if response.status == 200:
                    stream["delay"] = int((time.time() - start_time) * 1000)
                    return stream
        except Exception:
            pass
        return None

async def process_country(session, semaphore, country_code, streams):
    print(f"⚡ 正在对 [{country_code}] 的 {len(streams)} 个频道发起并发网络拨测...")
    tasks = [test_single_stream(session, semaphore, s) for s in streams]
    results = await asyncio.gather(*tasks)
    valid_streams = [r for r in results if r is not None]
    valid_streams.sort(key=lambda x: x["delay"])
    return valid_streams

async def main():
    start_all = time.time()
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print("📡 正在从 iptv-org 拉取全球全量直播源数据库...")
    async with aiohttp.ClientSession() as session:
        async with session.get(STREAMS_API) as resp:
            if resp.status != 200:
                print("❌ 错误：拉取全球总库失败")
                return
            all_streams = await resp.json()

    print(f"📊 全球总库拉取成功，共计含有 {len(all_streams)} 条原始流。")

    country_buckets = {country: [] for country in TARGET_COUNTRIES}
    for stream in all_streams:
        channel_id = stream.get("channel")
        if channel_id and "." in channel_id:
            suffix = channel_id.split(".")[-1].upper()
            if suffix in country_buckets:
                country_buckets[suffix].append(stream)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    async with aiohttp.ClientSession() as session:
        for country, streams in country_buckets.items():
            if not streams:
                continue
            cleaned_list = await process_country(session, semaphore, country, streams)
            output_path = os.path.join(OUTPUT_DIR, f"api_{country.lower()}.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(cleaned_list, f, ensure_ascii=False, indent=2)
            print(f"💾 [{country}] 清洗完毕！留存纯净台: {len(cleaned_list)} 个 -> 已同步至 {output_path}")

    print(f"🎉 全盘耗时: {round(time.time() - start_all, 2)} 秒")

if __name__ == "__main__":
    asyncio.run(main())
