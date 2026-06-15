import asyncio
import aiohttp
import time
import json
import os

# =========================================================================
# ⚙️ 终极全球版：全量物理并网·台数暴增大坝引擎
# =========================================================================
TIMEOUT_LIMIT = 5.0           # 给跨国流留足 5 秒的握手时间
MAX_CONCURRENT_TASKS = 40     # 提高并发，加速冲洗几千个候选源

# 直接调用 iptv-org 最底层的明文大表，彻底绕过官方 Pages 的 API 严选限制！
RAW_STREAMS_URL = "https://raw.githubusercontent.com/iptv-org/database/master/data/streams.csv"

GLOBAL_COUNTRIES = {
    "CN": "中国内地", "HK": "中国香港", "TW": "中国台湾", 
    "US": "美国",     "GB": "英国",     "JP": "日本", 
    "KR": "韩国",     "FR": "法国",     "DE": "德国", 
    "CA": "加拿大"
}

OUTPUT_DIR = "output"

async def test_stream_details(session, semaphore, stream):
    url = stream.get("url")
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    headers = {"User-Agent": ua}
    if stream.get("referrer"):
        headers["Referer"] = stream["referrer"]

    async with semaphore:
        start_time = time.time()
        try:
            # 改用兼容性最强的 HEAD 探测，只要服务器应答 2xx/3xx，一律算活台！
            async with session.head(url, headers=headers, timeout=TIMEOUT_LIMIT) as response:
                if response.status in [200, 206, 301, 302]:
                    stream["delay"] = int((time.time() - start_time) * 1000)
                    stream["resolution"] = "1080p" if "hd" in url.lower() or "1080" in url else "720p"
                    return stream
        except Exception:
            pass
        return None

async def parse_raw_csv_database(session):
    """
    🔌 核心魔改：暴力解构官方几万条原始 CSV 数据库
    """
    streams = []
    print("📡 正在全量并网 iptv-org 最底层物理明文数据库...")
    try:
        async with session.get(RAW_STREAMS_URL, timeout=20) as resp:
            if resp.status == 200:
                text = await resp.text()
                lines = text.split("\n")
                
                for line in lines[1:]:
                    if not line.strip(): continue
                    parts = line.split(",")
                    if len(parts) >= 2:
                        channel = parts[0].strip()
                        url = parts[1].strip()
                        
                        # 🧠 暴力指纹拦截：只要频道名后缀匹配我们的全球矩阵，全部捞进来测试！
                        if "." in channel:
                            suffix = channel.split(".")[-1].upper()
                            if suffix in GLOBAL_COUNTRIES:
                                stream = {
                                    "channel": channel,
                                    "title": channel.split(".")[0],
                                    "url": url,
                                    "user_agent": parts[2].strip() if len(parts) > 2 and parts[2].strip() else "Mozilla/5.0",
                                    "referrer": parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                                }
                                streams.append(stream)
    except Exception as e:
        print(f"❌ 抓取全量明文大表失败: {e}")
    return streams

async def main():
    start_all = time.time()
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    async def run_wash():
        async with aiohttp.ClientSession() as session:
            # 1. 一枪轰出，捞出全人类维护的所有野生源候选大池子
            all_candidates = await parse_raw_csv_database(session)
            print(f"📊 全球候选种子池并网成功！共计锁定候选流: {len(all_candidates)} 条")

            # 2. 扔进国别桶
            country_buckets = {country: [] for country in GLOBAL_COUNTRIES}
            for s in all_candidates:
                suffix = s["channel"].split(".")[-1].upper()
                country_buckets[suffix].append(s)

            # 3. 柔性并发冲洗
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
            for country, streams in country_buckets.items():
                if not streams: continue
                
                print(f"⚡ 开始对 【{GLOBAL_COUNTRIES[country]}】 的 {len(streams)} 个野生候选流发起暴力并发测速...")
                tasks = [test_stream_details(session, semaphore, s) for s in streams]
                results = await asyncio.gather(*tasks)
                
                valid_streams = [r for r in results if r is not None]
                # 按延迟排序
                valid_streams.sort(key=lambda x: x["delay"])
                
                # 落地写入
                output_path = os.path.join(OUTPUT_DIR, f"api_{country.lower()}.json")
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(valid_streams, f, ensure_ascii=False, indent=2)
                print(f"💾 【{GLOBAL_COUNTRIES[country]}】 冲洗收工！最终全活存活频道: {len(valid_streams)} 个\n")

    await run_wash()
    print(f"🎉 真正的全量全球化大坝清洗完毕！总耗时: {round(time.time() - start_all, 2)} 秒")

if __name__ == "__main__":
    asyncio.run(main())
