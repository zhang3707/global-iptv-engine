import asyncio
import aiohttp
import time
import json
import os

# =========================================================================
# ⚙️ 终极解耦配置：直接绕过 iptv-org 的 API 限制，去捞他们的物理源头
# =========================================================================
TIMEOUT_LIMIT = 4.0           
MAX_CONCURRENT_TASKS = 30     
TARGET_COUNTRIES = ["CN", "HK", "TW", "US", "JP", "KR"]

# 💥 降维打击：直接调用 iptv-org 在 GitHub 仓库里的原始物理明文数据文件（100% 不会被限流截断！）
RAW_STREAMS_URL = "https://raw.githubusercontent.com/iptv-org/database/master/data/streams.csv"

# 🔌 同时聚合全网其他高质量明文订阅源（形成多物理源并网备灾）
CUSTOM_SUBSCRIPTIONS = [
    "https://raw.githubusercontent.com/Guovin/iptv-api/master/output/result.m3u",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv6.m3u"
]

OUTPUT_DIR = "output"

async def test_stream_details(session, semaphore, stream):
    url = stream.get("url")
    ua = stream.get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    headers = {"User-Agent": ua}
    
    async with semaphore:
        start_time = time.time()
        try:
            async with session.head(url, headers=headers, timeout=TIMEOUT_LIMIT) as response:
                if response.status in [200, 206, 302]:
                    stream["delay"] = int((time.time() - start_time) * 1000)
                    stream["resolution"] = "1080p HD" if "hd" in url.lower() or "1080" in url else "720p"
                    return stream
        except Exception:
            pass
        return None

async def parse_raw_csv_streams(session):
    """
    🎯 核心硬核魔改：直接解析 iptv-org 最底层的明文 CSV 数据库
    这等于把数据全盘私有化下载到了你自己的虚拟机内存中，官方怎么限流都拿你没办法！
    """
    streams = []
    print("📡 正在绕过官方API，直击物理源头拉取原始 CSV 数据库...")
    try:
        async with session.get(RAW_STREAMS_URL, timeout=15) as resp:
            if resp.status == 200:
                text = await resp.text()
                lines = text.split("\n")
                
                # CSV 的头部行特征解析
                # 格式通常是: channel,url,user_agent,referrer,banned,...
                header = [h.strip() for h in lines[0].split(",")]
                
                for line in lines[1:]:
                    if not line.strip(): continue
                    # 粗暴切分字段（简单对齐标准）
                    parts = line.split(",")
                    if len(parts) >= 2:
                        channel = parts[0].strip()
                        url = parts[1].strip()
                        
                        # 判断是不是我们要测试的目标国家频道
                        if any(f".{c.lower()}" in channel for c in TARGET_COUNTRIES):
                            stream = {
                                "channel": channel,
                                "title": channel.split(".")[0], # 用频道名做临时标题
                                "url": url,
                                "user_agent": parts[2].strip() if len(parts) > 2 and parts[2].strip() else "Mozilla/5.0",
                                "referrer": parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                            }
                            streams.append(stream)
    except Exception as e:
        print(f"❌ 抓取物理源头失败: {e}")
    return streams

async def parse_m3u_subscription(session, url):
    streams = []
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                text = await resp.text()
                lines = text.split("\n")
                current_info = {}
                for line in lines:
                    line = line.strip()
                    if line.startswith("#EXTINF:"):
                        title = line.split(",")[-1].strip()
                        country = "CN"
                        if "hbo" in title.lower() or "bbc" in title.lower(): country = "US"
                        current_info = {"title": title, "channel": f"Sub.{title}.{country.lower()}"}
                    elif line.startswith("http") and current_info:
                        current_info["url"] = line
                        streams.append(current_info)
                        current_info = {}
    except Exception:
        pass
    return streams

async def main():
    start_all = time.time()
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    all_raw_streams = []
    
    async with aiohttp.ClientSession() as session:
        # 🚀 第一通路：从官方底裤仓库捞取全量明文数据
        raw_csv_data = await parse_raw_csv_streams(session)
        all_raw_streams.extend(raw_csv_data)

        # 🚀 第二通路：强力并网全网高含金量 M3U 订阅
        for sub_url in CUSTOM_SUBSCRIPTIONS:
            sub_streams = await parse_m3u_subscription(session, sub_url)
            all_raw_streams.extend(sub_streams)

    print(f"📊 独立大池子并网成功！累计锁定目标候选流: {len(all_raw_streams)} 条")

    # 分拣桶
    country_buckets = {country: [] for country in TARGET_COUNTRIES}
    for stream in all_raw_streams:
        channel_id = stream.get("channel", "")
        if channel_id and "." in channel_id:
            suffix = channel_id.split(".")[-1].upper()
            if suffix in country_buckets:
                country_buckets[suffix].append(stream)

    # 柔性测速
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    async with aiohttp.ClientSession() as session:
        for country, streams in country_buckets.items():
            if not streams: continue
                
            print(f"⚡ 开始对 [{country}] 的 {len(streams)} 个流发起全自主网络测速...")
            tasks = [test_stream_details(session, semaphore, s) for s in streams]
            results = await asyncio.gather(*tasks)
            
            valid_streams = [r for r in results if r is not None]
            valid_streams.sort(key=lambda x: x["delay"])
            
            # 落地写入你自己的仓库
            output_path = os.path.join(OUTPUT_DIR, f"api_{country.lower()}.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(valid_streams, f, ensure_ascii=False, indent=2)
            print(f"💾 [{country}] 测速收工！真正全活可用台: {len(valid_streams)} 个 -> 已同步至仓库")

    print(f"🎉 史诗级完全自主清洗圆满结束！累计耗时: {round(time.time() - start_all, 2)} 秒")

if __name__ == "__main__":
    asyncio.run(main())
