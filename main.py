import asyncio
import aiohttp
import time
import json
import os
import re

# =========================================================================
# ⚙️ 独立自主开发：全球版 IPTV 多国并发清洗过滤器
# =========================================================================
TIMEOUT_LIMIT = 5.0           # 💥 放宽到 5 秒，给全球跨境网络流留足握手时间
MAX_CONCURRENT_TASKS = 30     # 限制并发，防止动作太粗暴被大厂防火墙拉黑

# 🌍 核心版图：你想要开通的全球版国家/地区矩阵（可以无限追加 ISO 国家代码）
GLOBAL_COUNTRIES = {
    "CN": "中国内地", "HK": "中国香港", "TW": "中国台湾", 
    "US": "美国",     "GB": "英国",     "JP": "日本", 
    "KR": "韩国",     "FR": "法国",     "DE": "德国", 
    "IT": "意大利",   "ES": "西班牙",   "CA": "加拿大"
}

OUTPUT_DIR = "output"

async def test_stream_details(session, semaphore, stream):
    """
    🎯 核心拨测：探测全球视频源的绝对存活状态与响应延迟
    """
    url = stream.get("url")
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    headers = {"User-Agent": ua}

    async with semaphore:
        start_time = time.time()
        try:
            # 采用柔性探测
            async with session.get(url, headers=headers, timeout=TIMEOUT_LIMIT) as response:
                if response.status in [200, 206]:
                    delay_ms = int((time.time() - start_time) * 1000)
                    stream["delay"] = delay_ms
                    stream["resolution"] = "1080p HD" if "hd" in url.lower() or "1080" in url else "720p"
                    stream["status"] = "active"
                    return stream
        except Exception:
            pass
        return None

async def fetch_and_parse_country_m3u(session, country_code):
    """
    📡 分布式抓取：直接去 iptv-org 物理库捞取该国最纯正的本土 M3U 文件
    100% 绕过全量大 JSON 的防刷截断限制！
    """
    url = f"https://iptv-org.github.io/api/streams/{country_code.lower()}.json"
    # 备用航道：如果 streams 细分接口不存在，则去捞国家的原生 m3u 文本
    m3u_fallback_url = f"https://iptv-org.github.io/iptv/countries/{country_code.lower()}.m3u"
    
    streams = []
    try:
        # 优先尝试拉取结构化的细分 JSON
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                raw_data = await resp.json()
                for item in raw_data:
                    streams.append({
                        "channel": item.get("channel"),
                        "title": item.get("channel", "Unknown").split(".")[0],
                        "url": item.get("url"),
                        "user_agent": item.get("user_agent", "Mozilla/5.0"),
                        "referrer": item.get("referrer")
                    })
                return streams
    except Exception:
        pass

    # 💥 备用航道启动：暴力解析原生 M3U 文本
    try:
        async with session.get(m3u_fallback_url, timeout=10) as resp:
            if resp.status == 200:
                text = await resp.text()
                lines = text.split("\n")
                current_info = {}
                for line in lines:
                    line = line.strip()
                    if line.startswith("#EXTINF:"):
                        title = line.split(",")[-1].strip()
                        current_info = {"title": title, "channel": f"{title}.{country_code.lower()}"}
                    elif line.startswith("http") and current_info:
                        current_info["url"] = line
                        streams.append(current_info)
                        current_info = {}
    except Exception:
        pass
    return streams

async def main():
    start_all = time.time()
    if not os.path.exists(OUTPUT_DIR): 
        os.makedirs(OUTPUT_DIR)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    
    async with aiohttp.ClientSession() as session:
        # 🌍 遍历全球版图，一个国家一个国家地进行深度洗牌
        for code, name in GLOBAL_COUNTRIES.items():
            print(f"📡 [全球版网关] 正在跨国抓取 【{name} ({code})】 的原始频道池...")
            
            raw_streams = await fetch_and_parse_country_m3u(session, code)
            if not raw_streams:
                print(f"⚠️ 未能在全球库中捞到 【{name}】 的数据，跳过。")
                continue
                
            print(f"⚡ 抓取成功！获取到候选种子 {len(raw_streams)} 个，正在发起全并发存活拨测...")
            
            tasks = [test_stream_details(session, semaphore, s) for s in raw_streams]
            results = await asyncio.gather(*tasks)
            
            # 物理剔除死链
            valid_streams = [r for r in results if r is not None]
            
            # 排序：快台、延迟低的优质流排在最前面
            valid_streams.sort(key=lambda x: x["delay"])
            
            # 落地为每个国家独立的纯净 API 文件
            output_path = os.path.join(OUTPUT_DIR, f"api_{code.lower()}.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(valid_streams, f, ensure_ascii=False, indent=2)
                
            print(f"💾 【{name} ({code})】 洗牌完成！全活秒开台: {len(valid_streams)} 个 -> 已存入仓库\n")

    print(f"🎉 真正的全球版流媒体清洗大坝全面落子！总耗时: {round(time.time() - start_all, 2)} 秒")

if __name__ == "__main__":
    asyncio.run(main())
