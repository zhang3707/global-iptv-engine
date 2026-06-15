import asyncio
import aiohttp
import time
import json
import os
import csv
import io

# =========================================================================
# ⚙️ 工业级配置面板：多级漏斗增量大坝（标准 CSV 解析版）
# =========================================================================
MAX_CONCURRENT_TASKS = 50     # 粗筛并发
TIMEOUT_FAST = 2.0            # ⚡ 粗筛超时：2秒不回应直接判定为死链
TIMEOUT_DEEP = 5.0            # ⏱️ 精测超时：5秒给跨境流握手
RE_TEST_DAYS = 3              # 🔄 查重机制：3天内测过的活台直接沿用

RAW_STREAMS_URL = "https://raw.githubusercontent.com/iptv-org/database/master/data/streams.csv"
DB_FILE = "database.json"     # 💾 本地状态数据库
OUTPUT_DIR = "output"

GLOBAL_COUNTRIES = {
    "CN": "中国内地", "HK": "中国香港", "TW": "中国台湾", 
    "US": "美国",     "GB": "英国",     "JP": "日本", 
    "KR": "韩国",     "FR": "法国",     "DE": "德国", 
    "CA": "加拿大"
}

def load_database():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"active": {}, "blacklist": {}}

def save_database(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

# =========================================================================
# 🧼 第一级漏斗：轻量级极速粗筛（干掉死链、识别黑名单）
# =========================================================================
async def fast_screen_stream(session, semaphore, stream, db):
    url = stream["url"]
    
    if url in db["blacklist"]:
        return None
    if url in db["active"] and (time.time() - db["active"][url].get("last_test", 0)) < (RE_TEST_DAYS * 86400):
        return db["active"][url]

    async with semaphore:
        try:
            async with session.head(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT_FAST) as resp:
                if resp.status in [200, 206, 301, 302]:
                    return stream 
        except Exception:
            pass
        
        db["blacklist"][url] = {"fail_time": time.time(), "reason": "Dead Link or Timeout"}
        if url in db["active"]: del db["active"][url]
        return None

# =========================================================================
# ⚡ 第二级漏斗：精准带宽与延迟拨测
# =========================================================================
async def deep_test_stream(session, semaphore, stream, db):
    url = stream["url"]
    if "delay" in stream:
        return stream

    async with semaphore:
        start_time = time.time()
        try:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT_DEEP) as resp:
                if resp.status in [200, 206]:
                    connect_time = time.time()
                    delay_ms = int((connect_time - start_time) * 1000)
                    
                    chunk_size = 1024 * 64
                    chunk_start = time.time()
                    content = await resp.content.read(chunk_size)
                    chunk_end = time.time()
                    
                    if not content: return None
                    
                    download_time = chunk_end - chunk_start
                    speed_kbs = int((len(content) / 1024) / (download_time if download_time > 0 else 0.001))
                    
                    stream["delay"] = delay_ms
                    stream["speed_kbs"] = speed_kbs
                    stream["resolution"] = "1080p" if speed_kbs > 800 or "hd" in url.lower() else "720p"
                    stream["last_test"] = time.time()
                    
                    db["active"][url] = stream
                    return stream
        except Exception:
            pass
        
        db["blacklist"][url] = {"fail_time": time.time(), "reason": "Deep Test Timeout"}
        if url in db["active"]: del db["active"][url]
        return None

# =========================================================================
# 🚀 主控制中心
# =========================================================================
async def main():
    start_all = time.time()
    db = load_database()
    print(f"📂 成功加载本地数据库：当前标记活台 {len(db['active'])} 个，拉黑死链 {len(db['blacklist'])} 个。")

    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    all_raw = []
    async with aiohttp.ClientSession() as session:
        print("📡 正在全量并网官方物理明文 CSV 数据库...")
        try:
            async with session.get(RAW_STREAMS_URL, timeout=30) as resp:
                if resp.status != 200:
                    print(f"❌ 抓取失败，状态码: {resp.status}")
                    return
                
                # 💥 工业级核心重构：将文本流喂给标准 csv 解析器，规避逗号错位地雷
                csv_text = await resp.text()
                f = io.StringIO(csv_text)
                reader = csv.reader(f)
                
                # 跳过表头 (channel, url, user_agent, referrer, ...)
                header = next(reader)
                
                for row in reader:
                    if len(row) < 2: continue
                    channel = row[0].strip()
                    url = row[1].strip()
                    
                    if "." in channel:
                        suffix = channel.split(".")[-1].upper()
                        if suffix in GLOBAL_COUNTRIES:
                            all_raw.append({
                                "channel": channel,
                                "title": channel.split(".")[0],
                                "url": url,
                                "user_agent": row[2].strip() if len(row) > 2 and row[2].strip() else "Mozilla/5.0",
                                "referrer": row[3].strip() if len(row) > 3 and row[3].strip() else None
                            })
        except Exception as e:
            print(f"❌ 运行期网络中断: {e}")
            return

    print(f"📊 数据库解析完毕！成功锁定全球核心候选流: {len(all_raw)} 条。开始第一级【漏斗粗筛】...")
    if not all_raw:
        print("⚠️ 候选池为空，请检查上游数据源结构是否变动。")
        return

    # 2. 批量执行第一级粗筛
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    async with aiohttp.ClientSession() as session:
        tasks = [fast_screen_stream(session, semaphore, s, db) for s in all_raw]
        screened_results = await asyncio.gather(*tasks)
        passed_streams = [r for r in screened_results if r is not None]
        
    print(f"🧼 粗筛收工！阻击了死链。晋级深度精测的种子流: {len(passed_streams)} 个。")

    # 3. 批量执行第二级精测
    async with aiohttp.ClientSession() as session:
        print("⚡ 开始对晋级种子发起第二级【精准带宽/延迟计算】...")
        tasks = [deep_test_stream(session, semaphore, s, db) for s in passed_streams]
        final_results = await asyncio.gather(*tasks)
        valid_streams = [r for r in final_results if r is not None]

    # 4. 落地存储并分类打包
    save_database(db)

    country_buckets = {c: [] for c in GLOBAL_COUNTRIES}
    for s in valid_streams:
        suffix = s["channel"].split(".")[-1].upper()
        if suffix in country_buckets:
            country_buckets[suffix].append(s)

    for country, streams in country_buckets.items():
        streams.sort(key=lambda x: x.get("delay", 9999))
        output_path = os.path.join(OUTPUT_DIR, f"api_{country.lower()}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(streams, f, ensure_ascii=False, indent=2)
        print(f"💾 【{GLOBAL_COUNTRIES[country]}】 最终产出全活秒开台: {len(streams)} 个 -> {output_path}")

    print(f"🎉 真正的全量全球化大坝清洗完毕！总耗时: {round(time.time() - start_all, 2)} 秒")

if __name__ == "__main__":
    asyncio.run(main())
