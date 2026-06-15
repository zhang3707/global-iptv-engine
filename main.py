import asyncio
import aiohttp
import time
import json
import os

# =========================================================================
# ⚙️ 工业级配置面板：多级漏斗增量大坝
# =========================================================================
MAX_CONCURRENT_TASKS = 50     # 粗筛并发可以开得极大
TIMEOUT_FAST = 2.0            # ⚡ 粗筛超时：2秒不回应直接判定为死链
TIMEOUT_DEEP = 5.0            # ⏱️ 精测超时：5秒给跨境流握手
RE_TEST_DAYS = 3              # 🔄 查重机制：活台在 3 天内不需要重复测速，直接沿用历史数据

RAW_STREAMS_URL = "https://raw.githubusercontent.com/iptv-org/database/master/data/streams.csv"
DB_FILE = "database.json"     # 💾 核心本地状态数据库（存放历史测速、黑名单、存活状态）
OUTPUT_DIR = "output"

TARGET_COUNTRIES = ["CN", "HK", "TW", "US", "JP", "KR", "GB"]

def load_database():
    """读取历史状态机数据库"""
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"active": {}, "blacklist": {}}

def save_database(db):
    """保存状态机数据库"""
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

# =========================================================================
# 🧼 第一级漏斗：轻量级极速粗筛（干掉死链、识别黑名单）
# =========================================================================
async def fast_screen_stream(session, semaphore, stream, db):
    url = stream["url"]
    
    # 🛡️ 查重与拦截：如果在永久黑名单里，或者3天内刚刚测过是活的，直接跳过
    if url in db["blacklist"]:
        return None
    if url in db["active"] and (time.time() - db["active"][url].get("last_test", 0)) < (RE_TEST_DAYS * 86400):
        # 沿用历史数据，不重复跑网络请求
        return db["active"][url]

    async with semaphore:
        try:
            # 用极其轻量的 HEAD 请求进行一触即返的粗测
            async with session.head(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT_FAST) as resp:
                if resp.status in [200, 206, 301, 302]:
                    return stream # 粗筛通过，晋级下一轮
        except Exception:
            pass
        
        # 🚫 粗筛失败：直接打入黑名单暂存区
        db["blacklist"][url] = {"fail_time": time.time(), "reason": "Dead Link or Timeout"}
        if url in db["active"]: del db["active"][url]
        return None

# =========================================================================
# ⚡ 第二级漏斗：精准带宽与延迟拨测（只对粗筛通过的活台开火）
# =========================================================================
async def deep_test_stream(session, semaphore, stream, db):
    url = stream["url"]
    # 如果是沿用历史数据的台，直接放行
    if "delay" in stream:
        return stream

    async with semaphore:
        start_time = time.time()
        try:
            # 浪涌式拉取前 64KB 数据切片，死算真实下行速率与延迟
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
                    
                    # 更新流状态属性
                    stream["delay"] = delay_ms
                    stream["speed_kbs"] = speed_kbs
                    stream["resolution"] = "1080p HD" if speed_kbs > 800 or "hd" in url.lower() else "720p"
                    stream["last_test"] = time.time()
                    
                    # 写入活台数据库
                    db["active"][url] = stream
                    return stream
        except Exception:
            pass
        
        # 精测失败，打入黑名单
        db["blacklist"][url] = {"fail_time": time.time(), "reason": "Deep Test Timeout"}
        if url in db["active"]: del db["active"][url]
        return None

# =========================================================================
# 🚀 大坝中央调度指挥中心
# =========================================================================
async def main():
    start_all = time.time()
    db = load_database()
    print(f"📂 成功加载本地数据库：当前标记活台 {len(db['active'])} 个，拉黑死链 {len(db['blacklist'])} 个。")

    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    # 1. 抓取全球原始总表
    all_raw = []
    async with aiohttp.ClientSession() as session:
        print("📡 正在并网官方最底层物理明文数据库...")
        try:
            async with session.get(RAW_STREAMS_URL, timeout=20) as resp:
                if resp.status == 200:
                    lines = (await resp.text()).split("\n")
                    for line in lines[1:]:
                        if not line.strip(): continue
                        parts = line.split(",")
                        if len(parts) >= 2:
                            channel = parts[0].strip()
                            if "." in channel and channel.split(".")[-1].upper() in TARGET_COUNTRIES:
                                all_raw.append({
                                    "channel": channel, "title": channel.split(".")[0], "url": parts[1].strip()
                                })
        except Exception as e:
            print(f"❌ 抓取大表失败: {e}")
            return

    print(f"📊 本次触发的目标候选流共计: {len(all_raw)} 条。开始第一级【漏斗粗筛】...")

    # 2. 批量执行第一级粗筛
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    async with aiohttp.ClientSession() as session:
        tasks = [fast_screen_stream(session, semaphore, s, db) for s in all_raw]
        screened_results = await asyncio.gather(*tasks)
        passed_streams = [r for r in screened_results if r is not None]
        
    print(f"🧼 粗筛收工！阻击了大量死链和历史黑名单。晋级精测的种子流: {len(passed_streams)} 个。")

    # 3. 批量执行第二级精测（只对新晋级、或者需要更新的流发起）
    async with aiohttp.ClientSession() as session:
        print("⚡ 开始对晋级种子发起第二级【精测排序与带宽计算】...")
        tasks = [deep_test_stream(session, semaphore, s, db) for s in passed_streams]
        final_results = await asyncio.gather(*tasks)
        valid_streams = [r for r in final_results if r is not None]

    # 4. 落地存储状态机，并产出分国 API JSON
    save_database(db)
    print(f"💾 本地记忆状态库 `database.json` 增量同步完毕。")

    # 按国家分桶输出纯净菜单
    country_buckets = {c: [] for c in TARGET_COUNTRIES}
    for s in valid_streams:
        suffix = s["channel"].split(".")[-1].upper()
        if suffix in country_buckets:
            country_buckets[suffix].append(s)

    for country, streams in country_buckets.items():
        streams.sort(key=lambda x: x.get("delay", 9999))
        output_path = os.path.join(OUTPUT_DIR, f"api_{country.lower()}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(streams, f, ensure_ascii=False, indent=2)
        print(f"💾 产出分国极速菜单 -> {output_path} (共 {len(streams)} 个有效台)")

    print(f"🎉 智商升级版大坝全盘收网！总耗时: {round(time.time() - start_all, 2)} 秒")

if __name__ == "__main__":
    asyncio.run(main())
