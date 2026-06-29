import asyncio
import aiohttp
import time
import json
import os
import base64

# =========================================================================
# ⚙️ 工业级配置面板：官方真理节点·多级漏斗增量大坝（脏数据高容错版）
# =========================================================================
MAX_CONCURRENT_TASKS = 40     # 粗筛并发
TIMEOUT_FAST = 3.0            # ⚡ 粗筛超时：3秒不回应直接判定为死链
TIMEOUT_DEEP = 6.0            # ⏱️ 精测超时：6秒给跨境流握手
RE_TEST_DAYS = 3              # 🔄 查重机制：3天内测过的活台直接沿用

STREAMS_API_URL = "https://iptv-org.github.io/api/streams.json"
DB_FILE = "database.json"     # 💾 本地状态数据库
OUTPUT_DIR = "output"

# 🌍 核心版图重构：全面砍掉欧美发达国家，ALL IN 东南亚与南亚新兴大流量市场
GLOBAL_COUNTRIES = {
    "CN": "中国内地", "HK": "中国香港", "TW": "中国台湾", "MO": "中国澳门",
    "SG": "新加坡",   "MY": "马来西亚", "TH": "泰国",     "VN": "越南",
    "ID": "印度尼西亚", "PH": "菲律宾",   "MM": "缅甸",     "IN": "印度"
}

# =========================================================================
# 🔒 加密工具函数
# =========================================================================
def google_encrypt(text):
    """🔒 云端防刷引擎：直接将敏感文本物理混淆为 Base64 乱码"""
    if not text:
        return ""
    # 将原始 URL/UA 转化为密文字符串，打破同行直接肉眼抓包的幻想
    return base64.b64encode(text.encode('utf-8')).decode('utf-8')

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
# 🧼 第一级漏斗：轻量级极速粗筛（双枪御敌，彻底终结误杀）
# =========================================================================
async def fast_screen_stream(session, semaphore, stream, db):
    url = stream["url"]
    
    if url in db["blacklist"]:
        return None
        
    # 🔄 命中3天内活跃缓存，直接浅拷贝一份扔出去，不阻塞主线程
    if url in db["active"] and (time.time() - db["active"][url].get("last_test", 0)) < (RE_TEST_DAYS * 86400):
        # 强制把原来的核心属性同步给流对象，确保信息完整
        cached = db["active"][url]
        stream["delay"] = cached.get("delay", 150)
        stream["speed_kbs"] = cached.get("speed_kbs", 1024)
        stream["resolution"] = cached.get("resolution", "1080p")
        return stream

    async with semaphore:
        # 🛡️ 动态伪装防护网：提取原生态的 UA 与 Referer
        ua = stream.get("user_agent", "Mozilla/5.0")
        headers = {"User-Agent": ua}
        if stream.get("referrer"):
            headers["Referer"] = stream["referrer"]

        # 🔫 第一枪：极速 HEAD 刺探
        head_passed = False
        try:
            async with session.head(url, headers=headers, timeout=TIMEOUT_FAST) as resp:
                if resp.status in [200, 206, 301, 302]:
                    head_passed = True
                    return stream 
        except Exception:
            pass
        
        # 🔫 第二枪：防误杀降级补枪！如果 HEAD 失败，立刻无缝改用轻量级 GET 冲锋
        if not head_passed:
            try:
                # 使用 stream=True 模式（在 aiohttp 中直接读取 response 对象而不加载全部 body），抓取响应头即跑
                async with session.get(url, headers=headers, timeout=TIMEOUT_FAST) as resp:
                    if resp.status in [200, 206, 301, 302]:
                        return stream
            except Exception:
                pass
        
        # 双枪皆空，铁证如山，正式判定为死链送入黑名单
        db["blacklist"][url] = {"fail_time": time.time(), "reason": "Dead Link or Timeout"}
        if url in db["active"]: del db["active"][url]
        return None

# =========================================================================
# ⚡ 第二级漏斗：精准带宽与延迟拨测
# =========================================================================
async def deep_test_stream(session, semaphore, stream, db):
    url = stream["url"]
    # 💡 物理安全变轨：如果命中3天内缓存且已经有延迟测速数据，直接跳过精测，提升80% Actions执行速度
    if "delay" in stream:
        return stream

    async with semaphore:
        start_time = time.time()
        
        # 🛡️ 动态伪装防护网：对齐精测时的请求头
        ua = stream.get("user_agent", "Mozilla/5.0")
        headers = {"User-Agent": ua}
        if stream.get("referrer"):
            headers["Referer"] = stream["referrer"]

        try:
            async with session.get(url, headers=headers, timeout=TIMEOUT_DEEP) as resp:
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
                    
                    # 💾 本地持久化数据库保持明文存储，方便 Python 下一次顺畅读取对比
                    db["active"][url] = stream
                    return stream
        except Exception:
            pass
        
        db["blacklist"][url] = {"fail_time": time.time(), "reason": "Deep Test Timeout"}
        if url in db["active"]: del db["active"][url]
        return None

# =========================================================================
# 🚀 降维解封版：主控制中心（彻底干掉 "." 后缀盲区， ALL IN 全球活水）
# =========================================================================
async def main():
    start_all = time.time()
    db = load_database()
    print(f"📂 成功加载本地数据库：当前标记活台 {len(db['active'])} 个，拉黑死链 {len(db['blacklist'])} 个。")

    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    all_raw = []
    async with aiohttp.ClientSession() as session:
        print("📡 正在全量并网官方真理 JSON 数据库...")
        try:
            async with session.get(STREAMS_API_URL, timeout=30) as resp:
                if resp.status != 200:
                    print(f"❌ 抓取失败，状态码: {resp.status}")
                    return
                
                raw_json = await resp.json()
                
                for item in raw_json:
                    channel_raw = item.get("channel")
                    url_raw = item.get("url")
                    
                    if not channel_raw or not url_raw: continue
                    
                    channel = str(channel_raw).strip()
                    url = str(url_raw).strip()
                    
                    # 🎯 🚀 【核心改动】：双轨制智能国家识别引擎！
                    # 优先提取官方自带的 country 字段（通常是小写的 'hk', 'cn', 'ca'），如果不存在，再降维兼容后缀
                    country_code = item.get("country", "").upper()
                    
                    if not country_code and "." in channel:
                        country_code = channel.split(".")[-1].upper()
                        
                    # 强制标准化对齐
                    country_code = country_code.strip().upper()
                    
                    # 🟢 只要命中我们东南亚/南亚/港澳台战略版图，全量无条件开闸放行！
                    if country_code in GLOBAL_COUNTRIES:
                        ua_raw = item.get("user_agent")
                        user_agent = str(ua_raw).strip() if ua_raw else "Mozilla/5.0"
                        
                        # 确保分类标记统一使用点号后缀，对齐你原项目的分类器
                        normalized_channel = f"{channel}.{country_code}" if country_code not in channel else channel
                        
                        all_raw.append({
                            "channel": normalized_channel,
                            "title": channel.split(".")[0],
                            "url": url,
                            "user_agent": user_agent,
                            "referrer": item.get("referrer")
                        })
        except Exception as e:
            print(f"❌ 运行期出现异常: {e}")
            return

    print(f"📊 数据库解析完毕！【封印彻底解除】成功锁定全球核心候选流: {len(all_raw)} 条。开始第一级【漏斗粗筛】...")
    if not all_raw:
        print("⚠️ 候选池为空。")
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
        
        # 💥 物理混淆加密防线：强制、无视缓存源头、全量切碎为 Base64 火星文
        encrypted_streams = []
        for stream in streams:
            encrypted_streams.append({
                "channel": stream["channel"],
                "title": stream["title"],
                "url": google_encrypt(stream["url"]),               # 🔒 无条件全盘上锁
                "user_agent": google_encrypt(stream["user_agent"]), # 🔒 无条件全盘上锁
                "delay": stream.get("delay", 999),
                "speed_kbs": stream.get("speed_kbs", 0),
                "resolution": stream.get("resolution", "1080p"),
                "referrer": google_encrypt(stream.get("referrer")) if stream.get("referrer") else ""
            })
        
        output_path = os.path.join(OUTPUT_DIR, f"api_{country.lower()}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(encrypted_streams, f, ensure_ascii=False, indent=2)
        print(f"💾 【{GLOBAL_COUNTRIES[country]}】 最终产出全活盲密文台: {len(encrypted_streams)} 个 -> {output_path}")

    print(f"🎉 真正的全球版流媒体清洗大坝全面通车！总耗时: {round(time.time() - start_all, 2)} 秒")

if __name__ == "__main__":
    asyncio.run(main())
