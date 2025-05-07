"""
Extrae las métricas diarias de @pasealafama, las imprime
y las inserta en la tabla DATA_OF_PALF de Supabase
pendiente: actualizar db 
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
import requests
from requests_oauthlib import OAuth1

# 1) CREDENCIALES
#    (en producción usa variables de entorno; aquí las pego
#     para que corra tal cual)
CONSUMER_KEY    = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN    = ""
ACCESS_SECRET   = ""

SUPABASE_URL = ""
SUPABASE_KEY = ""

if not all([CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
    sys.exit("❌  Faltan credenciales de X.")

oauth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

# 2) HELPERS
def get_json(url: str, params: dict | None = None):
    """GET con back-off automático si la API responde 429."""
    while True:
        r = requests.get(url, params=params, auth=oauth, timeout=30)
        if r.status_code == 429:
            reset = int(r.headers.get("x-rate-limit-reset", time.time() + 60))
            wait  = max(reset - int(time.time()), 15)
            print(f"⏳  rate-limited → intento en {wait}s…")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()

# 3) CONFIG
USERNAME = "pasealafama"
if len(sys.argv) > 1:
    USERNAME = sys.argv[1]

API = "https://api.twitter.com/2"

# 4) LOOKUP DEL USUARIO
user = get_json(f"{API}/users/by/username/{USERNAME}",
                {"user.fields": "public_metrics"})
user_id   = user["data"]["id"]
followers = user["data"]["public_metrics"]["followers_count"]

# 5) TWEETS ÚLTIMAS 24 h
utc_now  = datetime.now(timezone.utc)
yday_iso = (utc_now - timedelta(days=1)).isoformat(timespec="seconds")

params = {
    "max_results": 100,
    "start_time": yday_iso,
    "tweet.fields": "public_metrics,non_public_metrics",
}
tweets = get_json(f"{API}/users/{user_id}/tweets", params=params)

# 6) AGREGAR MÉTRICAS
agg = dict.fromkeys(
    ["impressions", "likes", "retweets", "replies",
     "engagements", "tweets_count"], 0)

for t in tweets.get("data", []):
    pub   = t.get("public_metrics", {})
    non_p = t.get("non_public_metrics", {})
    agg["likes"]       += pub.get("like_count", 0)
    agg["retweets"]    += pub.get("retweet_count", 0)
    agg["replies"]     += pub.get("reply_count", 0)
    agg["impressions"] += non_p.get("impression_count", 0)
    agg["tweets_count"] += 1

agg["engagements"] = agg["likes"] + agg["retweets"] + agg["replies"]

# 7) IMPRIMIR REPORTE
date_str = utc_now.date().isoformat()
print(f"\n📊  Daily metrics for @{USERNAME} — {date_str}")
print(" Followers        :", followers)
print(" Tweets analysed  :", agg['tweets_count'])
print(" Impressions      :", agg['impressions'])
print(" Likes            :", agg['likes'])
print(" Retweets         :", agg['retweets'])
print(" Replies          :", agg['replies'])
print(" Total engagement :", agg['engagements'])

# 8) INSERTAR / ACTUALIZAR EN SUPABASE
if SUPABASE_URL and SUPABASE_KEY:
    payload = {
        "stat_date"    : date_str,
        "account_id"   : USERNAME,
        "impressions"  : agg["impressions"],
        "engagements"  : agg["engagements"],
        "likes"        : agg["likes"],
        "retweets"     : agg["retweets"],
        "replies"      : agg["replies"],
        "new_followers": None,
        "mentions"     : None
    }

    endpoint = f"{SUPABASE_URL}/rest/v1/DATA_OF_PALF"
    headers  = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    r = requests.post(
        endpoint,
        json=payload,
        headers=headers,
        params={"on_conflict": "stat_date,account_id"},
        timeout=15
    )

    if r.ok:
        print("✅  Fila insertada / actualizada en Supabase.")
    else:
        print("⚠️  Supabase error:", r.status_code, r.text)
