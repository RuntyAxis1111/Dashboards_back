#todo el siguiente codigo va estar dedinido por que primero vamos a necesitar que en config esten todas las cositas que necesitemos como por ejemplo que
# todas las api esten ya duncionando bien, vamos a hacer los cambios

import os, datetime, requests
from supabase import create_client, Client

#  credenciales & config
SUPABASE_URL   = os.environ[""]
SUPABASE_KEY   = os.environ[""]
IG_USER_ID     = os.environ[""]           
IG_TOKEN       = os.environ[""]      
TABLE          = "DATA_PALF_INSTAGRAM"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

ACC_METRICS = "impressions,reach,profile_views,followers_count"

def fetch_account_insights(for_date: datetime.date) -> dict:
    since = for_date.isoformat()
    until = (for_date + datetime.timedelta(days=1)).isoformat()
    url = (
        f"https://graph.facebook.com/v19.0/{IG_USER_ID}/insights"
        f"?metric={ACC_METRICS}&period=day&since={since}&until={until}"
        f"&access_token={IG_TOKEN}"
    )
    data = requests.get(url, timeout=30).json()["data"]
    return {m["name"]: m["values"][0]["value"] for m in data}

def list_media_ids(since_utc: datetime.datetime):
    url = (
        f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media"
        f"?fields=id,timestamp&access_token={IG_TOKEN}&limit=100"
    )
    ids, after = [], None
    while True:
        page = requests.get(url, timeout=30).json()
        ids += [
            m["id"] for m in page["data"]
            if datetime.datetime.fromisoformat(
                   m["timestamp"].replace("Z", "+00:00")) >= since_utc
        ]
        after = page.get("paging", {}).get("next")
        if not after:
            break
        url = after
    return ids

def media_totals(ids):
    metrics = "likes,comments,saves,plays"
    tot = {"likes":0,"comments":0,"saves":0,"plays":0}
    for mid in ids:
        url = (
            f"https://graph.facebook.com/v19.0/{mid}/insights"
            f"?metric={metrics}&access_token={IG_TOKEN}"
        )
        data = requests.get(url, timeout=30).json().get("data",[])
        for m in data:
            tot[m["name"]] += m["values"][0]["value"]
    return tot

def main():
    day      = datetime.date.today() - datetime.timedelta(days=1)
    sinceutc = datetime.datetime.combine(day, datetime.time.min,
                                         tzinfo=datetime.timezone.utc)

    acc  = fetch_account_insights(day)
    mids = list_media_ids(sinceutc)
    med  = media_totals(mids) if mids else {"likes":0,"comments":0,"saves":0,"plays":0}

    row = {
        "ig_user_id":        IG_USER_ID,
        "stat_date":         day.isoformat(),
        "impressions":       acc.get("impressions"),
        "reach":             acc.get("reach"),
        "profile_views":     acc.get("profile_views"),
        "followers_total":   acc.get("followers_count"),
        "likes":             med["likes"],
        "comments":          med["comments"],
        "saves":             med["saves"],
        "plays":             med["plays"],
        "created_at":        datetime.datetime.utcnow().isoformat()
    }

    supabase.table(TABLE).upsert(row, on_conflict="stat_date,ig_user_id").execute()
    print("Instagram insert OK")

if __name__ == "__main__":
    main()
