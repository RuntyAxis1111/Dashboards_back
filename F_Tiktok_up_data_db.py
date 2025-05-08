# daily_account_stats_supabase.py
import os, datetime, requests, math
from supabase import create_client, Client

# credenciales 
SUPABASE_URL   = os.environ["https://reilyngaidrxfsoglnqz.supabase.co"]          # https://xxxx.supabase.co
SERVICE_KEY    = os.environ["eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJlaWx5bmdhaWRyeGZzb2dsbnF6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NjU1MDQ4NSwiZXhwIjoyMDYyMTI2NDg1fQ.RNRdyJbBNYSFiBGEIBWs_F0FzFBnid1IL6b4ygRgD6M"]
CLIENT_KEY     = os.environ["aw79zy77ngff0uu2"]
CLIENT_SECRET  = os.environ["dAEXYF4wArxbWDMz73BIiWo4dPgVcbIb"]
USERNAME       = os.getenv("TIKTOK_USERNAME", "pasealafama")
TABLE          = "DATA_PALF_TIKTOK"                  # respeta el snake/upper que uses

supabase: Client = create_client(SUPABASE_URL, SERVICE_KEY)

#  OAuth client_credentials 
def get_token() -> str:
    r = requests.post(
        "https://open.tiktokapis.com/v2/oauth/token/",
        data={
            "client_key": CLIENT_KEY,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

#  métricas de la cuenta 
def user_info(token: str) -> dict:
    url = ("https://open.tiktokapis.com/v2/research/user/info/"
           "?fields=follower_count")
    r = requests.post(url,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        json={"username": USERNAME}, timeout=30)
    r.raise_for_status()
    return r.json()["data"]

#  videos últimos 24 h 
def video_ids(token: str, since_ts: int):
    url = ("https://open.tiktokapis.com/v2/research/video/list/"
           "?fields=id,create_time")
    body = {"username": USERNAME, "max_count": 100}
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json"}
    vids, cursor, more = [], None, True
    while more:
        if cursor:
            body["cursor"] = cursor
        j = requests.post(url, headers=headers, json=body, timeout=30).json()["data"]
        vids.extend([v["id"] for v in j["videos"] if v["create_time"] >= since_ts])
        more, cursor = j.get("has_more", False), j.get("cursor")
    return vids

def video_stats(token: str, ids):
    stats, url = [], ("https://open.tiktokapis.com/v2/research/video/query/"
                      "?fields=view_count,like_count,comment_count,share_count")
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json"}
    for i in range(0, len(ids), 20):
        body = {"filters": {"video_ids": ids[i:i+20]}}
        stats += requests.post(url, headers=headers, json=body,
                               timeout=30).json()["data"]["videos"]
    return stats

def aggregate(videos):
    agg = {"total_views":0,"likes":0,"comments":0,"shares":0}
    for v in videos:
        agg["total_views"] += v["view_count"]
        agg["likes"]       += v["like_count"]
        agg["comments"]    += v["comment_count"]
        agg["shares"]      += v["share_count"]
    total = agg["total_views"]
    agg["engagement_rate"] = round((agg["likes"]+agg["comments"]+agg["shares"]) / total, 4) if total else None
    return agg

def main():
    token   = get_token()
    today   = datetime.date.today()
    since   = int((datetime.datetime.utcnow() -
                   datetime.timedelta(days=1)).timestamp())
    vids    = video_ids(token, since)
    vstats  = video_stats(token, vids) if vids else []
    daily   = aggregate(vstats)
    followers = user_info(token)["follower_count"]

    # followers_gain = seguidores de hoy - de ayer (query simple)
    prev = (supabase.table(TABLE)
                    .select("followers_gained")
                    .eq("date", today - datetime.timedelta(days=1))
                    .maybe_single())
    followers_gain = None if prev is None else followers - prev["followers_gained"]

    row = {
        "date": today.isoformat(),
        "account_id": USERNAME,
        **daily,
        "followers_gained": followers_gain,
        "audience_retention": None,
        "completion_rate": None,
    }
    supabase.table(TABLE).upsert(row, on_conflict="date,account_id").execute()
    print("Done →", row)

if __name__ == "__main__":
    main()
