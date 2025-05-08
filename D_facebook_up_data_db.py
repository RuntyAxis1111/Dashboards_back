import os, datetime, requests
from supabase import create_client, Client

# credenciales 
SUPABASE_URL   = os.environ[""]
SUPABASE_KEY   = os.environ[""]
FB_PAGE_ID     = os.environ[""]           
FB_TOKEN       = os.environ[""]      
TABLE          = "DATA_PALF_FACEBOOK"              

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

METRICS = ",".join([
    "page_impressions_unique",
    "page_impressions_paid_unique",
    "page_engaged_users",
    "page_reactions_like_total",
    "page_reactions_love_total",
    "page_reactions_wow_total",
    "page_reactions_haha_total",
    "page_reactions_angry_total",
    "page_video_views",
    "page_video_view_time",
    "page_fan_adds",
    "page_fans"
])

def fetch_page_insights(for_date: datetime.date) -> dict:
    since = for_date.isoformat()
    until = (for_date + datetime.timedelta(days=1)).isoformat()
    url = (
        f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/insights"
        f"?metric={METRICS}&period=day&since={since}&until={until}"
        f"&access_token={FB_TOKEN}"
    )
    data = requests.get(url, timeout=30).json()["data"]
    return {m["name"]: m["values"][0]["value"] for m in data}

def main():
    day   = datetime.date.today() - datetime.timedelta(days=1)  # métricas de AYER
    d     = fetch_page_insights(day)

    row = {
        "page_id":                 FB_PAGE_ID,
        "stat_date":               day.isoformat(),
        "reach_total":             d.get("page_impressions_unique"),
        "reach_paid":              d.get("page_impressions_paid_unique"),
        "engagements_total":       d.get("page_engaged_users"),
        "reactions_like":          d.get("page_reactions_like_total"),
        "reactions_love":          d.get("page_reactions_love_total"),
        "reactions_wow":           d.get("page_reactions_wow_total"),
        "reactions_haha":          d.get("page_reactions_haha_total"),
        "reactions_angry":         d.get("page_reactions_angry_total"),
        "video_views_total":       d.get("page_video_views"),
        "video_view_time_min":     d.get("page_video_view_time"),
        "followers_new":           d.get("page_fan_adds"),
        "followers_total":         d.get("page_fans"),
        "created_at":              datetime.datetime.utcnow().isoformat()
    }

    supabase.table(TABLE).upsert(row, on_conflict="stat_date,page_id").execute()
    print("Facebook insert OK")

if __name__ == "__main__":
    main()
