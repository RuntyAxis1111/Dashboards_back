import os, datetime, requests
from supabase import create_client, Client

#  credenciales
SUPABASE_URL   = os.environ[""]
SUPABASE_KEY   = os.environ[""]
YT_CHANNEL_ID  = os.environ[""]        
YT_CLIENT_ID   = os.environ[""]
YT_CLIENT_SEC  = os.environ[""]
YT_REFRESH_TK  = os.environ[""]      
TABLE          = "DATA_PALF_YOUTUBE"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

#  1. refrescar access-token 
def yt_access_token() -> str:
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": YT_CLIENT_ID,
            "client_secret": YT_CLIENT_SEC,
            "refresh_token": YT_REFRESH_TK,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

HEAD = {
    "Accept": "application/json",
}

# helper para llamar a /reports
YTA_URL = "https://youtubeanalytics.googleapis.com/v2/reports"

def yt_report(params: dict, token: str) -> list:
    # ids, startDate, endDate, metrics, dimensions, filters (opcional)
    p = params.copy()
    p["access_token"] = token
    r = requests.get(YTA_URL, params=p, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r.json().get("rows", [])

#  2. bajar métricas crudas 
def fetch_all(for_date: datetime.date, token: str) -> dict:
    day = for_date.isoformat()
    base_params = {
        "ids":    f"channel=={YT_CHANNEL_ID}",
        "startDate": day,
        "endDate":   day,
        "dimensions": "day",
    }

    # 2A · récord principal (todas las métricas "simples")
    main_metrics = ",".join([
        "views",
        "estimatedMinutesWatched",
        "averageViewDuration",
        "likes",
        "comments",
        "shares",
        "subscribersGained",
        "thumbnailImpressionsClickThroughRate",
        "impressions",
        "impressionsCtr",
        "estimatedRevenue"
    ])
    mrow = yt_report({**base_params, "metrics": main_metrics}, token)[0]

    # orden ↔ métricas
    (views, watch_min, avg_dur, likes, comments, shares,
     subs_gain, thumb_ctr, impressions, impressions_ctr,
     revenue) = mrow[1:]  # índice 0 = fecha

    # 2B · tráfico (search / suggested / external)
    tr_metrics = "views"
    tr_dim     = "day,trafficSourceType"
    rows_tr    = yt_report({**base_params, "metrics": tr_metrics,
                            "dimensions": tr_dim}, token)
    traffic = {"SEARCH":0, "SUGGESTED_VIDEO":0, "EXT_URL":0}
    for _, ttype, v in rows_tr:
        if ttype in traffic:
            traffic[ttype] = v

    # 2C · shorts vs long-form
    vt_metrics = "views,estimatedMinutesWatched"
    vt_dim     = "day,videoType"
    rows_vt    = yt_report({**base_params, "metrics": vt_metrics,
                            "dimensions": vt_dim}, token)
    shorts_views = long_views = shorts_min = long_min = 0
    for _, vtype, v, m in rows_vt:
        if vtype == "SHORTS":
            shorts_views, shorts_min = v, m
        elif vtype == "REGULAR":
            long_views, long_min = v, m

    # 2D · retención 30 s
    ret_rows = yt_report({**base_params,
                          "metrics": "audienceWatchRatio",
                          "dimensions": "day,elapsedVideoTimeRatio",
                          "filters": "elapsedVideoTimeRatio==0.5"}, token)
    retention = ret_rows[0][2] if ret_rows else None

    return {
        "total_views":             views,
        "total_watch_time_min":    watch_min,
        "avg_view_duration_sec":   avg_dur,
        "retention_30s_pct":       retention,
        "likes":                   likes,
        "comments":                comments,
        "shares":                  shares,
        "subscribers_gained":      subs_gain,
        "thumbnail_ctr_pct":       thumb_ctr,
        "traffic_search_views":    traffic["SEARCH"],
        "traffic_suggested_views": traffic["SUGGESTED_VIDEO"],
        "traffic_external_views":  traffic["EXT_URL"],
        "shorts_views":            shorts_views,
        "longform_views":          long_views,
        "shorts_watch_time_min":   shorts_min,
        "longform_watch_time_min": long_min,
        "revenue_estimated_usd":   revenue,
        "impressions":             impressions,
        "impressions_ctr_pct":     impressions_ctr,
    }

#  3. main / insert en Supabase 
def main():
    token   = yt_access_token()
    day     = datetime.date.today() - datetime.timedelta(days=1)  
    stats   = fetch_all(day, token)

    row = {
        "stat_date":       day.isoformat(),
        **stats,
        "created_at":      datetime.datetime.utcnow().isoformat()
    }

    supabase.table(TABLE).upsert(row, on_conflict="stat_date").execute()
    print("YouTube insert OK →", day)

if __name__ == "__main__":
    main()
