"""
Daily refresh: query Databricks SQL API → generate dashboard HTML.
Requires env var DATABRICKS_TOKEN (Personal Access Token).
"""
import os, sys, time, csv, io, json
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DATABRICKS_HOST = "https://nubank-e2-general.cloud.databricks.com"
WAREHOUSE_ID = "1b00e95ae40fe45b"
BUSINESS_AREA = "Global Credit Card Factory"

TOOL_COLORS = {
    'claude_code': '#9b59b6', 'cursor': '#3498db', 'databricks_assistant': '#2ecc71',
    'glean_ai': '#1abc9c', 'google_gemini_app': '#e67e22', 'google_workspace': '#95a5a6',
}
TOOL_LABELS = {
    'claude_code': 'Claude Code', 'cursor': 'Cursor', 'databricks_assistant': 'DB Assistant',
    'glean_ai': 'Glean', 'google_gemini_app': 'Gemini', 'google_workspace': 'GWS',
}
MAIN_TOOLS = list(TOOL_COLORS.keys())
CH_COLORS = {'Engineer': '#3498db', 'Business Analyst': '#1abc9c', 'Product': '#c084fc'}
LV_COLORS = ['#3498db', '#e74c3c', '#2ecc71', '#c084fc', '#f39c12', '#e91e63']


# ═══════════════════════════════════════════════════════════
# DATABRICKS SQL API
# ═══════════════════════════════════════════════════════════

def run_sql(query, token):
    """Execute SQL via Databricks Statement Execution API and return rows as list of dicts."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": query,
        "wait_timeout": "50s",
        "disposition": "INLINE",
    }

    resp = requests.post(f"{DATABRICKS_HOST}/api/2.0/sql/statements", headers=headers, json=payload)
    resp.raise_for_status()
    data = resp.json()

    if data["status"]["state"] == "PENDING":
        stmt_id = data["statement_id"]
        for _ in range(30):
            time.sleep(2)
            r = requests.get(f"{DATABRICKS_HOST}/api/2.0/sql/statements/{stmt_id}", headers=headers)
            r.raise_for_status()
            data = r.json()
            if data["status"]["state"] != "PENDING":
                break

    if data["status"]["state"] != "SUCCEEDED":
        print(f"Query failed: {json.dumps(data['status'], indent=2)}", file=sys.stderr)
        sys.exit(1)

    columns = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array", [])
    return [dict(zip(columns, row)) for row in rows]


def fetch_data(token):
    """Pull 60 days of data for rolling window calculations."""
    query = f"""
    SELECT * FROM usr.ai_nubank_raw_logs.consolidation_daily_v2
    WHERE date >= DATE_SUB(CURRENT_DATE(), 60)
      AND business_area_or_area = '{BUSINESS_AREA}'
    """
    print(f"Querying Databricks ({BUSINESS_AREA}, last 60 days)...")
    rows = run_sql(query, token)
    print(f"  → {len(rows)} rows returned")
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    df['total_requests'] = pd.to_numeric(df['total_requests'], errors='coerce').fillna(0).astype(int)
    return df


# ═══════════════════════════════════════════════════════════
# PLOTLY HELPERS
# ═══════════════════════════════════════════════════════════

_plotly_included = False

def plotly_html(fig, height=400):
    global _plotly_included
    inc = True if not _plotly_included else False
    _plotly_included = True
    return fig.to_html(full_html=False, include_plotlyjs=inc,
        config={'displayModeBar': True, 'modeBarButtonsToRemove': ['lasso2d','select2d']},
        default_height=f'{height}px')


def sortable_table(table_id, headers, rows, highlight_col=None):
    h = f'<table id="{table_id}" class="sortable"><thead><tr>'
    for i, c in enumerate(headers):
        h += f'<th onclick="sortTable(\'{table_id}\',{i})">{c} <span class="sort-arrow">⇅</span></th>'
    h += '</tr></thead><tbody>'
    for row in rows:
        h += '<tr>'
        for j, cell in enumerate(row):
            cls = ''
            if highlight_col is not None and j == highlight_col:
                try:
                    if float(str(cell).replace(',','').replace('—','0')) > 1000: cls = ' class="hl-orange"'
                except: pass
            h += f'<td{cls}>{cell}</td>'
        h += '</tr>'
    h += '</tbody></table>'
    return h


# ═══════════════════════════════════════════════════════════
# DASHBOARD GENERATION
# ═══════════════════════════════════════════════════════════

def generate_dashboard(df_full):
    all_dates_full = sorted(df_full['date'].unique())
    cutoff_30d = pd.Timestamp(all_dates_full[-1]) - pd.Timedelta(days=29)
    display_dates = sorted([d for d in all_dates_full if pd.Timestamp(d) >= cutoff_30d])
    df = df_full[df_full['date'] >= cutoff_30d].copy()

    date_min = df['date'].min().strftime('%b %d, %Y')
    date_max = df['date'].max().strftime('%b %d, %Y')
    total_members = df['ident__email'].nunique()

    # 1. Executive Summary
    cc_df = df[df['tool'] == 'claude_code']
    cc_users = cc_df[cc_df['total_requests'] > 0]['ident__email'].nunique()
    cc_adoption = cc_users / total_members * 100
    total_cc_req = int(cc_df['total_requests'].sum())
    total_all_req = int(df['total_requests'].sum())
    cc_pct = total_cc_req / total_all_req * 100 if total_all_req else 0
    cc_per_user = cc_df.groupby('ident__email')['total_requests'].sum()
    cc_act = cc_per_user[cc_per_user > 0]
    mean_req = cc_act.mean()
    median_req = cc_act.median()
    skew_val = mean_req / median_req if median_req else 0

    # 2. Daily Trend
    daily = df[df['tool'].isin(MAIN_TOOLS)].groupby(['date','tool'])['total_requests'].sum().unstack(fill_value=0)
    for t in MAIN_TOOLS:
        if t not in daily.columns: daily[t] = 0
    daily = daily[MAIN_TOOLS]

    fig_daily = go.Figure()
    for t in MAIN_TOOLS:
        fig_daily.add_trace(go.Bar(x=daily.index, y=daily[t], name=TOOL_LABELS[t], marker_color=TOOL_COLORS[t],
            hovertemplate='%{x|%b %d}: <b>%{y:,}</b> req<extra>' + TOOL_LABELS[t] + '</extra>'))
    fig_daily.update_layout(paper_bgcolor='#1a1a2e', plot_bgcolor='#16213e', font=dict(color='#e0e0e0'), barmode='stack', hovermode='x unified',
        legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center', font=dict(size=11)),
        yaxis_title='Requests', yaxis=dict(gridcolor='#333355'), xaxis=dict(tickformat='%m-%d', gridcolor='#333355'), margin=dict(l=50,r=30,t=60,b=50))
    daily_html = plotly_html(fig_daily, 420)

    # 3. Adoption Over Time
    cc_act_full = df_full[(df_full['tool']=='claude_code') & (df_full['total_requests']>0)]
    ever_used = set(); adopt_rows = []
    for d in all_dates_full:
        ever_used |= set(cc_act_full[cc_act_full['date']==d]['ident__email'])
        d7 = pd.Timestamp(d)-pd.Timedelta(days=6)
        weekly = set(cc_act_full[(cc_act_full['date']>=d7)&(cc_act_full['date']<=d)]['ident__email'])
        d30 = pd.Timestamp(d)-pd.Timedelta(days=29)
        monthly = set(cc_act_full[(cc_act_full['date']>=d30)&(cc_act_full['date']<=d)]['ident__email'])
        adopt_rows.append({'date':pd.Timestamp(d),'w7':len(weekly)/total_members*100,'m30':len(monthly)/total_members*100,'ever':len(ever_used)/total_members*100})
    adopt_disp = pd.DataFrame(adopt_rows)
    adopt_disp = adopt_disp[adopt_disp['date']>=cutoff_30d].reset_index(drop=True)

    fig_adopt = go.Figure()
    for col, color, name in [('w7','#f39c12','Weekly active (7d)'),('m30','#c084fc','Monthly active (30d)'),('ever','#2ecc71','Ever used (cumulative)')]:
        fig_adopt.add_trace(go.Scatter(x=adopt_disp['date'], y=adopt_disp[col], name=name, mode='lines',
            line=dict(color=color, width=2.5, shape='spline', smoothing=1.2),
            hovertemplate='%{x|%b %d}: <b>%{y:.1f}%</b><extra>'+name+'</extra>'))
    fig_adopt.update_layout(paper_bgcolor='#1a1a2e', plot_bgcolor='#16213e', font=dict(color='#e0e0e0'), hovermode='x unified',
        yaxis=dict(range=[0,105], ticksuffix='%', gridcolor='#333355'),
        legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center', font=dict(size=11)),
        xaxis=dict(tickformat='%Y-%m-%d', gridcolor='#333355'), margin=dict(l=50,r=30,t=60,b=50))
    adopt_html = plotly_html(fig_adopt, 400)

    # 4. Median CC Requests Over Time
    cc_full_data = df_full[df_full['tool']=='claude_code'].copy()
    med_rows = []
    for d in all_dates_full:
        d7 = pd.Timestamp(d)-pd.Timedelta(days=6)
        w7 = cc_full_data[(cc_full_data['date']>=d7)&(cc_full_data['date']<=d)].groupby('ident__email')['total_requests'].sum()
        w7a = w7[w7>0]
        d30 = pd.Timestamp(d)-pd.Timedelta(days=29)
        w30 = cc_full_data[(cc_full_data['date']>=d30)&(cc_full_data['date']<=d)].groupby('ident__email')['total_requests'].sum()
        w30a = w30[w30>0]
        med_rows.append({'date':pd.Timestamp(d),'m7':w7a.median() if len(w7a) else 0,'m30':w30a.median() if len(w30a) else 0})
    mdf_disp = pd.DataFrame(med_rows)
    mdf_disp = mdf_disp[mdf_disp['date']>=cutoff_30d].reset_index(drop=True)

    fig_med = go.Figure()
    for col, color, name in [('m7','#f39c12','Median req (7d)'),('m30','#c084fc','Median req (30d)')]:
        fig_med.add_trace(go.Scatter(x=mdf_disp['date'], y=mdf_disp[col], name=name, mode='lines',
            line=dict(color=color, width=2.5, shape='spline', smoothing=1.2),
            hovertemplate='%{x|%b %d}: <b>%{y:.0f}</b> req<extra>'+name+'</extra>'))
    fig_med.update_layout(paper_bgcolor='#1a1a2e', plot_bgcolor='#16213e', font=dict(color='#e0e0e0'), hovermode='x unified',
        yaxis=dict(title='Median req', gridcolor='#333355'),
        legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center', font=dict(size=11)),
        xaxis=dict(tickformat='%Y-%m-%d', gridcolor='#333355'), margin=dict(l=50,r=30,t=60,b=50))
    median_html = plotly_html(fig_med, 400)

    # 5. By Chapter
    chapters = sorted(df['job_family'].dropna().unique())
    ch_table_rows = []
    for ch in chapters:
        c = df[df['job_family']==ch]; m = c['ident__email'].nunique()
        ccc = c[c['tool']=='claude_code']; cu = ccc[ccc['total_requests']>0]['ident__email'].nunique()
        cr = int(ccc['total_requests'].sum()); avg = cr/cu if cu else 0
        per_u = ccc.groupby('ident__email')['total_requests'].sum(); act = per_u[per_u>0]
        med = act.median() if len(act) else 0
        top_all = c.groupby('ident__email')['total_requests'].sum()
        te = top_all.idxmax() if len(top_all) else ''
        tn = c[c['ident__email']==te]['ident__name'].iloc[0] if te else ''
        tp = top_all.max()/top_all.sum()*100 if top_all.sum() else 0
        ch_table_rows.append({'ch':ch,'members':m,'cc_users':cu,'adopt':f'{cu/m*100:.1f}%','cc_req':cr,'avg':f'{avg:,.0f}',
            'cursor':int(c[c['tool']=='cursor']['total_requests'].sum()),'db':int(c[c['tool']=='databricks_assistant']['total_requests'].sum()),
            'glean':int(c[c['tool']=='glean_ai']['total_requests'].sum()),'gemini':int(c[c['tool']=='google_gemini_app']['total_requests'].sum()),
            'gws':int(c[c['tool']=='google_workspace']['total_requests'].sum()),'median':f'{med:,.0f}','top':f'{tn} = {tp:.0f}% of group'})
    ch_table_rows.sort(key=lambda x: -x['cc_req'])

    fig_ch = make_subplots(rows=1, cols=2, subplot_titles=('7-day rolling Claude Code adoption — by chapter','7-day rolling median Claude Code requests — by chapter'), horizontal_spacing=0.08)
    for ch in chapters:
        ch_cc = df_full[(df_full['job_family']==ch)&(df_full['tool']=='claude_code')&(df_full['total_requests']>0)]
        cm = df[df['job_family']==ch]['ident__email'].nunique()
        ad, ms = [], []
        for d in display_dates:
            d7 = pd.Timestamp(d)-pd.Timedelta(days=6)
            w = ch_cc[(ch_cc['date']>=d7)&(ch_cc['date']<=d)]
            ad.append(w['ident__email'].nunique()/cm*100 if cm else 0)
            ur = w.groupby('ident__email')['total_requests'].sum(); ar = ur[ur>0]
            ms.append(ar.median() if len(ar) else 0)
        col = CH_COLORS.get(ch,'#888'); lb = f'{ch} ({cm})'
        fig_ch.add_trace(go.Scatter(x=display_dates, y=ad, name=lb, legendgroup=ch, line=dict(color=col, width=2.5, shape='spline', smoothing=1.2), hovertemplate='%{x|%b %d}: <b>%{y:.1f}%</b><extra>'+lb+'</extra>'), row=1, col=1)
        fig_ch.add_trace(go.Scatter(x=display_dates, y=ms, name=lb, legendgroup=ch, showlegend=False, line=dict(color=col, width=2.5, shape='spline', smoothing=1.2), hovertemplate='%{x|%b %d}: <b>%{y:.0f}</b> req<extra>'+lb+'</extra>'), row=1, col=2)
    fig_ch.update_layout(paper_bgcolor='#1a1a2e', plot_bgcolor='#16213e', font=dict(color='#e0e0e0'), hovermode='x unified',
        legend=dict(orientation='h', y=1.18, x=0.5, xanchor='center', font=dict(size=10)), margin=dict(l=50,r=30,t=80,b=50))
    fig_ch.update_yaxes(range=[0,105], ticksuffix='%', gridcolor='#333355', row=1, col=1)
    fig_ch.update_yaxes(gridcolor='#333355', row=1, col=2)
    fig_ch.update_xaxes(tickformat='%m-%d', gridcolor='#333355')
    for ann in fig_ch['layout']['annotations']: ann['font'] = dict(size=12, color='#e0e0e0')
    ch_chart_html = plotly_html(fig_ch, 420)

    # 6. By Level
    levels = df['level_code'].dropna().unique()
    lv_table_rows = []
    for lv in levels:
        l = df[df['level_code']==lv]; m = l['ident__email'].nunique()
        lcc = l[l['tool']=='claude_code']; cu = lcc[lcc['total_requests']>0]['ident__email'].nunique()
        cr = int(lcc['total_requests'].sum()); avg = cr/cu if cu else 0
        per_u = lcc.groupby('ident__email')['total_requests'].sum(); act = per_u[per_u>0]
        med = act.median() if len(act) else 0
        top_all = l.groupby('ident__email')['total_requests'].sum()
        te = top_all.idxmax() if len(top_all) else ''
        tn = l[l['ident__email']==te]['ident__name'].iloc[0] if te else ''
        tp = top_all.max()/top_all.sum()*100 if top_all.sum() else 0
        lv_table_rows.append({'level':lv,'members':m,'cc_users':cu,'adopt':f'{cu/m*100:.1f}%','cc_req':cr,'avg':f'{avg:,.0f}',
            'cursor':int(l[l['tool']=='cursor']['total_requests'].sum()),'db':int(l[l['tool']=='databricks_assistant']['total_requests'].sum()),
            'glean':int(l[l['tool']=='glean_ai']['total_requests'].sum()),'gemini':int(l[l['tool']=='google_gemini_app']['total_requests'].sum()),
            'gws':int(l[l['tool']=='google_workspace']['total_requests'].sum()),'median':f'{med:,.0f}','top':f'{tn} = {tp:.0f}% of group'})
    lv_table_rows.sort(key=lambda x: -x['cc_req'])
    top_levels = [r['level'] for r in lv_table_rows[:6]]

    fig_lv = make_subplots(rows=1, cols=2, subplot_titles=(f'7-day rolling Claude Code adoption — top {len(top_levels)} levels', f'7-day rolling median Claude Code requests — top {len(top_levels)} levels'), horizontal_spacing=0.08)
    for i, lv in enumerate(top_levels):
        lcc = df_full[(df_full['level_code']==lv)&(df_full['tool']=='claude_code')&(df_full['total_requests']>0)]
        lm = df[df['level_code']==lv]['ident__email'].nunique()
        ad, ms = [], []
        for d in display_dates:
            d7 = pd.Timestamp(d)-pd.Timedelta(days=6)
            w = lcc[(lcc['date']>=d7)&(lcc['date']<=d)]
            ad.append(w['ident__email'].nunique()/lm*100 if lm else 0)
            ur = w.groupby('ident__email')['total_requests'].sum(); ar = ur[ur>0]
            ms.append(ar.median() if len(ar) else 0)
        col_c = LV_COLORS[i%len(LV_COLORS)]; lb = f'{lv} ({lm})'
        fig_lv.add_trace(go.Scatter(x=display_dates, y=ad, name=lb, legendgroup=lv, line=dict(color=col_c, width=2.5, shape='spline', smoothing=1.2), hovertemplate='%{x|%b %d}: <b>%{y:.1f}%</b><extra>'+lb+'</extra>'), row=1, col=1)
        fig_lv.add_trace(go.Scatter(x=display_dates, y=ms, name=lb, legendgroup=lv, showlegend=False, line=dict(color=col_c, width=2.5, shape='spline', smoothing=1.2), hovertemplate='%{x|%b %d}: <b>%{y:.0f}</b> req<extra>'+lb+'</extra>'), row=1, col=2)
    fig_lv.update_layout(paper_bgcolor='#1a1a2e', plot_bgcolor='#16213e', font=dict(color='#e0e0e0'), hovermode='x unified',
        legend=dict(orientation='h', y=1.22, x=0.5, xanchor='center', font=dict(size=10)), margin=dict(l=50,r=30,t=90,b=50))
    fig_lv.update_yaxes(range=[0,105], ticksuffix='%', gridcolor='#333355', row=1, col=1)
    fig_lv.update_yaxes(gridcolor='#333355', row=1, col=2)
    fig_lv.update_xaxes(tickformat='%m-%d', gridcolor='#333355')
    for ann in fig_lv['layout']['annotations']: ann['font'] = dict(size=12, color='#e0e0e0')
    lv_chart_html = plotly_html(fig_lv, 440)

    # 7. All Members
    member_rows = []
    for email in df['ident__email'].unique():
        u = df[df['ident__email']==email]
        row = {'name':u['ident__name'].iloc[0],'level':u['level_code'].iloc[0],'chapter':u['job_family'].iloc[0]}
        total = 0
        for t in MAIN_TOOLS:
            v = int(u[u['tool']==t]['total_requests'].sum()); row[t] = v; total += v
        row['total'] = total; member_rows.append(row)
    member_rows.sort(key=lambda x: -x['total'])

    # 8. Intensity Buckets
    cc_user_total = df[df['tool']=='claude_code'].groupby('ident__email')['total_requests'].sum()
    for u in set(df['ident__email'].unique()) - set(cc_user_total.index):
        cc_user_total.loc[u] = 0
    n_days = len(display_dates)

    def bucket(req):
        daily = req / n_days
        if req == 0: return 'Inactive'
        if daily < 29: return 'Light'
        if daily < 149: return 'Moderate'
        return 'Engaged'

    bkt = pd.DataFrame({'email':cc_user_total.index,'cc_req':cc_user_total.values})
    bkt['bucket'] = bkt['cc_req'].apply(bucket)
    bucket_order = ['Inactive','Light','Moderate','Engaged']
    bucket_colors_map = {'Inactive':'#555','Light':'#3498db','Moderate':'#f39c12','Engaged':'#2ecc71'}
    bucket_ranges = {'Inactive':'0','Light':'< 29 req/day','Moderate':'29–148 req/day','Engaged':'149+ req/day'}
    bucket_summary = []
    for b in bucket_order:
        bd = bkt[bkt['bucket']==b]; bm = len(bd); bcr = int(bd['cc_req'].sum())
        bmd = bd[bd['cc_req']>0]['cc_req'].median() if len(bd[bd['cc_req']>0]) else 0
        bucket_summary.append({'bucket':b,'range':bucket_ranges[b],'members':bm,'pct':f'{bm/total_members*100:.1f}%','cc_req':bcr,
            'pct_total':f'{bcr/total_cc_req*100:.1f}%' if total_cc_req else '0%','median':f'{bmd:,.0f}' if bmd else '—'})

    fig_bkt = go.Figure()
    left = 0
    for bs in bucket_summary:
        w = bs['members']
        fig_bkt.add_trace(go.Bar(x=[w], y=[''], orientation='h', name=f"{bs['bucket']} ({w})",
            marker_color=bucket_colors_map[bs['bucket']], text=f"{bs['bucket']} ({w})", textposition='inside',
            textfont=dict(color='white', size=12), base=left,
            hovertemplate=f"<b>{bs['bucket']}</b><br>{w} members ({bs['pct']})<br>{bs['cc_req']:,} CC req<extra></extra>"))
        left += w
    fig_bkt.update_layout(paper_bgcolor='#1a1a2e', plot_bgcolor='#16213e', font=dict(color='#e0e0e0'),
        showlegend=False, barmode='stack', height=80, margin=dict(l=10,r=10,t=30,b=10),
        title=dict(text=f'Distribution of {total_members} members by Claude Code request volume (30 days)', font=dict(size=12)),
        xaxis=dict(showticklabels=False, showgrid=False), yaxis=dict(showticklabels=False, showgrid=False))
    bucket_bar_html = plotly_html(fig_bkt, 90)

    # 9. Where to Act
    top5 = member_rows[:5]
    top5_cc = sum(r['claude_code'] for r in top5)
    top5_pct = top5_cc / total_cc_req * 100 if total_cc_req else 0
    top1 = top5[0]
    zero_cc_emails = set(bkt[bkt['cc_req']==0]['email'])
    cursor_no_cc = []
    for email in zero_cc_emails:
        cr = df[(df['ident__email']==email)&(df['tool']=='cursor')]['total_requests'].sum()
        if cr > 0:
            name = df[df['ident__email']==email]['ident__name'].iloc[0]
            cursor_no_cc.append((name, int(cr)))
    cursor_no_cc.sort(key=lambda x: -x[1])
    conversion_str = ', '.join(f'{n} ({r} Cursor)' for n, r in cursor_no_cc[:5])
    lowest_adopt = min(lv_table_rows, key=lambda x: float(x['adopt'].rstrip('%')))

    # Table data
    ch_h = ['#','Chapter','Members','CC Users','Adoption','CC Req','Avg/CC User','Cursor','DB Asst','Glean','Gemini','GWS','Median','Top User']
    ch_r = [[i+1,r['ch'],r['members'],r['cc_users'],r['adopt'],f"{r['cc_req']:,}",r['avg'],f"{r['cursor']:,}",f"{r['db']:,}",f"{r['glean']:,}",f"{r['gemini']:,}",f"{r['gws']:,}",r['median'],r['top']] for i,r in enumerate(ch_table_rows)]
    lv_h = ['#','Level','Members','CC Users','Adoption','CC Req','Avg/CC User','Cursor','DB Asst','Glean','Gemini','GWS','Median','Top User']
    lv_r = [[i+1,r['level'],r['members'],r['cc_users'],r['adopt'],f"{r['cc_req']:,}",r['avg'],f"{r['cursor']:,}",f"{r['db']:,}",f"{r['glean']:,}",f"{r['gemini']:,}",f"{r['gws']:,}",r['median'],r['top']] for i,r in enumerate(lv_table_rows)]
    mb_h = ['#','Name','Level','Chapter','Claude Code','Cursor','DB Asst','Glean','Gemini','GWS','Total']
    mb_r = [[i+1,r['name'],r['level'],r['chapter'],f"{r['claude_code']:,}" if r['claude_code'] else '—',f"{r['cursor']:,}" if r['cursor'] else '—',f"{r['databricks_assistant']:,}" if r['databricks_assistant'] else '—',f"{r['glean_ai']:,}" if r['glean_ai'] else '—',f"{r['google_gemini_app']:,}" if r['google_gemini_app'] else '—',f"{r['google_workspace']:,}" if r['google_workspace'] else '—',f"{r['total']:,}"] for i,r in enumerate(member_rows)]
    bk_h = ['#','Bucket','Range','Members','% Team','CC Req','% Total','Median']
    bk_r = [[i+1,b['bucket'],b['range'],b['members'],b['pct'],f"{b['cc_req']:,}",b['pct_total'],b['median']] for i,b in enumerate(bucket_summary)]

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Global Credit Card Factory — AI Usage Deep Dive</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0f0f1a;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:32px 48px;line-height:1.5}}
h1{{font-size:26px;font-weight:700;margin-bottom:4px}}
.sub{{color:#aaa;font-size:13px;margin-bottom:28px}}.sub span{{display:inline-flex;align-items:center;margin-right:18px}}
.badge{{background:#5b3a8c;color:#c084fc;padding:2px 10px;border-radius:4px;font-size:12px;font-weight:600}}
.badge-g{{background:#1b3a2d;color:#4ade80}}
.sec{{margin:36px 0 0}}.sec-t{{font-size:18px;font-weight:700;color:#c084fc;margin-bottom:16px;border-bottom:1px solid #2a2a4a;padding-bottom:8px}}
.toc{{background:#16162a;border:1px solid #2a2a4a;border-radius:10px;padding:20px 28px;margin-bottom:32px}}
.toc-t{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#888;margin-bottom:10px}}
.toc ol{{padding-left:20px;columns:2}}.toc li{{font-size:13px;color:#c084fc;margin-bottom:4px}}
.kpi-row{{display:flex;gap:14px;margin:14px 0;flex-wrap:wrap}}
.kpi{{padding:18px 22px;border-radius:10px;flex:1;min-width:150px}}
.kpi .v{{font-size:30px;font-weight:800}}.kpi .l{{font-size:11px;opacity:.75;margin-top:4px;line-height:1.3}}
.kp{{background:#1e1636;border:1px solid #3d2a6e}}.kp .v{{color:#c084fc}}
.kg{{background:#0f2618;border:1px solid #1e5c35}}.kg .v{{color:#4ade80}}
.ky{{background:#261f0f;border:1px solid #5c4a1e}}.ky .v{{color:#facc15}}
.ko{{background:#26190f;border:1px solid #5c3a1e}}.ko .v{{color:#fb923c}}
.kr{{background:#260f0f;border:1px solid #5c1e1e}}.kr .v{{color:#f87171}}
.ins{{padding:14px 20px;margin:12px 0;border-radius:6px;font-size:13px;line-height:1.6}}
.ins-p{{background:#1a1530;border-left:4px solid #c084fc}}
.ins-o{{background:#261a10;border-left:4px solid #f39c12}}
.ins-g{{background:#0f2618;border-left:4px solid #2ecc71}}
.ins-b{{background:#0f1826;border-left:4px solid #3498db}}
.ins-y{{background:#26220f;border-left:4px solid #facc15}}
.ins-r{{background:#260f0f;border-left:4px solid #f87171}}
table.sortable{{width:100%;border-collapse:collapse;font-size:12px;margin:12px 0}}
table.sortable thead{{background:#16162a}}
table.sortable th{{padding:10px 12px;text-align:left;font-weight:600;color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid #2a2a4a;white-space:nowrap;cursor:pointer;user-select:none}}
table.sortable th:hover{{color:#c084fc}}
.sort-arrow{{font-size:10px;opacity:.4;margin-left:2px;transition:opacity .2s}}
th[data-sort="asc"] .sort-arrow,th[data-sort="desc"] .sort-arrow{{opacity:1;color:#c084fc}}
table.sortable td{{padding:9px 12px;border-bottom:1px solid #1e1e3a}}
table.sortable tr:hover{{background:#1a1a30}}
.hl-orange{{color:#f39c12!important;font-weight:700}}
.chart-wrap{{margin:16px 0}}
.footer{{margin-top:40px;padding-top:16px;border-top:1px solid #2a2a4a;font-size:11px;color:#666}}
</style>
</head><body>
<h1>Global Credit Card Factory — AI Usage Deep Dive</h1>
<div class="sub">
  <span>All {total_members} members · {date_min} – {date_max} (30 days)</span>
  <span class="badge">📅 {date_min} – {date_max}</span>
  <span class="badge badge-g">👥 {total_members} tracked members</span>
</div>
<div class="toc"><div class="toc-t">Contents</div>
<ol><li>Executive Summary</li><li>Daily Trend</li><li>Claude Code Adoption Over Time</li><li>Median Claude Code Requests Over Time</li><li>By Chapter</li><li>By Level</li><li>All Members</li><li>Intensity Buckets</li><li>Where to Act</li></ol></div>

<div class="sec"><div class="sec-t">1 · Executive Summary</div>
<div class="kpi-row">
  <div class="kpi kp"><div class="v">{mean_req:.0f}</div><div class="l">Mean req / Claude Code user</div></div>
  <div class="kpi kp"><div class="v">{median_req:.0f}</div><div class="l">Median req / Claude Code user</div></div>
  <div class="kpi ky"><div class="v">{skew_val:.1f}x</div><div class="l">Skew (mean ÷ median)</div></div>
</div>
<div class="ins ins-p">Mean is <b style="color:#facc15">{skew_val:.1f}x above median</b> — a small cohort of power users inflates the average. Median ({median_req:.0f} req) better represents typical Claude Code usage.</div>
<div class="kpi-row">
  <div class="kpi kg"><div class="v">{total_members}</div><div class="l">Total members</div></div>
  <div class="kpi kg"><div class="v">{cc_users}</div><div class="l">Claude Code users<br>{cc_adoption:.1f}% adoption</div></div>
  <div class="kpi kp"><div class="v">{total_cc_req:,}</div><div class="l">Total Claude Code req<br>{cc_pct:.1f}% of all AI req</div></div>
  <div class="kpi ko"><div class="v">{mean_req:.0f}</div><div class="l">Mean req / Claude Code user</div></div>
  <div class="kpi kr"><div class="v">{median_req:.0f}</div><div class="l">Median req / Claude Code user</div></div>
</div></div>

<div class="sec"><div class="sec-t">2 · Daily Trend</div><div class="chart-wrap">{daily_html}</div></div>
<div class="sec"><div class="sec-t">3 · Claude Code Adoption Over Time</div><div class="chart-wrap">{adopt_html}</div></div>
<div class="sec"><div class="sec-t">4 · Median Claude Code Requests Over Time</div><div class="chart-wrap">{median_html}</div></div>
<div class="sec"><div class="sec-t">5 · By Chapter</div>{sortable_table('tbl-ch', ch_h, ch_r)}<div class="chart-wrap">{ch_chart_html}</div></div>
<div class="sec"><div class="sec-t">6 · By Level</div>{sortable_table('tbl-lv', lv_h, lv_r)}<div class="chart-wrap">{lv_chart_html}</div></div>
<div class="sec"><div class="sec-t">7 · All Members</div>{sortable_table('tbl-mb', mb_h, mb_r, highlight_col=4)}</div>
<div class="sec"><div class="sec-t">8 · Intensity Buckets</div><div class="chart-wrap">{bucket_bar_html}</div>{sortable_table('tbl-bk', bk_h, bk_r)}</div>

<div class="sec"><div class="sec-t">9 · Where to Act</div>
<div class="ins ins-g"><b style="color:#2ecc71">Top 5 users account for {top5_pct:.0f}% of all Claude Code requests.</b> {top1['name']} leads with {top1['claude_code']:,} req. Broadening the base beyond power users is key to sustainable adoption.</div>
<div class="ins ins-y"><b style="color:#facc15">{len(zero_cc_emails)} members ({len(zero_cc_emails)/total_members*100:.0f}%) have zero Claude Code usage.</b> Top Cursor users with zero Claude Code: {conversion_str}. These are the warmest conversion targets.</div>
<div class="ins ins-r"><b style="color:#f87171">{lowest_adopt['level']} has lowest adoption ({lowest_adopt['adopt']}).</b> Running targeted demos for this group is the highest-leverage expansion play.</div>
</div>

<div class="footer">Report generated {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M UTC')} · Data: usr.ai_nubank_raw_logs.consolidation_daily_v2 · Period: {date_min} – {date_max} (30 days) · Business Area: Global Credit Card Factory</div>

<script>
function sortTable(id, col) {{
  const table = document.getElementById(id);
  const tbody = table.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  const ths = table.querySelectorAll('th');
  const th = ths[col];
  const asc = th.dataset.sort !== 'asc';
  ths.forEach(h => {{ h.dataset.sort = ''; h.querySelector('.sort-arrow').textContent = '⇅'; }});
  th.dataset.sort = asc ? 'asc' : 'desc';
  th.querySelector('.sort-arrow').textContent = asc ? '↑' : '↓';
  rows.sort((a, b) => {{
    let va = a.cells[col].textContent.trim();
    let vb = b.cells[col].textContent.trim();
    const cleanA = va.replace(/[,%—]/g, '').replace('%','');
    const cleanB = vb.replace(/[,%—]/g, '').replace('%','');
    const na = parseFloat(cleanA);
    const nb = parseFloat(cleanB);
    const bothNum = !isNaN(na) && !isNaN(nb) && va !== '—' && vb !== '—';
    if (bothNum) return asc ? na - nb : nb - na;
    if (va === '—' && vb !== '—') return asc ? 1 : -1;
    if (va !== '—' && vb === '—') return asc ? -1 : 1;
    return asc ? va.localeCompare(vb) : vb.localeCompare(va);
  }});
  rows.forEach((r, i) => {{ r.cells[0].textContent = i + 1; tbody.appendChild(r); }});
}}
</script>
</body></html>"""

    return html


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        print("ERROR: Set DATABRICKS_TOKEN env var", file=sys.stderr)
        sys.exit(1)

    df_full = fetch_data(token)
    html = generate_dashboard(df_full)

    with open("index.html", "w") as f:
        f.write(html)

    print(f"Dashboard written to index.html ({len(html)/1024:.0f} KB)")
