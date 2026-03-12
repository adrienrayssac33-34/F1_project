"""
F1 Data Hub — Dashboard Streamlit
Pages : Classements · Stats par GP · Pit Stops · Météo · Comparaison historique · 🤖 Prédiction IA
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import cfg

# ── CONFIGURATION PAGE ────────────────────────────────────────
st.set_page_config(
    page_title="🏎️ F1 Data Hub",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .stApp { background: linear-gradient(135deg,#0a0a0a 0%,#1a1a2e 100%); color:#fff; }
  h1 { color:#e10600 !important; }
  h2,h3 { color:#fff !important; }
  .stTabs [data-baseweb="tab"] { color:#aaa; }
  .stTabs [aria-selected="true"] { color:#e10600 !important; border-bottom:2px solid #e10600; }
  div[data-testid="metric-container"] {
      background:rgba(255,255,255,0.05);
      border:1px solid #333;
      border-radius:8px;
      padding:10px;
  }
</style>
""", unsafe_allow_html=True)

DARK = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(20,20,30,0.8)",
)
TEAM_COLORS = {
    "Red Bull":"#1E3A5F","red_bull":"#1E3A5F",
    "McLaren":"#FF8000","mclaren":"#FF8000",
    "Ferrari":"#DC0000","ferrari":"#DC0000",
    "Mercedes":"#00D2BE","mercedes":"#00D2BE",
    "Aston Martin":"#006F62","aston_martin":"#006F62",
    "Alpine":"#0090FF","alpine":"#0090FF",
    "Williams":"#005AFF","williams":"#005AFF",
    "RB":"#1E3A8A","rb":"#1E3A8A",
    "Haas":"#CCCCCC","haas":"#CCCCCC",
    "Kick Sauber":"#52E252","kick_sauber":"#52E252",
    "Sauber":"#52E252",
}


# ── BASE DE DONNÉES ───────────────────────────────────────────

@st.cache_resource
def get_conn():
    db_path = os.path.join(cfg.DB_DIR, "f1_hub.db")
    if os.path.exists(db_path):
        return sqlite3.connect(db_path, check_same_thread=False)
    return None


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"SQL : {e}")
        return pd.DataFrame()


def db_ok() -> bool:
    conn = get_conn()
    if conn is None:
        return False
    try:
        conn.execute("SELECT 1 FROM race_results LIMIT 1")
        return True
    except:
        return False


def available_seasons() -> list:
    df = q("SELECT DISTINCT season FROM race_results ORDER BY season")
    return df["season"].tolist() if not df.empty else [2024]


# ── DONNÉES DÉMO ─────────────────────────────────────────────

def demo_standings():
    return pd.DataFrame({
        "position": range(1,11),
        "driver_surname":["Verstappen","Norris","Leclerc","Piastri","Sainz",
                          "Russell","Hamilton","Alonso","Stroll","Perez"],
        "constructor":   ["Red Bull","McLaren","Ferrari","McLaren","Ferrari",
                          "Mercedes","Mercedes","Aston Martin","Aston Martin","Red Bull"],
        "points":        [437,374,356,292,290,235,174,162,49,152],
        "wins":          [9,4,3,2,2,1,2,0,0,2],
        "season":        [2024]*10,
    })


def demo_results():
    import random; random.seed(42)
    gps = ["Bahrain","Saudi Arabia","Australia","Japan","Monaco",
           "Britain","Hungary","Italy","Singapore","Abu Dhabi"]
    rows=[]
    for i,gp in enumerate(gps,1):
        for j,(drv,team,pts) in enumerate(zip(
            ["Verstappen","Norris","Leclerc","Piastri","Sainz"],
            ["Red Bull","McLaren","Ferrari","McLaren","Ferrari"],
            [25,18,15,12,10]
        ),1):
            rows.append({"round":i,"gp_name":f"{gp} Grand Prix","country":gp,
                         "driver_surname":drv,"constructor_name":team,
                         "position_num":j,"points":pts,"grid":random.randint(1,10),
                         "season":2024,"laps":50+random.randint(-5,10)})
    return pd.DataFrame(rows)


# ── HEADER ───────────────────────────────────────────────────

st.markdown("""
<h1 style='text-align:center;font-size:2.8em;margin-bottom:0'>🏎️ F1 DATA HUB</h1>
<p style='text-align:center;color:#888;margin-top:0'>
Analyse multi-saisons · OpenF1 · jolpi.ca · Wikipedia · 🤖 IA
</p>
<hr style='border-color:#e10600;margin-bottom:20px'>
""", unsafe_allow_html=True)

# ── SIDEBAR ───────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Navigation")
    page = st.radio("", [
        "🏆 Classements",
        "📊 Stats par GP",
        "📈 Comparaison historique",
        "🌦️ Météo & Circuits",
        "👤 Profil Pilote",
        "🤖 Prédiction IA",
    ], label_visibility="collapsed")

    st.markdown("---")
    seasons = available_seasons()
    if len(seasons) > 1:
        selected_season = st.selectbox("📅 Saison", seasons, index=len(seasons)-1)
    else:
        selected_season = seasons[0] if seasons else 2024
        st.info(f"Saison : {selected_season}")

    st.markdown("---")
    st.markdown("### 📡 Sources")
    st.markdown("- **OpenF1** — Temps réel")
    st.markdown("- **jolpi.ca** — Historique")
    st.markdown("- **Wikipedia** — Enrichissement")
    st.markdown("---")
    if db_ok():
        st.success(f"✅ BDD connectée · {len(seasons)} saison(s)")
        st.caption(f"Mode : {cfg.DB_MODE.upper()}")
    else:
        st.warning("⚠️ Mode démo")
        st.caption("Lancez `bash run_pipeline.sh`")


# ════════════════════════════════════════════════════════════
# PAGE : CLASSEMENTS
# ════════════════════════════════════════════════════════════

if page == "🏆 Classements":
    st.markdown(f"## 🏆 Classements — Saison {selected_season}")
    tab1, tab2 = st.tabs(["👨‍🏎️ Pilotes", "🏭 Constructeurs"])

    with tab1:
        if db_ok():
            df = q(f"SELECT * FROM driver_standings WHERE season={selected_season} ORDER BY position")
        else:
            df = demo_standings()

        if not df.empty:
            sc = "driver_surname" if "driver_surname" in df.columns else df.columns[4]
            pc = "points"
            tc = "constructor"

            c1, c2 = st.columns([2, 1])
            with c1:
                fig = px.bar(df.head(20), x=sc, y=pc,
                             color=tc, text=pc,
                             color_discrete_map=TEAM_COLORS,
                             title=f"Points pilotes {selected_season}",
                             **DARK)
                fig.update_layout(xaxis_title="", yaxis_title="Points",
                                  title_font_color="#e10600", showlegend=True)
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.markdown("### 📋 Top 10")
                cols = [c for c in ["position",sc,pc,"wins",tc] if c in df.columns]
                st.dataframe(df[cols].head(10), hide_index=True,
                             use_container_width=True)

            m1,m2,m3,m4 = st.columns(4)
            top = df.iloc[0]
            m1.metric("🥇 Champion", top.get(sc,"—"))
            m2.metric("🏆 Points", f"{top.get(pc,0):.0f}")
            m3.metric("🎯 Victoires", top.get("wins","—"))
            m4.metric("🏁 GPs", df["season"].count() if "season" in df.columns else "—")

    with tab2:
        if db_ok():
            df_c = q(f"SELECT * FROM constructor_standings WHERE season={selected_season} ORDER BY position")
        else:
            df_c = pd.DataFrame({
                "position":[1,2,3,4,5],
                "constructor_name":["McLaren","Ferrari","Red Bull","Mercedes","Aston Martin"],
                "points":[666,652,589,409,211],"wins":[6,5,9,4,0],
            })

        if not df_c.empty:
            nc = "constructor_name" if "constructor_name" in df_c.columns else df_c.columns[1]
            c1, c2 = st.columns([1.5,1])
            with c1:
                fig_p = px.pie(df_c, names=nc, values="points",
                               title=f"Répartition points constructeurs {selected_season}",
                               hole=0.4,
                               color=nc, color_discrete_map=TEAM_COLORS,
                               **DARK)
                fig_p.update_layout(title_font_color="#e10600")
                st.plotly_chart(fig_p, use_container_width=True)
            with c2:
                st.markdown("### 📋 Classement")
                st.dataframe(df_c, hide_index=True, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE : STATS PAR GP
# ════════════════════════════════════════════════════════════

elif page == "📊 Stats par GP":
    st.markdown(f"## 📊 Résultats par Grand Prix — {selected_season}")

    if db_ok():
        df_r = q(f"""
            SELECT round, gp_name, country,
                   driver_forename||' '||driver_surname AS driver,
                   constructor_name AS team,
                   position_num, points, grid, laps, status
            FROM race_results WHERE season={selected_season}
            ORDER BY round, position_num
        """)
    else:
        df_r = demo_results()
        df_r["driver"] = df_r["driver_surname"]
        df_r["team"]   = df_r["constructor_name"]

    if not df_r.empty:
        gps = sorted(df_r["gp_name"].dropna().unique())
        sel = st.selectbox("🏁 Grand Prix", gps, index=len(gps)-1)
        gp  = df_r[df_r["gp_name"] == sel].copy()

        if not gp.empty:
            dc = "driver" if "driver" in gp.columns else "driver_surname"
            tc = "team"   if "team"   in gp.columns else "constructor_name"

            win = gp[gp["position_num"]==1]
            if not win.empty:
                w = win.iloc[0]
                c1,c2,c3 = st.columns(3)
                c1.metric("🥇 Vainqueur", w.get(dc,"—"), w.get(tc,""))
                c2.metric("🏁 Tours",     w.get("laps","—"))
                c3.metric("🏎️ Grille",   f"P{int(w.get('grid',0))}" if w.get("grid") else "—")

            st.markdown("---")
            c1,c2 = st.columns([2,1])
            with c1:
                fig = px.bar(gp.head(10), x=dc, y="points",
                             color=tc, text="points",
                             color_discrete_map=TEAM_COLORS,
                             title=f"Points — {sel}", **DARK)
                fig.update_layout(xaxis_title="", yaxis_title="Points",
                                  title_font_color="#e10600")
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                cols = [c for c in [dc,tc,"position_num","points","grid","status"]
                        if c in gp.columns]
                st.dataframe(gp[cols].head(20), hide_index=True,
                             use_container_width=True)

        # Évolution cumulée des points
        st.markdown("---")
        st.markdown("### 📈 Évolution des points cumulés — Top 5")
        dc2 = "driver" if "driver" in df_r.columns else "driver_surname"
        top5 = df_r.groupby(dc2)["points"].sum().nlargest(5).index.tolist()
        df5  = df_r[df_r[dc2].isin(top5)].sort_values("round")
        df5["cumpoints"] = df5.groupby(dc2)["points"].cumsum()
        fig2 = px.line(df5, x="round", y="cumpoints", color=dc2,
                       title="Points cumulés au fil des GPs",
                       markers=True, **DARK)
        fig2.update_layout(xaxis_title="Round", yaxis_title="Points cumulés",
                           title_font_color="#e10600")
        st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE : COMPARAISON HISTORIQUE (MULTI-SAISONS)
# ════════════════════════════════════════════════════════════

elif page == "📈 Comparaison historique":
    st.markdown("## 📈 Comparaison historique multi-saisons")

    seasons_avail = available_seasons()
    if not db_ok() or len(seasons_avail) < 2:
        st.info("Cette page nécessite au moins 2 saisons en base de données.\n\n"
                "Dans `.env`, configurez `SEASONS=2022,2023,2024` puis relancez le pipeline.")
        st.stop()

    tab1, tab2, tab3 = st.tabs(["👑 Champions", "🏭 Constructeurs", "👨‍🏎️ Pilote"])

    with tab1:
        st.markdown("### Champions du monde par saison")
        df_champ = q("""
            SELECT season, driver_forename||' '||driver_surname AS champion,
                   constructor AS team, points, wins
            FROM driver_standings WHERE position=1
            ORDER BY season
        """)
        if not df_champ.empty:
            fig = px.bar(df_champ, x="season", y="points",
                         color="team", text="champion",
                         color_discrete_map=TEAM_COLORS,
                         title="Points du champion par saison",
                         **DARK)
            fig.update_layout(title_font_color="#e10600",
                               xaxis=dict(tickmode="array",
                                          tickvals=df_champ["season"].tolist()))
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_champ, hide_index=True, use_container_width=True)

    with tab2:
        st.markdown("### Évolution des constructeurs")
        df_cons = q("""
            SELECT season, constructor_name, points, position, wins
            FROM constructor_standings ORDER BY season, position
        """)
        if not df_cons.empty:
            top_teams = df_cons.groupby("constructor_name")["points"].sum()\
                               .nlargest(8).index.tolist()
            df_top = df_cons[df_cons["constructor_name"].isin(top_teams)]
            fig = px.line(df_top, x="season", y="points",
                          color="constructor_name", markers=True,
                          color_discrete_map=TEAM_COLORS,
                          title="Points constructeurs par saison", **DARK)
            fig.update_layout(title_font_color="#e10600",
                               xaxis_title="Saison", yaxis_title="Points")
            st.plotly_chart(fig, use_container_width=True)

            # Heatmap position par saison
            pivot = df_cons.pivot(index="constructor_name",
                                  columns="season", values="position")
            fig2 = px.imshow(pivot, text_auto=True,
                             title="Classement par saison (1 = champion)",
                             color_continuous_scale="RdYlGn_r",
                             aspect="auto", **DARK)
            fig2.update_layout(title_font_color="#e10600")
            st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        st.markdown("### Carrière d'un pilote")
        df_career = q("""
            SELECT driver_id,
                   driver_forename||' '||driver_surname AS driver_name,
                   season, position, points, wins, constructor
            FROM driver_standings ORDER BY driver_id, season
        """)
        if not df_career.empty:
            pilots = sorted(df_career["driver_name"].dropna().unique())
            sel_p  = st.selectbox("Pilote", pilots)
            df_p   = df_career[df_career["driver_name"]==sel_p]

            c1,c2 = st.columns(2)
            with c1:
                fig = px.bar(df_p, x="season", y="points",
                             color="constructor",
                             color_discrete_map=TEAM_COLORS,
                             text="position",
                             title=f"{sel_p} — Points par saison", **DARK)
                fig.update_layout(title_font_color="#e10600")
                fig.update_traces(texttemplate="P%{text}", textposition="outside")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                fig2 = px.line(df_p, x="season", y="position",
                               markers=True,
                               title=f"{sel_p} — Classement final",
                               **DARK)
                fig2.update_layout(yaxis=dict(autorange="reversed",
                                              title="Position finale"),
                                   title_font_color="#e10600")
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("---")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("🏁 Saisons", len(df_p))
            c2.metric("🥇 Victoires", df_p["wins"].sum())
            c3.metric("🏆 Best classement", f"P{df_p['position'].min()}")
            c4.metric("📊 Max points", f"{df_p['points'].max():.0f}")


# ════════════════════════════════════════════════════════════
# PAGE : MÉTÉO & CIRCUITS
# ════════════════════════════════════════════════════════════

elif page == "🌦️ Météo & Circuits":
    st.markdown("## 🌦️ Météo & Circuits")
    tab1, tab2 = st.tabs(["🌡️ Météo", "🗺️ Carte circuits"])

    with tab1:
        if db_ok():
            df_w = q("SELECT * FROM weather")
        else:
            df_w = pd.DataFrame({
                "gp_name":       ["Bahrain GP","Monaco GP","Britain GP","Abu Dhabi GP"],
                "temp_air_c":    [28,22,18,27],
                "temp_track_c":  [42,38,28,43],
                "wind_kmh":      [15,12,35,8],
                "humidity_pct":  [55,60,70,45],
                "condition":     ["Dry","Dry","Wet","Dry"],
            })

        if not df_w.empty and "temp_air_c" in df_w.columns:
            fig = go.Figure()
            fig.add_trace(go.Bar(name="Air (°C)", x=df_w["gp_name"],
                                  y=df_w["temp_air_c"], marker_color="#FF8000"))
            if "temp_track_c" in df_w.columns:
                fig.add_trace(go.Bar(name="Piste (°C)", x=df_w["gp_name"],
                                      y=df_w["temp_track_c"], marker_color="#e10600"))
            fig.update_layout(barmode="group", title="Températures par GP",
                               **DARK, title_font_color="#e10600")
            st.plotly_chart(fig, use_container_width=True)

            if "condition" in df_w.columns:
                wet = df_w[df_w["condition"]=="Wet"]
                if not wet.empty:
                    st.markdown(f"🌧️ **Courses sous la pluie :** "
                                f"{', '.join(wet['gp_name'].tolist())}")

    with tab2:
        if db_ok():
            df_circ = q("SELECT * FROM circuits")
        else:
            df_circ = pd.DataFrame({
                "circuit_name":["Bahrain Int.","Monaco","Silverstone","Monza","Spa"],
                "country":["Bahrain","Monaco","UK","Italy","Belgium"],
                "latitude": [26.03,43.73,52.08,45.62,50.44],
                "longitude":[50.51, 7.42,-1.02, 9.28, 5.97],
            })

        if not df_circ.empty and "latitude" in df_circ.columns:
            nc = "circuit_name" if "circuit_name" in df_circ.columns else df_circ.columns[0]
            df_m = df_circ.dropna(subset=["latitude","longitude"])
            fig = px.scatter_geo(df_m, lat="latitude", lon="longitude",
                                  hover_name=nc,
                                  title="Circuits F1", **DARK)
            fig.update_traces(marker=dict(size=12,color="#e10600",symbol="circle"))
            fig.update_layout(
                geo=dict(bgcolor="rgba(20,20,30,0.8)",
                         showland=True, landcolor="rgba(50,50,70,0.8)",
                         showocean=True, oceancolor="rgba(10,10,20,0.8)"),
                title_font_color="#e10600",
            )
            st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE : PROFIL PILOTE
# ════════════════════════════════════════════════════════════

elif page == "👤 Profil Pilote":
    st.markdown("## 👤 Profil Pilote")

    if db_ok():
        df_s = q(f"SELECT * FROM driver_standings WHERE season={selected_season} ORDER BY position")
    else:
        df_s = demo_standings()

    if df_s.empty:
        st.warning("Pas de données de classement.")
        st.stop()

    sc  = "driver_surname" if "driver_surname" in df_s.columns else df_s.columns[4]
    tc  = "constructor"    if "constructor"    in df_s.columns else None
    sel = st.selectbox("🏎️ Choisir un pilote", df_s[sc].dropna().tolist())
    row = df_s[df_s[sc]==sel].iloc[0]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("📍 Classement",   f"P{row.get('position','—')}")
    c2.metric("🏆 Points",        f"{row.get('points',0):.0f}")
    c3.metric("🥇 Victoires",     row.get("wins","—"))
    c4.metric("🏭 Équipe",        row.get(tc,"—") if tc else "—")

    st.markdown("---")

    # Résultats course par course
    if db_ok():
        df_dr = q(f"""
            SELECT round, gp_name, position_num, points, grid,
                   constructor_name, status
            FROM race_results
            WHERE season={selected_season} AND driver_surname='{sel}'
            ORDER BY round
        """)
    else:
        df_all = demo_results()
        df_dr  = df_all[df_all["driver_surname"]==sel].copy()

    if not df_dr.empty:
        c1,c2 = st.columns([2,1])
        with c1:
            fig = px.line(df_dr, x="round", y="position_num",
                          markers=True,
                          title=f"Classements {sel} — {selected_season}",
                          color_discrete_sequence=["#e10600"], **DARK)
            fig.update_layout(yaxis=dict(autorange="reversed",title="Position"),
                               xaxis_title="Round",
                               title_font_color="#e10600")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            cols = [c for c in ["round","gp_name","position_num","points","grid","status"]
                    if c in df_dr.columns]
            st.dataframe(df_dr[cols], hide_index=True, use_container_width=True)

        m1,m2,m3 = st.columns(3)
        m1.metric("🏁 Courses",   len(df_dr))
        m2.metric("📊 Pts totaux", f"{df_dr['points'].sum():.0f}")
        m3.metric("📈 Moy. pos.", f"{df_dr['position_num'].mean():.1f}" if "position_num" in df_dr.columns else "—")


# ════════════════════════════════════════════════════════════
# PAGE : PRÉDICTION IA
# ════════════════════════════════════════════════════════════

elif page == "🤖 Prédiction IA":
    st.markdown("## 🤖 Prédiction du Vainqueur — IA")
    st.caption("Modèle entraîné sur l'historique des saisons · RandomForest calibré")

    # Vérifier si le modèle existe
    model_path = cfg.MODEL_PATH
    meta_path  = model_path.replace(".pkl","_meta.json")
    model_ok   = os.path.exists(model_path)

    if not model_ok:
        st.warning(
            "⚠️ Aucun modèle entraîné détecté.\n\n"
            "Lancez d'abord :\n```\npython scripts/07_ml_predict.py\n```"
        )
        st.markdown("### 📖 Comment fonctionne le modèle ?")
        st.markdown("""
Le modèle de prédiction utilise un **RandomForestClassifier** calibré (Platt scaling)
pour estimer la probabilité de victoire de chaque pilote avant une course.

**Features utilisées :**

| Feature | Description |
|---|---|
| `grid` | Position de départ sur la grille |
| `pole_position` | Partait-il de la pole ? |
| `avg_finish_prev3` | Moyenne des 3 derniers résultats |
| `win_rate_prev` | Taux de victoire historique |
| `podium_rate_prev` | Taux de podiums historique |
| `constructor_wins_season` | Victoires de l'écurie cette saison |
| `circuit_win_rate` | Taux de victoire sur ce circuit précis |

**Validation :** TimeSeriesSplit (5 folds) — respecte l'ordre chronologique.
        """)
        st.stop()

    # Charger les métadonnées
    import json
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)

    # Affichage des infos modèle
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("🤖 Modèle",    meta.get("model_name","RandomForest"))
    c2.metric("📊 AUC-ROC",   f"{meta.get('metrics',{}).get('auc',0):.4f}")
    c3.metric("🎯 Précision", f"{meta.get('metrics',{}).get('precision',0):.4f}")
    c4.metric("🗓️ Entraîné", meta.get("trained_at","—")[:10])

    st.markdown("---")
    st.markdown("### 🔮 Simuler un prochain Grand Prix")

    # Interface de simulation
    known_drivers  = meta.get("drivers", ["verstappen","norris","leclerc"])
    known_circuits = meta.get("circuits", ["bahrain","monaco","silverstone"])

    col1, col2 = st.columns(2)
    with col1:
        sel_circuit = st.selectbox("🏁 Circuit", sorted(known_circuits))
    with col2:
        st.markdown(f"**{len(known_drivers)} pilotes connus du modèle**")

    st.markdown("#### 🏎️ Configurer la grille de départ")
    st.caption("Renseignez les 5 premiers pilotes et leurs statistiques récentes")

    # Formulaire de saisie par pilote
    entries = []
    cols_form = st.columns(5)
    default_drivers = known_drivers[:5] if len(known_drivers) >= 5 else known_drivers

    for i, col in enumerate(cols_form):
        with col:
            drv = st.selectbox(f"P{i+1}", known_drivers,
                               index=i if i<len(known_drivers) else 0,
                               key=f"drv_{i}")
            grid_pos  = i + 1
            avg_fin   = st.number_input("Moy. 3 derniers", 1.0, 20.0,
                                         float(i*2+1.5), 0.5, key=f"avg_{i}")
            win_rate  = st.number_input("Taux victoires", 0.0, 1.0,
                                         max(0.0, 0.5-i*0.1), 0.05, key=f"wr_{i}")
            entries.append({
                "driver_id":          drv,
                "constructor_id":     "unknown",
                "circuit_id":         sel_circuit,
                "grid":               grid_pos,
                "pole_position":      1 if i==0 else 0,
                "avg_finish_prev3":   avg_fin,
                "win_rate_prev":      win_rate,
                "podium_rate_prev":   min(win_rate * 2.5, 1.0),
                "constructor_wins_season": max(0, 5-i),
                "circuit_win_rate":   max(0.0, 0.3 - i*0.06),
            })

    if st.button("🚀 Lancer la prédiction", type="primary"):
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "ml", os.path.join(os.path.dirname(__file__), "..", "scripts", "07_ml_predict.py")
            )
            ml_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(ml_mod)

            results = ml_mod.predict_race(entries)

            if not results.empty:
                st.markdown("---")
                st.markdown("### 🏆 Résultats de la prédiction")

                # Graphique en jauge
                fig_pred = px.bar(
                    results,
                    x="driver_id", y="win_probability",
                    title=f"Probabilités de victoire — {sel_circuit}",
                    color="win_probability",
                    color_continuous_scale=["#333", "#FF8000", "#e10600"],
                    text=results["win_probability"].apply(lambda x: f"{x*100:.1f}%"),
                    **DARK,
                )
                fig_pred.update_layout(
                    title_font_color="#e10600",
                    yaxis=dict(title="Probabilité", tickformat=".0%"),
                    coloraxis_showscale=False,
                )
                fig_pred.update_traces(textposition="outside")
                st.plotly_chart(fig_pred, use_container_width=True)

                # Favori mis en avant
                fav = results.iloc[0]
                st.success(
                    f"🏆 **Favori prédit : {fav['driver_id'].upper()}** "
                    f"avec {fav['win_probability']*100:.1f}% de probabilité de victoire"
                )

                # Tableau détaillé
                results["win_probability_pct"] = (results["win_probability"]*100).round(1)
                st.dataframe(results[["predicted_rank","driver_id",
                                       "win_probability_pct","grid","pole_position"]],
                             hide_index=True, use_container_width=True)

        except Exception as e:
            st.error(f"Erreur lors de la prédiction : {e}")
            import traceback
            st.code(traceback.format_exc())

    # Explication du modèle
    with st.expander("📖 Comment interpréter ces probabilités ?"):
        st.markdown("""
- Les probabilités reflètent les **tendances historiques** du pilote et de son écurie.
- Un pilote avec une **forte dominance** (ex: Verstappen 2023-2024) aura naturellement une probabilité plus élevée.
- La **position sur la grille** est le facteur le plus déterminant.
- Les **performances récentes** (avg_finish_prev3) pèsent davantage que la carrière complète.
- Ce modèle ne prend **pas en compte** : la météo, les accidents, les stratégies pit stop.

> ⚠️ Ces prédictions sont à titre éducatif — la F1 reste imprévisible !
        """)

# ── FOOTER ────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<p style='text-align:center;color:#555;font-size:.8em'>
F1 Data Hub · Projet 3 Wild Code School ·
<a href='https://openf1.org' style='color:#e10600'>OpenF1</a> ·
<a href='https://jolpi.ca' style='color:#e10600'>jolpi.ca</a> ·
Wikipedia
</p>
""", unsafe_allow_html=True)
