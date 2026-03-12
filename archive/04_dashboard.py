"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          PROJET 3 — WILD DATA HUB · API FORMULE 1                           ║
║          Phase 4 : Dashboard Streamlit                                       ║
║                                                                              ║
║  Lancer : streamlit run 04_dashboard.py                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Configuration Streamlit ────────────────────────────────────────────────
st.set_page_config(
    page_title="🏎️  F1 Data Hub",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Palette ────────────────────────────────────────────────────────────────
COLORS = {
    "red":    "#E84545",
    "navy":   "#1A2744",
    "cyan":   "#17A8C8",
    "gold":   "#D4A017",
    "green":  "#1FAB6A",
    "white":  "#FFFFFF",
    "gray":   "#F4F7FB",
}

TEAM_COLORS = {
    "mercedes":    "#00D2BE",
    "red_bull":    "#0600EF",
    "ferrari":     "#DC0000",
    "mclaren":     "#FF8700",
    "alpine":      "#0090FF",
    "aston_martin":"#006F62",
    "haas":        "#FFFFFF",
    "alfa":        "#900000",
    "williams":    "#005AFF",
    "alphatauri":  "#2B4562",
}

# ── CSS personnalisé ───────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0A1128; }
    .main .block-container { padding: 1rem 2rem; }
    h1, h2, h3 { color: #17A8C8; font-family: 'Arial Black', sans-serif; }
    .metric-card {
        background: linear-gradient(135deg, #1A2744 0%, #0D1B2E 100%);
        border: 1px solid #17A8C8;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: bold; color: #D4A017; }
    .metric-label { font-size: 0.85rem; color: #7A8EA8; }
    .stSelectbox > div { background-color: #1A2744; }
    .stSidebar { background-color: #0D1B2E; }
    [data-testid="metric-container"] {
        background: #1A2744;
        border: 1px solid #17A8C8;
        border-radius: 8px;
        padding: 0.5rem 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  CONNEXION BDD + CACHE
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_engine():
    host     = os.getenv("DB_HOST",     "localhost")
    port     = os.getenv("DB_PORT",     "5432")
    name     = os.getenv("DB_NAME",     "f1_db")
    user     = os.getenv("DB_USER",     "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url, echo=False)


@st.cache_data(ttl=3600)
def query(_engine, sql: str, params: dict = None) -> pd.DataFrame:
    """Exécute une requête SQL avec cache 1h."""
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ══════════════════════════════════════════════════════════════════════════════
#  CHARGEMENT DES DONNÉES PRINCIPALES
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def load_data(_engine):
    resultats = query(_engine, "SELECT * FROM v_resultats_complets")
    palmares  = query(_engine, "SELECT * FROM v_palmares_pilotes")
    ecuries   = query(_engine, "SELECT * FROM v_palmares_ecuries")
    classement= query(_engine, "SELECT * FROM v_classement_saison_pilotes")
    circuits  = query(_engine, "SELECT * FROM circuits")
    pitstops  = query(_engine, "SELECT * FROM v_stats_pit_stops")
    return resultats, palmares, ecuries, classement, circuits, pitstops


# ── Fallback CSV si pas de BDD ────────────────────────────────────────────
def load_from_csv():
    """Chargement depuis data/clean/ si la BDD n'est pas dispo."""
    clean = Path("data/clean")
    dfs   = {}
    for f in clean.glob("*.csv"):
        dfs[f.stem] = pd.read_csv(f, low_memory=False)
    return dfs


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar(resultats: pd.DataFrame):
    with st.sidebar:
        st.markdown("## 🏎️  F1 Data Hub")
        st.markdown("---")

        # Navigation
        page = st.radio(
            "Navigation",
            ["🏠 Accueil",
             "📊 Résultats",
             "🏆 Palmarès",
             "🏁 Circuits",
             "⏱️  Pit Stops",
             "📈 Tendances"],
            label_visibility="collapsed"
        )

        st.markdown("---")
        st.markdown("### Filtres")

        # Sélection saison
        saisons = sorted(resultats["saison"].dropna().unique(), reverse=True)
        saison  = st.selectbox("Saison", saisons, index=0)

        # Sélection pilote
        pilotes_saison = sorted(
            resultats[resultats["saison"] == saison]["pilote"].dropna().unique()
        )
        pilote = st.selectbox("Pilote", ["Tous"] + list(pilotes_saison))

        # Sélection écurie
        ecuries_saison = sorted(
            resultats[resultats["saison"] == saison]["ecurie"].dropna().unique()
        )
        ecurie = st.selectbox("Écurie", ["Toutes"] + list(ecuries_saison))

        st.markdown("---")
        st.caption("🔗 Données : Ergast API + OpenF1")
        st.caption("📚 Wild Code School — Projet 3")

    return page, saison, pilote, ecurie


# ══════════════════════════════════════════════════════════════════════════════
#  PAGES
# ══════════════════════════════════════════════════════════════════════════════

def page_accueil(resultats, palmares, ecuries_df, saison):
    st.title("🏎️  Formula 1 — Data Hub")
    st.markdown(f"*Tableau de bord analytique · Saison sélectionnée : **{saison}***")

    df_s = resultats[resultats["saison"] == saison]

    # ── KPIs ────────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("🏁 Courses", df_s["round"].nunique())
    with col2:
        st.metric("🧑‍✈️ Pilotes", df_s["pilote"].nunique())
    with col3:
        st.metric("🏭 Écuries", df_s["ecurie"].nunique())
    with col4:
        top_pilote = df_s.groupby("pilote")["points"].sum().idxmax() if not df_s.empty else "—"
        top_pts    = df_s.groupby("pilote")["points"].sum().max()    if not df_s.empty else 0
        st.metric("🥇 Champion pts", f"{top_pts:.0f}", top_pilote)
    with col5:
        top_ec = df_s.groupby("ecurie")["points"].sum().idxmax() if not df_s.empty else "—"
        ec_pts = df_s.groupby("ecurie")["points"].sum().max()    if not df_s.empty else 0
        st.metric("🏆 Meilleure écurie", f"{ec_pts:.0f}", top_ec)

    st.markdown("---")

    # ── Évolution des points pilotes top 5 ──────────────────────────────
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader(f"📈 Évolution des points — Saison {saison}")

        df_prog = df_s.groupby(["round","pilote"])["points"].sum().reset_index()
        df_prog = df_prog.sort_values("round")
        df_prog["cumul"] = df_prog.groupby("pilote")["points"].cumsum()

        top5 = df_s.groupby("pilote")["points"].sum().nlargest(5).index.tolist()
        df_top5 = df_prog[df_prog["pilote"].isin(top5)]

        fig = px.line(
            df_top5, x="round", y="cumul", color="pilote",
            title="Points cumulés — Top 5 pilotes",
            labels={"round": "Round", "cumul": "Points cumulés"},
            template="plotly_dark",
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig.update_layout(
            paper_bgcolor="#1A2744",
            plot_bgcolor="#0D1B2E",
            font_color="#FFFFFF",
            legend_title="Pilote",
        )
        fig.update_traces(line_width=2.5)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("🏆 Classement")
        class_s = df_s.groupby("pilote").agg(
            points=("points", "sum"),
            victoires=("est_victoire", "sum"),
            podiums=("est_podium", "sum"),
        ).sort_values("points", ascending=False).head(10).reset_index()
        class_s.index = range(1, len(class_s)+1)
        class_s.columns = ["Pilote", "Pts", "V", "Podiums"]
        st.dataframe(class_s, use_container_width=True, height=380)

    # ── Camembert victoires par écurie ──────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("🏭 Victoires par écurie")
        vic_ec = df_s[df_s["est_victoire"]].groupby("ecurie").size().reset_index(name="victoires")
        fig2 = px.pie(
            vic_ec, values="victoires", names="ecurie",
            template="plotly_dark", hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig2.update_layout(paper_bgcolor="#1A2744", font_color="#FFFFFF")
        st.plotly_chart(fig2, use_container_width=True)

    with col_b:
        st.subheader("🗓️ Points par Grand Prix")
        pts_round = df_s.groupby(["round","nom_gp"])["points"].sum().reset_index()
        fig3 = px.bar(
            pts_round, x="round", y="points",
            hover_data=["nom_gp"],
            template="plotly_dark",
            color_discrete_sequence=[COLORS["cyan"]],
            labels={"round": "Round", "points": "Points distribués"},
        )
        fig3.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E", font_color="#FFFFFF")
        st.plotly_chart(fig3, use_container_width=True)


def page_resultats(resultats, saison, pilote, ecurie):
    st.title("📊 Résultats de courses")

    df = resultats[resultats["saison"] == saison].copy()
    if pilote != "Tous":
        df = df[df["pilote"] == pilote]
    if ecurie != "Toutes":
        df = df[df["ecurie"] == ecurie]

    # Heatmap position par round
    st.subheader("🗺️ Carte de chaleur — Position par round")
    pivot = df.pivot_table(index="pilote", columns="round", values="position", aggfunc="first")
    fig_heat = px.imshow(
        pivot, aspect="auto",
        color_continuous_scale=[(0,"#1FAB6A"),(0.5,"#D4A017"),(1,"#E84545")],
        template="plotly_dark",
        labels={"x": "Round", "y": "Pilote", "color": "Position"},
    )
    fig_heat.update_layout(paper_bgcolor="#1A2744", font_color="#FFFFFF")
    st.plotly_chart(fig_heat, use_container_width=True)

    # Scatter grille → position finale
    st.subheader("📉 Grille de départ vs Position finale")
    col1, col2 = st.columns(2)
    with col1:
        fig_scatter = px.scatter(
            df, x="grille", y="position",
            color="ecurie", hover_data=["pilote","nom_gp","round"],
            template="plotly_dark",
            labels={"grille": "Grille de départ", "position": "Position finale"},
        )
        fig_scatter.add_shape(type="line", x0=1,y0=1,x1=20,y1=20,
                              line=dict(color="white",dash="dash",width=1))
        fig_scatter.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E", font_color="#FFFFFF")
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col2:
        st.subheader("📋 Tableau détaillé")
        cols_show = ["round","nom_gp","pilote","ecurie","grille","position","points","statut"]
        cols_show = [c for c in cols_show if c in df.columns]
        st.dataframe(
            df[cols_show].sort_values(["round","position"]),
            use_container_width=True, height=400
        )


def page_palmares(palmares, ecuries_df):
    st.title("🏆 Palmarès")

    tab1, tab2 = st.tabs(["🧑‍✈️ Pilotes", "🏭 Écuries"])

    with tab1:
        st.subheader("Top 20 — Points en carrière")
        top20 = palmares.nlargest(20, "points_total")

        fig_bar = px.bar(
            top20, x="points_total", y="nom_complet",
            orientation="h", color="victoires",
            color_continuous_scale="YlOrRd",
            template="plotly_dark",
            labels={"points_total": "Points totaux", "nom_complet": "", "victoires": "Victoires"},
        )
        fig_bar.update_layout(
            paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E",
            font_color="#FFFFFF", yaxis=dict(autorange="reversed")
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # Tableau complet
        cols = ["nom_complet","nationalite","saisons","courses","points_total",
                "victoires","podiums","poles","position_moyenne","abandons"]
        cols = [c for c in cols if c in palmares.columns]
        st.dataframe(
            palmares[cols].sort_values("points_total", ascending=False),
            use_container_width=True
        )

    with tab2:
        st.subheader("Palmarès par écurie")
        fig_ec = px.bar(
            ecuries_df.nlargest(15, "points_total"),
            x="ecurie", y="points_total",
            color="victoires",
            color_continuous_scale="Blues",
            template="plotly_dark",
            labels={"points_total": "Points", "ecurie": "Écurie", "victoires": "Victoires"},
        )
        fig_ec.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E", font_color="#FFFFFF")
        st.plotly_chart(fig_ec, use_container_width=True)


def page_circuits(circuits_df, resultats):
    st.title("🏁 Circuits")

    st.subheader("🌍 Carte des circuits")
    df_map = circuits_df.dropna(subset=["latitude","longitude"])

    fig_map = px.scatter_mapbox(
        df_map, lat="latitude", lon="longitude",
        hover_name="nom", hover_data=["pays","localite"],
        zoom=1, height=500,
        color_discrete_sequence=[COLORS["red"]],
    )
    fig_map.update_layout(
        mapbox_style="carto-darkmatter",
        paper_bgcolor="#1A2744",
        font_color="#FFFFFF",
        margin={"r":0,"t":0,"l":0,"b":0},
    )
    st.plotly_chart(fig_map, use_container_width=True)

    # Statistiques circuits
    if "pays_circuit" in resultats.columns:
        st.subheader("📊 Circuits les plus visités")
        visits = resultats.groupby("circuit")["saison"].nunique().reset_index()
        visits.columns = ["Circuit","Saisons"]
        visits = visits.sort_values("Saisons", ascending=False).head(20)
        fig_vis = px.bar(visits, x="Saisons", y="Circuit", orientation="h",
                         template="plotly_dark",
                         color_discrete_sequence=[COLORS["cyan"]])
        fig_vis.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E",
                               font_color="#FFFFFF", yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_vis, use_container_width=True)


def page_pitstops(pitstops_df):
    st.title("⏱️  Pit Stops")

    if pitstops_df.empty:
        st.warning("Données pit stops non disponibles.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏎️ Durée moyenne par écurie")
        fig = px.bar(
            pitstops_df.sort_values("duree_moy_sec"),
            x="duree_moy_sec", y="ecurie",
            orientation="h",
            color="duree_moy_sec",
            color_continuous_scale=[(0,"#1FAB6A"),(0.5,"#D4A017"),(1,"#E84545")],
            template="plotly_dark",
            labels={"duree_moy_sec": "Durée moyenne (s)", "ecurie": "Écurie"},
        )
        fig.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E", font_color="#FFFFFF")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("📋 Statistiques détaillées")
        st.dataframe(pitstops_df, use_container_width=True)

    # Évolution temporelle
    if "saison" in pitstops_df.columns:
        st.subheader("📈 Évolution des durées de pit stops")
        fig_evo = px.line(
            pitstops_df.groupby("saison")["duree_moy_sec"].mean().reset_index(),
            x="saison", y="duree_moy_sec",
            template="plotly_dark",
            labels={"saison": "Saison", "duree_moy_sec": "Durée moyenne (s)"},
            markers=True,
        )
        fig_evo.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E", font_color="#FFFFFF")
        fig_evo.update_traces(line_color=COLORS["cyan"], marker_color=COLORS["gold"])
        st.plotly_chart(fig_evo, use_container_width=True)


def page_tendances(resultats):
    st.title("📈 Tendances & Analyses")

    # Évolution domination écuries
    st.subheader("🏭 Domination des écuries par saison")
    dom = resultats[resultats["est_victoire"]].groupby(["saison","ecurie"]).size().reset_index(name="victoires")
    fig_dom = px.bar(
        dom, x="saison", y="victoires", color="ecurie",
        barmode="stack", template="plotly_dark",
        labels={"saison": "Saison", "victoires": "Victoires", "ecurie": "Écurie"},
    )
    fig_dom.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E", font_color="#FFFFFF")
    st.plotly_chart(fig_dom, use_container_width=True)

    # Nationalités
    st.subheader("🌍 Nationalités des pilotes champions")
    col1, col2 = st.columns(2)
    with col1:
        nat = resultats[resultats["est_victoire"]].groupby("nationalite_pilote").size().reset_index(name="victoires")
        nat = nat.sort_values("victoires", ascending=False).head(15)
        fig_nat = px.bar(nat, x="victoires", y="nationalite_pilote", orientation="h",
                         template="plotly_dark",
                         color_discrete_sequence=[COLORS["red"]],
                         labels={"victoires":"Victoires","nationalite_pilote":"Nationalité"})
        fig_nat.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E",
                               font_color="#FFFFFF", yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_nat, use_container_width=True)

    with col2:
        # Corrélation grille → résultat
        corr_data = resultats.dropna(subset=["grille","position"]).copy()
        fig_corr = px.density_heatmap(
            corr_data, x="grille", y="position",
            nbinsx=20, nbinsy=20,
            template="plotly_dark",
            labels={"grille":"Grille de départ","position":"Position finale"},
            color_continuous_scale="Plasma",
        )
        fig_corr.update_layout(paper_bgcolor="#1A2744", plot_bgcolor="#0D1B2E", font_color="#FFFFFF")
        st.subheader("📉 Corrélation grille → position")
        st.plotly_chart(fig_corr, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # Connexion BDD
    try:
        engine = get_engine()
        resultats, palmares, ecuries_df, classement, circuits_df, pitstops_df = load_data(engine)
        db_ok = True
    except Exception as e:
        st.warning(f"⚠️  BDD non disponible ({e}) — chargement depuis CSV")
        dfs = load_from_csv()
        resultats    = dfs.get("resultats",           pd.DataFrame())
        palmares     = dfs.get("classements_pilotes", pd.DataFrame())
        ecuries_df   = dfs.get("classements_ecuries", pd.DataFrame())
        classement   = pd.DataFrame()
        circuits_df  = dfs.get("circuits",            pd.DataFrame())
        pitstops_df  = dfs.get("pit_stops",           pd.DataFrame())
        db_ok = False

    if resultats.empty:
        st.error("❌ Aucune donnée disponible. Lancez d'abord : python 01_collecte_donnees.py test")
        st.stop()

    # Sidebar
    page, saison, pilote, ecurie = render_sidebar(resultats)

    # Router pages
    if page == "🏠 Accueil":
        page_accueil(resultats, palmares, ecuries_df, saison)
    elif page == "📊 Résultats":
        page_resultats(resultats, saison, pilote, ecurie)
    elif page == "🏆 Palmarès":
        page_palmares(palmares, ecuries_df)
    elif page == "🏁 Circuits":
        page_circuits(circuits_df, resultats)
    elif page == "⏱️  Pit Stops":
        page_pitstops(pitstops_df)
    elif page == "📈 Tendances":
        page_tendances(resultats)


if __name__ == "__main__":
    main()
