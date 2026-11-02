import os
from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from angel_client import AngelClient, load_credentials
from scanner import build_timeframes, analyze_smc_stack

load_dotenv()
st.set_page_config(page_title="NIFTY SMC Scanner", layout="wide")
st.title("NIFTY 50 · Multi-Timeframe SMC Scanner")
st.caption("Angel One SmartAPI · Weekly options context · Moderate risk · Manual execution only")

with st.sidebar:
    st.header("Connection")
    client_code = st.text_input("Angel One client code", value=os.getenv("ANGEL_CLIENT_CODE", ""))
    api_key = st.text_input("SmartAPI key", value=os.getenv("ANGEL_API_KEY", ""), type="password")
    pin = st.text_input("PIN / password", value=os.getenv("ANGEL_PIN", ""), type="password")
    totp_secret = st.text_input("TOTP secret", value=os.getenv("ANGEL_TOTP_SECRET", ""), type="password")
    st.caption("Credentials stay on this computer. Never share them in chat or commit .env.")
    st.divider()
    token = st.text_input("NIFTY 50 symbol token", value=os.getenv("ANGEL_NIFTY_TOKEN", "99926000"))
    exchange = st.selectbox("Exchange", ["NSE"], index=0)
    st.caption("Verify the token against Angel One's current instrument master before use.")
    days = st.slider("Calendar days to request", 15, 30, 20)

if "angel_client" not in st.session_state:
    st.session_state.angel_client = None
if "login_message" not in st.session_state:
    st.session_state.login_message = ""

c1, c2 = st.columns(2)
with c1:
    if st.button("Login to Angel One", type="primary"):
        try:
            creds = load_credentials(client_code, api_key, pin, totp_secret)
            client = AngelClient(creds)
            client.login()
            st.session_state.angel_client = client
            st.session_state.login_message = "Login successful. Credentials were not displayed."
        except Exception as e:
            st.session_state.angel_client = None
            st.session_state.login_message = f"Login failed: {e}"
with c2:
    fetch = st.button("Fetch historical candles")

if st.session_state.login_message:
    st.info(st.session_state.login_message)

if fetch:
    if not st.session_state.angel_client:
        st.error("Login first.")
    else:
        try:
            end = datetime.now()
            start = end - timedelta(days=days)
            with st.spinner("Fetching NIFTY historical candles..."):
                one = st.session_state.angel_client.historical(
                    exchange, token, "ONE_MINUTE", start, end
                )
            frames = build_timeframes(one)
            st.session_state.frames = frames
            st.success(f"Loaded {len(one)} one-minute candles.")
        except Exception as e:
            st.error(f"Data fetch failed: {e}")

if "frames" in st.session_state:
    frames = st.session_state.frames
    tabs = st.tabs(["5m BOS / MSS", "2m FVG / OB / IDM", "1m Entry"])
    for tab, tf in zip(tabs, ["5m", "2m", "1m"]):
        with tab:
            df = frames.get(tf, pd.DataFrame())
            if df.empty:
                st.warning(f"No {tf} candles available.")
                continue
            st.metric("Candles", len(df))
            st.dataframe(df.tail(30), use_container_width=True)
    st.subheader("SMC signal analysis")
    analysis = analyze_smc_stack(frames)
    st.metric("Current signal", analysis["signal"])
    st.write("**BOS / MSS / FVG / Order Block / IDM checks**")
    for key, value in analysis.get("checks", {}).items():
        st.write(f"- {key}: {'PASS' if value else 'WAIT'}")
    for reason in analysis.get("reasons", []):
        st.write(f"- {reason}")
    st.warning("Signal window: 09:30–11:40 IST. Research prototype only; no order placement or guaranteed performance.")
else:
    st.markdown("""
    ### Selected configuration
    - Multi-timeframe: **5m structure → 2m setup → 1m entry**
    - SMC concepts: **FVG, order blocks, IDM, BOS and MSS**
    - Signal window: **09:30–11:40 IST**
    - Historical analysis: cached NIFTY one-minute candles, resampled locally to 2m and 5m
    """)
