import streamlit as st
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

st.set_page_config(page_title='ETF Industry Heatmap', layout='wide')


def parse_holdings_upload(uploaded_file):
    df = pd.read_csv(uploaded_file)
    return df


def parse_holdings_text(text):
    # expect CSV-like content
    from io import StringIO
    return pd.read_csv(StringIO(text))


@st.cache_data(ttl=3600)
def get_prices(tickers, start, end):
    # return Adj Close prices DataFrame
    df = yf.download(tickers, start=start, end=end, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    # yfinance returns DataFrame with columns; prefer 'Adj Close' if present
    if 'Adj Close' in df.columns:
        prices = df['Adj Close']
    else:
        # if single ticker, df may already be series-like
        if isinstance(df, pd.Series):
            prices = df
        else:
            # try Close
            prices = df['Close'] if 'Close' in df.columns else df
    # Ensure columns for single ticker case
    if isinstance(prices, pd.Series):
        prices = prices.to_frame()
    prices.columns = [str(c) for c in prices.columns]
    return prices


def compute_industry_metrics(holdings: pd.DataFrame, start: str, end: str):
    # holdings: columns [ETF, Ticker, Weight, Industry]
    # fetch prices
    tickers = holdings['Ticker'].unique().tolist()
    prices = get_prices(tickers, start, end)
    if prices.empty:
        st.error('No price data fetched for tickers.')
        return None

    # compute total return over period for each ticker
    start_prices = prices.loc[prices.index.min()]
    end_prices = prices.loc[prices.index.max()]
    returns = (end_prices / start_prices - 1).rename('Return')
    returns = returns.reset_index().melt(id_vars=None, var_name='Ticker', value_name='Return') if False else returns
    returns = returns.to_frame().reset_index()
    returns.columns = ['Ticker', 'Return']

    # merge with holdings
    df = holdings.merge(returns, on='Ticker', how='left')
    df['Weight'] = pd.to_numeric(df['Weight'], errors='coerce').fillna(0)

    # compute industry-level weighted return per ETF
    grouped = df.groupby(['ETF', 'Industry']).apply(lambda g: (g['Weight'] * g['Return']).sum() / (g['Weight'].sum() if g['Weight'].sum() != 0 else 1)).reset_index()
    grouped.columns = ['ETF', 'Industry', 'WeightedReturn']

    # compute exposure (sum of weights)
    exposure = df.groupby(['ETF', 'Industry'])['Weight'].sum().reset_index()
    exposure.columns = ['ETF', 'Industry', 'Exposure']

    metrics = grouped.merge(exposure, on=['ETF', 'Industry'], how='outer')
    metrics = metrics.fillna(0)
    return metrics


def draw_heatmap(metrics: pd.DataFrame, value_col: str = 'WeightedReturn'):
    # pivot for heatmap: ETF rows, Industry columns
    pivot = metrics.pivot(index='ETF', columns='Industry', values=value_col).fillna(0)
    plt.figure(figsize=(max(6, pivot.shape[1]*0.7), max(3, pivot.shape[0]*0.4)))
    sns.heatmap(pivot, annot=True, fmt='.2%', cmap='RdYlGn', center=0)
    st.pyplot(plt)


def main():
    st.title('ETF Industry Heatmap Scanner')

    st.markdown('Upload a holdings CSV with columns: `ETF,Ticker,Weight,Industry`. Weight should be numeric (e.g., percent or fraction).')

    col1, col2 = st.columns([1, 1])
    with col1:
        uploaded = st.file_uploader('Upload holdings CSV', type=['csv'])
    with col2:
        text_input = st.text_area('Or paste CSV text here')

    holdings = None
    if uploaded is not None:
        try:
            holdings = parse_holdings_upload(uploaded)
        except Exception as e:
            st.error(f'Failed to read CSV: {e}')
    elif text_input:
        try:
            holdings = parse_holdings_text(text_input)
        except Exception as e:
            st.error(f'Failed to parse text: {e}')

    st.sidebar.header('Settings')
    today = datetime.today().strftime('%Y-%m-%d')
    end = st.sidebar.date_input('End date', value=pd.to_datetime(today))
    start = st.sidebar.date_input('Start date', value=pd.to_datetime('2022-01-01'))
    value_metric = st.sidebar.selectbox('Heatmap metric', ['WeightedReturn', 'Exposure'])

    if holdings is None:
        st.info('Upload holdings CSV or paste holdings text to proceed. A sample file is provided in the repo (sample_holdings.csv).')
        st.stop()

    # validate holdings columns
    expected = {'ETF', 'Ticker', 'Weight', 'Industry'}
    if not expected.issubset(set(holdings.columns)):
        st.error(f'Holdings CSV must include columns: {expected}. Found: {list(holdings.columns)}')
        st.stop()

    # compute metrics
    metrics = compute_industry_metrics(holdings, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    if metrics is None:
        st.stop()

    st.subheader('Industry Heatmap')
    draw_heatmap(metrics, value_col=value_metric)

    st.subheader('Metrics Table')
    st.dataframe(metrics)


if __name__ == '__main__':
    main()
