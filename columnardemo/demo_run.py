##
##

import argparse
import streamlit as st
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from columnardemo.columnar_driver import CBSession


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
    parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
    parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
    parser.add_argument('-b', '--bucket', action='store', help="Bucket", default="cbdocs")
    parser.add_argument('-s', '--scope', action='store', help="Scope", default="_default")
    options = parser.parse_args()
    return options


def main():
    options = parse_args()

    st.set_page_config(page_title='Couchbase Columnar JSON Analytics', layout='wide', page_icon=':sparkles:')

    if "auth" not in st.session_state:
        st.session_state.auth = False

    if not st.session_state.auth:
        host_name = st.text_input("Couchbase Server Hostname", options.host, autocomplete="on")
        user_name = st.text_input("Username", options.user)
        user_password = st.text_input("Password", options.password, type="password")
        bucket_name = st.text_input("Database", options.bucket)
        scope_name = st.text_input("Scope", options.scope)
        pwd_submit = st.button("Start Demo")

        if pwd_submit:
            try:
                CBSession(host_name, user_name, user_password).session().bucket_name(bucket_name).scope_name(scope_name)
            except Exception:
                st.error("Can not login to Columnar")
            else:
                st.session_state.hostname = host_name
                st.session_state.username = user_name
                st.session_state.password = user_password
                st.session_state.bucket = bucket_name
                st.session_state.scope = scope_name
                st.session_state.auth = True
                st.rerun()

    if st.session_state.auth:
        top_spender_query = """
        SELECT c.name, SUM(amt.amount) AS total_spend
        FROM `customers` c
        JOIN `accounts` a ON ANY acc IN c.accounts SATISFIES acc = a.account_id END
        JOIN `transactions` t ON a.account_id = t.account_id
        UNNEST t.transactions AS amt
        GROUP BY c.name
        ORDER BY total_spend DESC
        LIMIT 10
        """

        t1, t2 = st.columns((0.07, 1))

        # t1.image('images/favicon.ico', width=120)
        t2.title("Customer Dashboard")
        t2.markdown("https://github/mminichino")

        with st.spinner('Updating Report...'):
            p1, p2 = st.columns((3, 1.7))

            session = CBSession(st.session_state.hostname, st.session_state.username, st.session_state.password).session().bucket_name(st.session_state.bucket).scope_name(st.session_state.scope)
            results = session.analytics_query(top_spender_query)
            hhc = pd.DataFrame(results)

            colorcode = []

            for i in range(0, 13):
                colorcode.append(hhc['c' + str(i)].tolist())

            fig = go.Figure(
                data=[go.Table(columnorder=[0, 1], columnwidth=[18, 12],
                               header=dict(
                                   values=list(hhc.columns),
                                   font=dict(size=11, color='white'),
                                   fill_color='#264653',
                                   line_color='rgba(255,255,255,0.2)',
                                   align=['left', 'center'],
                                   height=20
                               )
                               , cells=dict(
                        values=[hhc[K].tolist() for K in hhc.columns],
                        font=dict(size=10),
                        align=['left', 'center'],
                        fill_color=colorcode,
                        line_color='rgba(255,255,255,0.2)',
                        height=20))])

            fig.update_layout(title_text="Top Customers by Spend", title_font_color='#264653', title_x=0, margin=dict(l=0, r=10, b=10, t=30), height=600)

            p1.plotly_chart(fig, use_container_width=True)
