##
##

import math
import argparse
import os.path
import base64
import streamlit as st
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
from pathlib import Path
from pandas import DataFrame
import folium
from streamlit_folium import st_folium
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


def center_coordinate(df: DataFrame):
    x = 0.0
    y = 0.0
    z = 0.0

    for i, coord in df.iterrows():
        latitude = math.radians(coord['coordinates'][1])
        longitude = math.radians(coord['coordinates'][0])

        x += math.cos(latitude) * math.cos(longitude)
        y += math.cos(latitude) * math.sin(longitude)
        z += math.sin(latitude)

    total = len(df)

    x = x / total
    y = y / total
    z = z / total

    central_longitude = math.atan2(y, x)
    central_square_root = math.sqrt(x * x + y * y)
    central_latitude = math.atan2(z, central_square_root)

    mean_location = {
        'latitude': math.degrees(central_latitude),
        'longitude': math.degrees(central_longitude)
    }

    return mean_location


def get_color(x):
    if x >= 20:
        return [0, 128, 255]
    elif x < 20 or x >= 15:
        return [0, 255, 0]
    else:
        return [0, 0, 0]


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
        SELECT c.name, SUM(TO_NUMBER(amt.total)) AS total_spend
        FROM `customers` c
        JOIN `accounts` a ON ANY acc IN c.accounts SATISFIES acc = a.account_id END
        JOIN `transactions` t ON a.account_id = t.account_id
        UNNEST t.transactions AS amt
        GROUP BY c.name
        ORDER BY total_spend DESC
        LIMIT 10
        """

        restaurant_query = """
        SELECT r.address.coord AS coordinates, g.score, r.name, r.address.street, r.cuisine, r.borough
        FROM restaurants AS r
        UNNEST r.grades AS g
        WHERE g.grade = 'A'
        ORDER BY g.score DESC
        LIMIT 100
        """

        image_path = os.path.join(os.path.dirname(Path(__file__)), 'images', 'logo.png')
        st.image(image_path, width=80)
        st.title("Demo Dashboard")
        st.markdown("https://github/mminichino/columnar-demo-external")

        with st.spinner('Updating Report...'):
            f1, f2, f3 = st.columns([1, 1, 1])

            session = CBSession(st.session_state.hostname, st.session_state.username, st.session_state.password).session().bucket_name(st.session_state.bucket).scope_name(st.session_state.scope)
            results = session.analytics_query(top_spender_query)
            hhc = pd.DataFrame(results)

            fig = go.Figure(
                data=[go.Table(columnorder=[0, 1], columnwidth=[18, 12],
                               header=dict(
                                   values=list(item.title() for item in hhc.columns),
                                   font=dict(size=20, color='white'),
                                   fill_color='#264653',
                                   line_color='rgba(255,255,255,0.2)',
                                   align=['left', 'center'],
                                   height=30),
                               cells=dict(
                                   values=[hhc[K].tolist() for K in hhc.columns],
                                   font=dict(size=20),
                                   align=['left', 'center'],
                                   format=[[None], [',.2f']],
                                   prefix=["", "$"],
                                   line_color='rgba(255,255,255,0.2)',
                                   height=30))])

            fig.update_layout(title=dict(text="Top Customers by Spend", font=dict(size=30, color='#264653')), title_x=0, margin=dict(l=0, r=10, b=10, t=50), height=500, width=600)

            f2.plotly_chart(fig, use_container_width=True)

            c1, c2 = st.columns([1, 1])

            results = session.analytics_query(restaurant_query)
            df = pd.DataFrame(results)

            ddf = df[['score', 'name', 'street', 'cuisine', 'borough']]
            fig = go.Figure(
                data=[go.Table(columnorder=[0, 1, 2, 3, 4], columnwidth=[8, 18, 18, 18, 12],
                               header=dict(
                                   values=[item.title() for item in ddf.columns.to_list()],
                                   font=dict(size=14, color='white'),
                                   fill_color='#264653',
                                   line_color='rgba(255,255,255,0.2)',
                                   align='center',
                                   height=28),
                               cells=dict(
                                   values=[ddf[col].to_list() for col in ddf.columns],
                                   font=dict(size=14),
                                   align='center',
                                   line_color='rgba(255,255,255,0.2)',
                                   height=28))])

            fig.update_layout(title=dict(text="Highly Rated Restaurants", font=dict(size=30, color='#264653')), title_x=0, margin=dict(l=0, r=10, b=10, t=50), height=1000)

            c2.plotly_chart(fig, use_container_width=True)

            center = center_coordinate(df)

            view_state = pdk.ViewState(
                latitude=center['latitude'],
                longitude=center['longitude'],
                zoom=12,
                pitch=0,
                height=900,
                width=900
            )

            marker_path = os.path.join(os.path.dirname(Path(__file__)), 'images', 'marker_red.png')

            binary_fc = open(marker_path, 'rb').read()
            base64_utf8_str = base64.b64encode(binary_fc).decode('utf-8')

            ext = marker_path.split('.')[-1]
            dataurl = f'data:image/{ext};base64,{base64_utf8_str}'

            icon_data = {
                "url": dataurl,
                "width": 128,
                "height": 128,
                "anchorY": 128,
            }

            df['icon_data'] = [icon_data for _ in range(df.shape[0])]

            layer = pdk.Layer(
                "IconLayer",
                data=df,
                get_position='coordinates',
                get_icon='icon_data',
                get_size=4,
                size_scale=12,
                get_radius=100,
                pickable=True,
            )

            tooltip = {
                "html": """
                <b>Name:</b> {name} <br/>
                <b>Address:</b> {street} <br/>
                <b>Cuisine:</b> {cuisine} <br/>
                <b>Borough:</b> {borough}
                """,
                "style": {"backgroundColor": "steelblue", "color": "white"}
            }

            r = pdk.Deck(
                map_style="mapbox://styles/mapbox/streets-v12",
                initial_view_state=view_state,
                layers=[layer],
                tooltip=tooltip,
            )

            new_title = '<p style="font-family: &quot;Source Sans Pro&quot;, sans-serif; font-size: 30px; color: #264653; opacity: 1; font-weight: normal; white-space: pre;">Restaurant Map</p>'
            c1.markdown(new_title, unsafe_allow_html=True)
            c1.pydeck_chart(r, use_container_width=False)


if __name__ == '__main__':
    main()
