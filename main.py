import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hashlib
import json
from google.oauth2 import service_account
from posthog import Posthog
import numpy as np

# Initialize PostHog with error handling
try:
    posthog = Posthog(
        project_api_key='phc_iY1kjQZ5ib5oy0PU2fRIqJZ5323jewSS5fVDNyhe7RY',
        host='https://us.i.posthog.com'
    )
    POSTHOG_ENABLED = True
except Exception as e:
    print(f"PostHog initialization failed: {e}")
    posthog = None
    POSTHOG_ENABLED = False

# Authentication credentials (same as pipeline_matcher)
CREDENTIALS = {
    "admin": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",  # admin
    "user": "04f8996da763b7a969b1028ee3007569eaf3a635486ddab211d512c85b9df8fb",  # user
    "dina.teilab@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mai.sobhy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mostafa.sayed@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ahmed.hassan@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohamed.youssef@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ahmed.nagy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "adel.abuelella@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ammar.abdelbaset@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "youssef.mohamed@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "abdallah.hazem@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohamed.abdelgalil@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohanad.elgarhy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
}


def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in CREDENTIALS and \
                hashlib.sha256(st.session_state["password"].encode()).hexdigest() == CREDENTIALS[
            st.session_state["username"]]:
            st.session_state["password_correct"] = True
            st.session_state["current_user"] = st.session_state["username"]  # Store the username

            # First identify the user
            if POSTHOG_ENABLED and posthog:
                try:
                    posthog.identify(
                        st.session_state["username"],  # Use email as distinct_id
                        {
                            'email': st.session_state["username"],
                            'name': st.session_state["username"].split('@')[0].replace('.', ' ').title(),
                            'last_login': datetime.now().isoformat()
                        }
                    )

                    # Then capture the login event
                    posthog.capture(
                        st.session_state["username"],
                        '$login',
                        {
                            'app': 'Showroom Matcher',
                            'login_method': 'password',
                            'success': True
                        }
                    )
                except Exception as e:
                    print(f"PostHog tracking failed: {e}")

            del st.session_state["password"]  # Don't store the password
            del st.session_state["username"]  # Don't store the username
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated
    if st.session_state.get("password_correct", False):
        return True

    # Show input fields for username and password
    st.text_input("Username", key="username")
    st.text_input("Password", type="password", key="password")
    st.button("Login", on_click=password_entered)

    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")

    return False


# Set page config
st.set_page_config(
    page_title="Showroom Matcher - Car to Dealer Showroom Matching",
    page_icon="🏪",
    layout="wide"
)

# Main app logic
if check_password():

    # Define car groups by origin (same as pipeline_matcher)
    CAR_GROUPS = {
        'Japanese': ['Toyota', 'Honda', 'Nissan', 'Mazda', 'Subaru', 'Mitsubishi', 'Lexus', 'Infiniti', 'Acura'],
        'German': ['BMW', 'Mercedes-Benz', 'Mercedes', 'Audi', 'Volkswagen', 'Porsche', 'Mini', 'Opel'],
        'Chinese': ['Chery', 'Geely', 'BYD', 'MG', 'Changan', 'JAC', 'Dongfeng', 'Brilliance'],
        'Korean': ['Hyundai', 'Kia', 'Genesis', 'SsangYong', 'Daewoo'],
        'American': ['Ford', 'Chevrolet', 'Cadillac', 'Jeep', 'Chrysler', 'Dodge', 'Lincoln', 'GMC'],
        'French': ['Peugeot', 'Renault', 'Citroën', 'DS'],
        'Italian': ['Fiat', 'Alfa Romeo', 'Lancia', 'Ferrari', 'Lamborghini', 'Maserati'],
        'British': ['Land Rover', 'Range Rover', 'Jaguar', 'Bentley', 'Rolls-Royce', 'Aston Martin'],
        'Swedish': ['Volvo', 'Saab'],
        'Czech': ['Skoda']
    }


    def get_car_group(make):
        """Get the origin group for a car make"""
        for group, makes in CAR_GROUPS.items():
            if make in makes:
                return group
        return 'Other'


    def get_mileage_segment(km):
        """Get mileage segment for a car"""
        if pd.isna(km):
            return None
        if km <= 30000:
            return "0-30K"
        elif km <= 60000:
            return "30K-60K"
        elif km <= 90000:
            return "60K-90K"
        elif km <= 120000:
            return "90K-120K"
        else:
            return "120K+"


    def get_price_segment(price):
        """Get price segment for a car"""
        if pd.isna(price):
            return None
        if price <= 600000:
            return "0-600K"
        elif price <= 800000:
            return "600K-800K"
        elif price <= 900000:
            return "800K-900K"
        elif price <= 1100000:
            return "900K-1.1M"
        elif price <= 1300000:
            return "1.1M-1.3M"
        elif price <= 1600000:
            return "1.3M-1.6M"
        elif price <= 2100000:
            return "1.6M-2.1M"
        else:
            return "2.1M+"


    def get_year_segment(year):
        """Get year segment for a car"""
        if pd.isna(year):
            return None
        if 2010 <= year <= 2016:
            return "2010-2016"
        elif 2017 <= year <= 2019:
            return "2017-2019"
        elif 2020 <= year <= 2021:
            return "2020-2021"
        elif 2022 <= year <= 2024:
            return "2022-2024"
        else:
            return None  # Outside defined ranges


    @st.cache_data(ttl=43200)  # Cache data for 12 hours (manual refresh available)
    def load_showroom_data():
        """Load all necessary data for showroom matching"""
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error(
                    "No credentials found. Please configure either Streamlit secrets or provide a service_account.json file.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        client = bigquery.Client(credentials=credentials)

        # Inventory query (same as tgr.py)
        inventory_query = """
        with publishing AS (
        SELECT sf_vehicle_name,
               publishing_state,
               DATE(published_at) AS publishing_date,
               DATE_TRUNC(DATE(published_at),week) AS publishing_week,
               DATE_TRUNC(DATE(published_at),month) AS publishing_month,
               days_on_app AS DOA,
               MAX(published_at) over (partition by sf_vehicle_name) AS max_publish_date
        FROM ajans_dealers.ajans_wholesale_to_retail_publishing_logs
        WHERE sf_vehicle_name NOT in ("C-32211","C-32203") 
        QUALIFY published_at = max_publish_date
        ),

        selling_opportunity AS (
        SELECT a.car_name , 
               DATE(b.opportunity_creation_datetime) AS sold_date
        FROM reporting.vehicle_acquisition_to_selling a 
        LEFT JOIN reporting.wholesale_selling_opportunity b 
        ON a.selling_opportunity_id= b.opportunity_id
        WHERE a.selling_opportunity_id is NOT NULL ),

        sold_in_showroom AS (
        SELECT DISTINCT sf_vehicle_name,
               MAX(CASE WHEN request_type = "Showroom" AND request_status = "Succeeded" THEN dealer_code END) over (Partition BY sf_vehicle_name,dealer_code) AS showroom_flag,
               MAX(CASE WHEN request_type = "Buy Now" AND request_status = "Succeeded" THEN dealer_code END) over (Partition BY sf_vehicle_name,dealer_code) AS purchased_dealer
        FROM ajans_dealers.dealer_requests 
        QUALIFY showroom_flag = purchased_dealer ),

        buy_now_requests AS (
        SELECT a.sf_vehicle_name,
               MAX(consignment_date) AS consignment_date,
               MAX(wholesale_vehicle_sold_date) AS wholesale_vehicle_sold_date,
               DATE(MAX(opportunity_sold_status_datetime)) AS sf_sold_date,
               COUNT(CASE WHEN request_type = "Buy Now" THEN vehicle_request_id END) AS Buy_now_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(vehicle_request_created_at) >= DATE(published_at) THEN vehicle_request_id END) AS Buy_now_requests_count_from_last_publishing,
               COUNT(CASE WHEN request_type = "Showroom" AND DATE(vehicle_request_created_at) >= DATE(published_at) THEN vehicle_request_id END) AS Showroom_requests_count_from_last_publishing,
               COUNT(CASE WHEN request_type = "Buy Now" AND visited_at is NOT NULL THEN vehicle_request_id END) AS Buy_now_visits_count,
               COUNT(CASE WHEN request_type = "Showroom" THEN vehicle_request_id END) AS showroom_requests_count,
               COUNT(CASE WHEN request_type = "Showroom" AND request_status = "Succeeded" THEN vehicle_request_id END) AS succ_showroom_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(published_at) = DATE(vehicle_request_created_at) THEN vehicle_request_id END) AS first_day_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(published_at) = DATE(vehicle_request_created_at) AND wholesale_vehicle_sold_date is NOT NULL THEN vehicle_request_id END) AS sold_from_first_day_requests,
               COUNT(CASE WHEN request_type = "Buy Now" AND ((DATE(vehicle_request_created_at) <= consignment_date) OR (consignment_date is NULL)) THEN vehicle_request_id END) AS flash_sale_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND ((DATE(vehicle_request_created_at) <= consignment_date) OR (consignment_date is NULL)) AND wholesale_vehicle_sold_date is NOT NULL THEN vehicle_request_id END) AS sold_in_flash_sale,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(vehicle_request_created_at) > consignment_date THEN vehicle_request_id END) AS consignment_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(vehicle_request_created_at) > consignment_date AND wholesale_vehicle_sold_date is NOT NULL THEN vehicle_request_id END) AS sold_in_consignment
        FROM ajans_dealers.dealer_requests a 
        LEFT JOIN ajans_dealers.ajans_wholesale_to_retail_publishing_logs  b ON a.sf_vehicle_name = b.sf_vehicle_name
        LEFT JOIN (SELECT DISTINCT car_name,
               MAX(DATE(log_date)) AS consignment_date
        FROM ajans_dealers.wholesale_vehicle_activity_logs 
        WHERE flash_sale_enabled_before = "True" AND flash_sale_enabled_after = "False"  GROUP BY 1 ) c ON a.sf_vehicle_name = c.car_name  
        GROUP BY 1 ),

        live_cars AS (
        SELECT sf_vehicle_name,
               type AS live_status
        FROM reporting.ajans_vehicle_history 
        WHERE date_key = current_date() ),

        car_info AS (
        with max_date AS (
        SELECT sf_vehicle_name,
               event_date AS max_publish_date,
               make,
               model,
               year,
               kilometers,
               CASE WHEN (discount_enabled = True OR flash_sale_enabled = TRUE ) THEN discounted_price ELSE buy_now_price END AS App_price,
               buy_now_price,
               row_number()over(PARTITION BY sf_vehicle_name ORDER BY event_date DESC) AS row_number
        FROM ajans_dealers.vehicle_activity )

        SELECT *
        FROM max_date WHERE row_number = 1 )

        SELECT publishing.sf_vehicle_name,
               publishing_state,
               DOA,
               make,
               model,
               year,
               kilometers,
               car_condition,
               sylndr_offer_price,
               App_price,
               publishing_date,
               Buy_now_requests_count,
               Buy_now_requests_count_from_last_publishing,
               showroom_requests_count,
               succ_showroom_requests_count,
               Buy_now_visits_count,
               median_asked_price,
               current_status,
               CASE WHEN median_retail_price is NOT NULL THEN (App_price - a.median_retail_price)/a.median_retail_price
                    ELSE (App_price - a.median_asked_price)/a.median_asked_price END AS STM,
               CASE WHEN median_retail_price is NOT NULL THEN (a.sylndr_offer_price - a.median_retail_price)/a.median_retail_price
                    ELSE (a.sylndr_offer_price - a.median_asked_price)/a.median_asked_price END AS ATM
        FROM publishing
        LEFT JOIN selling_opportunity ON publishing.sf_vehicle_name = selling_opportunity.car_name
        LEFT JOIN live_cars ON publishing.sf_vehicle_name = live_cars.sf_vehicle_name
        LEFT JOIN car_info ON publishing.sf_vehicle_name = car_info.sf_vehicle_name 
        LEFT JOIN buy_now_requests ON publishing.sf_vehicle_name = buy_now_requests.sf_vehicle_name
        LEFT JOIN reporting.vehicle_acquisition_to_selling a ON publishing.sf_vehicle_name = a.car_name
        LEFT JOIN sold_in_showroom ON publishing.sf_vehicle_name = sold_in_showroom.sf_vehicle_name
        WHERE allocation_category = "Wholesale" AND current_status in ("Published" , "Being Sold")
        """

        # Showroom performance query
        showroom_performance_query = """
        with base AS (
        SELECT a.sf_vehicle_name,
               a.dealer_code,
               a.dealer_name,
               publishing_date,
               DATE_TRUNC(publishing_date,month) AS publish_month,
               DOA,
               vehicle_request_id,
               DATE(vehicle_request_created_at) AS request_date,
               CASE WHEN succeeded_at is NOT NULL OR inprogress_at is NOT NULL THEN dealer_code END AS showroom_dealer,
               CASE WHEN succeeded_at is NOT NULL THEN DATE(succeeded_at) 
                    WHEN inprogress_at is NOT NULL THEN DATE(inprogress_at) ELSE NULL END AS succ_date,
               purchased_dealer,
               sold_date,
               MAX( CASE WHEN succeeded_at is NOT NULL THEN DATE(succeeded_at) 
                    WHEN inprogress_at is NOT NULL THEN DATE(inprogress_at) END) over (PARTITION BY dealer_code) AS last_succ_requests
        FROM   ajans_dealers.dealer_showroom_requests a 
        LEFT JOIN ( 
        SELECT DISTINCT sf_vehicle_name,
               CASE WHEN wholesale_vehicle_sold_date is NOT NULL THEN dealer_code END AS purchased_dealer,
               DATE(wholesale_vehicle_sold_date) AS sold_date 
        FROM ajans_dealers.dealer_requests 
        WHERE dealer_code != "D-0200" AND wholesale_vehicle_sold_date is NOT NULL ) b ON a.sf_vehicle_name = b.sf_vehicle_name

        LEFT JOIN (
               SELECT sf_vehicle_name,
               publishing_state,
               DATE(min_published) AS publishing_date,
               CASE WHEN DATE(max_unpublished) is NULL THEN current_date() ELSE DATE(max_unpublished) END AS unpublish_date,
               minutes_published_for/1440 AS DOA,
               MAX(min_published) over (partition by sf_vehicle_name) AS max_publish_date
        FROM ajans_dealers.ajans_wholesale_to_retail_publishing_logs 
        QUALIFY min_published = max_publish_date ) c ON a.sf_vehicle_name = c.sf_vehicle_name

        WHERE a.sf_vehicle_name NOT in ("C-32211","C-32203") AND publishing_state = "Wholesale" AND DATE(vehicle_request_created_at) between publishing_date AND unpublish_date
        )
        SELECT base.dealer_code,
               base.dealer_name,
               request_date,
               DATE_TRUNC(DATE(request_date),month ) AS request_month,
               DATE_DIFF(CURRENT_DATE(), last_succ_requests, DAY) AS days_from_last_request,
               COUNT(vehicle_request_id) AS showroom_requests_count,
               COUNT(showroom_dealer) AS succ_showroom_requests,
               SUM(CASE WHEN showroom_dealer = purchased_dealer THEN 1 ELSE 0 END) AS sold_in_showroom,
               AVG(CASE WHEN showroom_dealer = purchased_dealer THEN DATE_DIFF(sold_date, succ_date, day) END) AS days_to_sold_in_showroom
        FROM base 
        WHERE EXTRACT(YEAR FROM request_date) = 2025
        GROUP BY 1,2,3,4,5
        """

        # Historical purchases query (full version from pipeline_matcher)
        historical_query = """
        WITH s AS (
            SELECT DISTINCT sf_vehicle_name, 
                   DATE(wholesale_vehicle_sold_date) AS request_date, 
                   dealer_code,
                   dealer_name, 
                   dealer_phone,
                   car_name,
                   CASE 
                       WHEN discount_enabled IS TRUE THEN discounted_price 
                       ELSE buy_now_price 
                   END AS price
            FROM `pricing-338819.ajans_dealers.dealer_requests`
            WHERE request_type = 'Buy Now' 
              AND wholesale_vehicle_sold_date IS NOT NULL
              AND DATE(wholesale_vehicle_sold_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
        ), p as (
            SELECT sf_vehicle_name, published_at, days_on_app
            FROM `pricing-338819.ajans_dealers.ajans_wholesale_to_retail_publishing_logs`
        ),
        cost as (
            SELECT DISTINCT car_name, sylndr_acquisition_price, market_retail_price, median_asked_price, refurbishment_cost 
            FROM `pricing-338819.reporting.daily_car_status`
        )

        SELECT s.request_date, s.dealer_code, s.dealer_name, s.dealer_phone, 
               round(p.days_on_app) as time_on_app, s.price, c.make, c.model, c.year, c.kilometers,
               cost.sylndr_acquisition_price, cost.market_retail_price
        FROM s 
        LEFT JOIN (
            SELECT DISTINCT sf_vehicle_name, make, model, year, kilometers 
            FROM `pricing-338819.reporting.ajans_vehicle_history`
        ) AS c 
        ON s.sf_vehicle_name = c.sf_vehicle_name
        LEFT JOIN cost on s.car_name = cost.car_name
        LEFT JOIN p ON s.sf_vehicle_name = p.sf_vehicle_name
        WHERE c.make IS NOT NULL
        """

        # Recent views query
        recent_views_query = """
        SELECT 
            s.time,
            s.make,
            s.model,
            s.trim,
            s.year,
            s.kilometrage,
            s.transmission,
            s.listing_title,
            s.buy_now_price,
            s.body_style,
            s.c_name,
            s.entity_code as dealer_code,
            du.dealer_user_phone as dealer_user_phone
        FROM `pricing-338819.silver_ajans_mixpanel.screen_car_profile_event` s
        LEFT JOIN ajans_dealers.dealer_users du ON s.user_code = du.dealer_user_code
        WHERE DATE(s.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND s.entity_code IS NOT NULL
        ORDER BY s.time DESC
        """

        # Recent filters query
        recent_filters_query = """
        SELECT 
            time,
            make,
            model,
            year,
            kilometrage,
            group_filter,
            status,
            no_of_cars,
            entity_code as dealer_code
        FROM `pricing-338819.silver_ajans_mixpanel.action_filter`
        WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND entity_code IS NOT NULL
        ORDER BY time DESC
        """

        # Dealer requests query (unpurchased requests)
        dealer_requests_query = """
        SELECT 
            vehicle_request_created_at,
            dealer_code,
            dealer_name,
            dealer_phone,
            request_type,
            car_make,
            car_model,
            car_year,
            car_kilometrage,
            buy_now_price,
            request_status,
            visited_at,
            sf_vehicle_name
        FROM `pricing-338819.ajans_dealers.dealer_requests`
        WHERE wholesale_vehicle_sold_date IS NULL
        AND DATE(vehicle_request_created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND dealer_code IS NOT NULL
        ORDER BY vehicle_request_created_at DESC
        """

        # OLX listings for all dealers
        olx_query = """
        WITH cleaned_numbers AS (
            SELECT
                DISTINCT seller_name,
                REGEXP_REPLACE(seller_phone_number, r'[^0-9,]', '') AS cleaned_phone_number,
                id,
                title,
                transmission_type,
                year,
                kilometers,
                make,
                model,
                payment_options,
                condition,
                engine_capacity,
                extra_features,
                color,
                body_type,
                ad_type,
                fuel_type,
                description,
                images,
                region,
                price,
                is_active,
                added_at,
                deactivated_at,
                is_dealer,
                created_at
            FROM olx.listings
            WHERE added_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ),
        split_numbers AS (
            SELECT
                *,
                SPLIT(cleaned_phone_number, ',') AS phone_numbers
            FROM cleaned_numbers
        ),
        flattened_numbers AS (
            SELECT
                DISTINCT
                id,
                title,
                transmission_type,
                year,
                kilometers,
                make,
                model,
                payment_options,
                condition,
                engine_capacity,
                extra_features,
                color,
                body_type,
                ad_type,
                fuel_type,
                description,
                images,
                region,
                price,
                seller_name,
                is_active,
                added_at,
                deactivated_at,
                is_dealer,
                created_at,
                SUBSTR(phone_number, 2) AS phone_number
            FROM split_numbers,
            UNNEST(phone_numbers) AS phone_number
        )

        SELECT 
            f.make,
            f.model,
            f.year,
            f.kilometers,
            f.price,
            f.added_at,
            d.dealer_name,
            d.dealer_code,
            d.dealer_status,
            d.dealer_email,
            d.branch_city,
            d.dealer_account_manager_name,
            d.dealer_account_manager_email
        FROM flattened_numbers f
        INNER JOIN gold_wholesale.dim_dealers d
        ON f.phone_number = d.dealer_phone
        WHERE f.make IS NOT NULL
        ORDER BY added_at DESC
        """

        # Car location query
        location_query = """
        SELECT 
            car_name,
            location_stage_name
        FROM reporting.daily_car_status 
        WHERE vehicle_allocation_category = "Wholesale"
        AND date_key = CURRENT_DATE()
        """

        # Current showroom requests query
        current_showroom_query = """
        SELECT dealer_code,
               dealer_name,
               car_name,
               car_make,
               car_year,
               car_kilometrage,
               request_status,
               days_on_hand
        FROM ajans_dealers.dealer_showroom_requests 
        WHERE request_status IN ("Received", "Queued", "Inprogress", "Contacted","Being Displayed")
        """

        # Queue position query
        queue_position_query = """
        SELECT sf_vehicle_name,
               dealer_code,
               dealer_name,
               row_number() OVER (PARTITION BY sf_vehicle_name ORDER BY vehicle_request_created_at) AS queue_position
        FROM ajans_dealers.dealer_showroom_requests 
        WHERE request_status = "Queued"
        """

        # Cars being displayed count query
        displayed_cars_query = """
        SELECT dealer_code,
               dealer_name,
               COUNT(*) AS cars_displayed_count
        FROM ajans_dealers.dealer_showroom_requests 
        WHERE request_status = "Being Displayed"
        GROUP BY dealer_code, dealer_name
        """

        # Consignment available cars query
        consignment_cars_query = """
        SELECT a.sf_vehicle_name,
               make,
               model,
               CASE WHEN flash_sale_enabled is TRUE THEN "Flash sale" ELSE "Consignment" END AS flash_sale_flag
        FROM ajans_dealers.vehicle_activity a 
        LEFT JOIN (
            SELECT sf_vehicle_name,
                   request_status
            FROM ajans_dealers.dealer_requests 
            WHERE request_status in ("Succeeded" , "Payment Log") AND request_type = "Buy Now" 
        ) b ON a.sf_vehicle_name = b.sf_vehicle_name
        WHERE event_date = current_date() AND request_status is NULL 
        """

        # Discount eligibility query
        discount_eligibility_query = """
        SELECT sf_vehicle_name,
               showroom_displayed_count,
               days_in_consignment,
               discount_eligibility_flag,
               car_status
        FROM wholesale_test.showroom_discount_eligibility
        """

        # Discount pricing query
        discount_pricing_query = """
        SELECT c_code,
               flash_price,
               consignment_price,
               speed_discount_price
        FROM wholesale_test.showroom_discount
        """

        print("Executing inventory_query...")
        inventory_df = client.query(inventory_query).to_dataframe()
        print("✓ inventory_query completed successfully")

        print("Executing showroom_performance_query...")
        showroom_performance_df = client.query(showroom_performance_query).to_dataframe()
        print("✓ showroom_performance_query completed successfully")

        print("Executing historical_query...")
        historical_df = client.query(historical_query).to_dataframe()
        print("✓ historical_query completed successfully")

        print("Executing recent_views_query...")
        recent_views_df = client.query(recent_views_query).to_dataframe()
        print("✓ recent_views_query completed successfully")

        print("Executing recent_filters_query...")
        recent_filters_df = client.query(recent_filters_query).to_dataframe()
        print("✓ recent_filters_query completed successfully")

        print("Executing dealer_requests_query...")
        dealer_requests_df = client.query(dealer_requests_query).to_dataframe()
        print("✓ dealer_requests_query completed successfully")

        print("Executing olx_query...")
        olx_df = client.query(olx_query).to_dataframe()
        print("✓ olx_query completed successfully")

        print("Executing location_query...")
        location_df = client.query(location_query).to_dataframe()
        print("✓ location_query completed successfully")

        print("Executing current_showroom_query...")
        current_showroom_df = client.query(current_showroom_query).to_dataframe()
        print("✓ current_showroom_query completed successfully")

        print("Executing queue_position_query...")
        queue_position_df = client.query(queue_position_query).to_dataframe()
        print("✓ queue_position_query completed successfully")

        print("Executing displayed_cars_query...")
        displayed_cars_df = client.query(displayed_cars_query).to_dataframe()
        print("✓ displayed_cars_query completed successfully")

        print("Executing consignment_cars_query...")
        consignment_cars_df = client.query(consignment_cars_query).to_dataframe()
        print("✓ consignment_cars_query completed successfully")

        print("Executing discount_eligibility_query...")
        discount_eligibility_df = client.query(discount_eligibility_query).to_dataframe()
        print("✓ discount_eligibility_query completed successfully")

        print("Executing discount_pricing_query...")
        discount_pricing_df = client.query(discount_pricing_query).to_dataframe()
        print("✓ discount_pricing_query completed successfully")

        # Data preprocessing
        if not inventory_df.empty:
            inventory_df['publishing_date'] = pd.to_datetime(inventory_df['publishing_date'])
            # Convert numeric columns
            inventory_numeric_columns = ['year', 'kilometers', 'DOA', 'App_price', 'sylndr_offer_price',
                                         'Buy_now_requests_count', 'Buy_now_requests_count_from_last_publishing',
                                         'showroom_requests_count', 'succ_showroom_requests_count',
                                         'Buy_now_visits_count', 'median_asked_price', 'STM', 'ATM']
            for col in inventory_numeric_columns:
                if col in inventory_df.columns:
                    inventory_df[col] = pd.to_numeric(inventory_df[col], errors='coerce')

        if not showroom_performance_df.empty:
            showroom_performance_df['request_date'] = pd.to_datetime(showroom_performance_df['request_date'])
            # Convert numeric columns
            showroom_numeric_columns = ['days_from_last_request', 'showroom_requests_count',
                                        'succ_showroom_requests', 'sold_in_showroom', 'days_to_sold_in_showroom']
            for col in showroom_numeric_columns:
                if col in showroom_performance_df.columns:
                    showroom_performance_df[col] = pd.to_numeric(showroom_performance_df[col], errors='coerce')

        if not historical_df.empty:
            historical_df['request_date'] = pd.to_datetime(historical_df['request_date'])
            # Convert numeric columns
            numeric_columns = ['time_on_app', 'price', 'year', 'kilometers', 'sylndr_acquisition_price',
                               'market_retail_price']
            for col in numeric_columns:
                if col in historical_df.columns:
                    historical_df[col] = pd.to_numeric(historical_df[col], errors='coerce')

        if not recent_views_df.empty:
            recent_views_df['time'] = pd.to_datetime(recent_views_df['time'])

        if not recent_filters_df.empty:
            recent_filters_df['time'] = pd.to_datetime(recent_filters_df['time'])

        if not dealer_requests_df.empty:
            dealer_requests_df['vehicle_request_created_at'] = pd.to_datetime(
                dealer_requests_df['vehicle_request_created_at'])
            dealer_requests_df['visited_at'] = pd.to_datetime(dealer_requests_df['visited_at'])
            # Convert numeric columns in dealer requests data
            requests_numeric_columns = ['car_year', 'car_kilometrage', 'buy_now_price']
            for col in requests_numeric_columns:
                if col in dealer_requests_df.columns:
                    dealer_requests_df[col] = pd.to_numeric(dealer_requests_df[col], errors='coerce')

        if not olx_df.empty:
            olx_df['added_at'] = pd.to_datetime(olx_df['added_at'])
            # Convert numeric columns in OLX data
            olx_numeric_columns = ['year', 'kilometers', 'price']
            for col in olx_numeric_columns:
                if col in olx_df.columns:
                    olx_df[col] = pd.to_numeric(olx_df[col], errors='coerce')

        if not current_showroom_df.empty:
            # Convert numeric columns in current showroom data
            showroom_numeric_columns = ['car_year', 'car_kilometrage', 'days_on_hand']
            for col in showroom_numeric_columns:
                if col in current_showroom_df.columns:
                    current_showroom_df[col] = pd.to_numeric(current_showroom_df[col], errors='coerce')

        return inventory_df, showroom_performance_df, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df, location_df, current_showroom_df, queue_position_df, displayed_cars_df, consignment_cars_df, discount_eligibility_df, discount_pricing_df


    def calculate_showroom_score(dealer_code, showroom_performance_df):
        """Calculate showroom performance score for a dealer (40 points total)"""

        dealer_data = showroom_performance_df[showroom_performance_df['dealer_code'] == dealer_code]

        if dealer_data.empty:
            return 0, {}

        dealer_row = dealer_data.iloc[0]
        total_score = 0
        score_breakdown = {}

        # Showroom selling rate (20 points) - (sold_in_showroom / succ_showroom_requests)
        succ_showroom_requests = dealer_row.get('succ_showroom_requests', 0)
        sold_in_showroom = dealer_row.get('sold_in_showroom', 0)

        if succ_showroom_requests > 0:
            selling_rate = (sold_in_showroom / succ_showroom_requests) * 100

            if selling_rate > 20:
                selling_rate_score = 20
            elif selling_rate >= 15:
                selling_rate_score = 15
            elif selling_rate >= 10:
                selling_rate_score = 10
            elif selling_rate >= 1:
                selling_rate_score = 5
            else:
                selling_rate_score = 0
        else:
            selling_rate = 0
            selling_rate_score = 0

        score_breakdown['Selling Rate'] = selling_rate_score
        total_score += selling_rate_score

        # Days to sell in showroom (10 points) - (days_to_sold_in_showroom)
        days_to_sell = dealer_row.get('days_to_sold_in_showroom', None)
        if pd.notna(days_to_sell) and days_to_sell > 0:
            if days_to_sell <= 7:
                days_score = 10
            elif days_to_sell <= 10:
                days_score = 7
            elif days_to_sell <= 15:
                days_score = 5
            else:
                days_score = 0
        else:
            days_score = 0

        score_breakdown['Days to Sell'] = days_score
        total_score += days_score

        # Days from last request (10 points) - (days_from_last_request)
        # More days = more points (less recent activity = higher availability)
        days_from_last = dealer_row.get('days_from_last_request', None)
        if pd.notna(days_from_last):
            if days_from_last > 14:
                days_from_last_score = 10
            else:
                days_from_last_score = 5
        else:
            days_from_last_score = 0

        score_breakdown['Days from Last Request'] = days_from_last_score
        total_score += days_from_last_score

        return total_score, score_breakdown


    def calculate_inventory_match_score(car, dealer_code, historical_df, recent_views_df, recent_filters_df,
                                        dealer_requests_df, olx_df):
        """Calculate how well an inventory car matches a dealer's preferences (full version from pipeline_matcher)"""

        total_score = 0
        score_breakdown = {}

        # Get dealer data
        dealer_historical = historical_df[historical_df['dealer_code'] == dealer_code]
        dealer_views = recent_views_df[recent_views_df['dealer_code'] == dealer_code]
        dealer_filters = recent_filters_df[recent_filters_df['dealer_code'] == dealer_code]
        dealer_requests = dealer_requests_df[dealer_requests_df['dealer_code'] == dealer_code]
        dealer_olx = olx_df[olx_df['dealer_code'] == dealer_code]

        # 1. Historical Purchase Patterns (40 points total)
        historical_score = 0
        if not dealer_historical.empty:
            # Check for exact model matches first
            model_purchases = dealer_historical[
                (dealer_historical['make'] == car['make']) &
                (dealer_historical['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_purchases = dealer_historical[dealer_historical['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_purchases = pd.DataFrame()
            if car_group != 'Other':
                group_purchases = dealer_historical[
                    dealer_historical['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not model_purchases.empty:
                # Exact Model Match (15 points max)
                model_frequency = len(model_purchases) / len(dealer_historical)
                model_score = min(15, model_frequency * 40)
                historical_score += model_score

                # Also gets make score (10 points max) since model implies make interest
                make_frequency = len(make_purchases) / len(dealer_historical)
                make_score = min(10, make_frequency * 25)
                historical_score += make_score

                # Also gets origin group score (6 points max) since model implies group interest
                if not group_purchases.empty:
                    group_frequency = len(group_purchases) / len(dealer_historical)
                    group_score = min(6, group_frequency * 15)
                    historical_score += group_score

            elif not make_purchases.empty:
                # Make match but no exact model - gets make + origin scores
                make_frequency = len(make_purchases) / len(dealer_historical)
                make_score = min(10, make_frequency * 25)
                historical_score += make_score

                # Also gets origin group score since make implies group interest
                if not group_purchases.empty:
                    group_frequency = len(group_purchases) / len(dealer_historical)
                    group_score = min(6, group_frequency * 15)
                    historical_score += group_score

            elif not group_purchases.empty:
                # Only origin group match - gets origin score only
                group_frequency = len(group_purchases) / len(dealer_historical)
                group_score = min(6, group_frequency * 15)
                historical_score += group_score

            # Similar Price Range (4 points max) - segment-based scoring
            car_price_segment = get_price_segment(car['App_price'])
            if dealer_historical['price'].notna().any() and car_price_segment is not None:
                # Check if dealer has bought cars in the same price segment
                dealer_price_segments = dealer_historical['price'].apply(get_price_segment)
                price_segment_purchases = dealer_price_segments[dealer_price_segments == car_price_segment]
                if not price_segment_purchases.empty:
                    price_segment_frequency = len(price_segment_purchases) / len(dealer_historical)
                    price_segment_score = min(4, price_segment_frequency * 12)
                    historical_score += price_segment_score

            # Year Segment preference (3 points max) - segment-based scoring
            car_year_segment = get_year_segment(car['year'])
            if dealer_historical['year'].notna().any() and car_year_segment is not None:
                # Check if dealer has bought cars in the same year segment
                dealer_year_segments = dealer_historical['year'].apply(get_year_segment)
                year_segment_purchases = dealer_year_segments[dealer_year_segments == car_year_segment]
                if not year_segment_purchases.empty:
                    # Full score if dealer has bought cars in this year segment
                    historical_score += 3

            # Mileage Segment preference (2 points max) - segment-based scoring
            car_mileage_segment = get_mileage_segment(car['kilometers'])
            if dealer_historical['kilometers'].notna().any() and car_mileage_segment is not None:
                # Check if dealer has bought cars in the same mileage segment
                dealer_mileage_segments = dealer_historical['kilometers'].apply(get_mileage_segment)
                mileage_segment_purchases = dealer_mileage_segments[dealer_mileage_segments == car_mileage_segment]
                if not mileage_segment_purchases.empty:
                    # Full score if dealer has bought cars in this mileage segment
                    historical_score += 2

        score_breakdown['Historical Purchases'] = historical_score
        total_score += historical_score

        # 2. Recent App Activity (40 points total) - enhanced with additive scoring
        activity_score = 0

        # Recent dealer requests with enhanced matching (20 points max) - Higher priority
        if not dealer_requests.empty:
            # Check for exact model matches first
            exact_model_requests = dealer_requests[
                (dealer_requests['car_make'] == car['make']) &
                (dealer_requests['car_model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_requests = dealer_requests[dealer_requests['car_make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_requests = pd.DataFrame()
            if car_group != 'Other':
                group_requests = dealer_requests[
                    dealer_requests['car_make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_requests.empty:
                # Exact Model Requests (12 points max)
                model_request_score = min(12, len(exact_model_requests) * 4)
                activity_score += model_request_score

                # Also gets make score (8 points max) since model implies make interest
                make_request_score = min(8, len(make_requests) * 1.5)
                activity_score += make_request_score

                # Also gets origin group score (4 points max) since model implies group interest
                if not group_requests.empty:
                    group_request_score = min(4, len(group_requests) * 0.8)
                    activity_score += group_request_score

            elif not make_requests.empty:
                # Make match but no exact model - gets make + origin scores
                make_request_score = min(8, len(make_requests) * 1.5)
                activity_score += make_request_score

                # Also gets origin group score since make implies group interest
                if not group_requests.empty:
                    group_request_score = min(4, len(group_requests) * 0.8)
                    activity_score += group_request_score

            elif not group_requests.empty:
                # Only origin group match - gets origin score only
                group_request_score = min(4, len(group_requests) * 0.8)
                activity_score += group_request_score

        # Recent views with enhanced matching (13 points max) - Lower priority
        if not dealer_views.empty:
            # Check for exact model matches first
            exact_model_views = dealer_views[
                (dealer_views['make'] == car['make']) &
                (dealer_views['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_views = dealer_views[dealer_views['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_views = pd.DataFrame()
            if car_group != 'Other':
                group_views = dealer_views[
                    dealer_views['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_views.empty:
                # Exact Model Views (8 points max)
                model_view_score = min(8, len(exact_model_views) * 6)
                activity_score += model_view_score

                # Also gets make score (5 points max) since model implies make interest
                make_view_score = min(5, len(make_views) * 2)
                activity_score += make_view_score

                # Also gets origin group score (2 points max) since model implies group interest
                if not group_views.empty:
                    group_view_score = min(2, len(group_views) * 1)
                    activity_score += group_view_score

            elif not make_views.empty:
                # Make match but no exact model - gets make + origin scores
                make_view_score = min(5, len(make_views) * 2)
                activity_score += make_view_score

                # Also gets origin group score since make implies group interest
                if not group_views.empty:
                    group_view_score = min(2, len(group_views) * 1)
                    activity_score += group_view_score

            elif not group_views.empty:
                # Only origin group match - gets origin score only
                group_view_score = min(2, len(group_views) * 1)
                activity_score += group_view_score

        # Recent filters with enhanced matching (7 points max)
        if not dealer_filters.empty:
            # Check for exact model matches first
            exact_model_filters = dealer_filters[
                (dealer_filters['make'] == car['make']) &
                (dealer_filters['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_filters = dealer_filters[dealer_filters['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_filters = pd.DataFrame()
            if car_group != 'Other':
                group_filters = dealer_filters[
                    dealer_filters['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_filters.empty:
                # Exact Model Filters (4 points max)
                model_filter_score = min(4, len(exact_model_filters) * 2)
                activity_score += model_filter_score

                # Also gets make score (3 points max) since model implies make interest
                make_filter_score = min(3, len(make_filters) * 1)
                activity_score += make_filter_score

                # Also gets origin group score (1 point max) since model implies group interest
                if not group_filters.empty:
                    group_filter_score = min(1, len(group_filters) * 0.5)
                    activity_score += group_filter_score

            elif not make_filters.empty:
                # Make match but no exact model - gets make + origin scores
                make_filter_score = min(3, len(make_filters) * 1)
                activity_score += make_filter_score

                # Also gets origin group score since make implies group interest
                if not group_filters.empty:
                    group_filter_score = min(1, len(group_filters) * 0.5)
                    activity_score += group_filter_score

            elif not group_filters.empty:
                # Only origin group match - gets origin score only
                group_filter_score = min(1, len(group_filters) * 0.5)
                activity_score += group_filter_score

        score_breakdown['Recent Activity'] = activity_score
        total_score += activity_score

        # 3. OLX Listings (40 points total) - updated to match Historical Purchase Patterns structure
        olx_score = 0
        if not dealer_olx.empty:
            # Check for exact model matches first
            exact_model_olx = dealer_olx[
                (dealer_olx['make'] == car['make']) &
                (dealer_olx['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_olx = dealer_olx[dealer_olx['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_olx = pd.DataFrame()
            if car_group != 'Other':
                group_olx = dealer_olx[
                    dealer_olx['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_olx.empty:
                # Exact Model Match (15 points max)
                model_frequency = len(exact_model_olx) / len(dealer_olx)
                model_score = min(15, model_frequency * 40)
                olx_score += model_score

                # Also gets make score (10 points max) since model implies make interest
                make_frequency = len(make_olx) / len(dealer_olx)
                make_score = min(10, make_frequency * 25)
                olx_score += make_score

                # Also gets origin group score (6 points max) since model implies group interest
                if not group_olx.empty:
                    group_frequency = len(group_olx) / len(dealer_olx)
                    group_score = min(6, group_frequency * 15)
                    olx_score += group_score

            elif not make_olx.empty:
                # Make match but no exact model - gets make + origin scores
                make_frequency = len(make_olx) / len(dealer_olx)
                make_score = min(10, make_frequency * 25)
                olx_score += make_score

                # Also gets origin group score since make implies group interest
                if not group_olx.empty:
                    group_frequency = len(group_olx) / len(dealer_olx)
                    group_score = min(6, group_frequency * 15)
                    olx_score += group_score

            elif not group_olx.empty:
                # Only origin group match - gets origin score only
                group_frequency = len(group_olx) / len(dealer_olx)
                group_score = min(6, group_frequency * 15)
                olx_score += group_score

            # Similar Price Range (4 points max) - segment-based scoring
            car_price_segment = get_price_segment(car['App_price'])
            if dealer_olx['price'].notna().any() and car_price_segment is not None:
                # Check if dealer has OLX listings in the same price segment
                dealer_price_segments = dealer_olx['price'].apply(get_price_segment)
                price_segment_listings = dealer_price_segments[dealer_price_segments == car_price_segment]
                if not price_segment_listings.empty:
                    price_segment_frequency = len(price_segment_listings) / len(dealer_olx)
                    price_segment_score = min(4, price_segment_frequency * 12)
                    olx_score += price_segment_score

            # Year Segment preference (3 points max) - segment-based scoring
            car_year_segment = get_year_segment(car['year'])
            if dealer_olx['year'].notna().any() and car_year_segment is not None:
                # Check if dealer has OLX listings in the same year segment
                dealer_year_segments = dealer_olx['year'].apply(get_year_segment)
                year_segment_listings = dealer_year_segments[dealer_year_segments == car_year_segment]
                if not year_segment_listings.empty:
                    # Full score if dealer has listings in this year segment
                    olx_score += 3

            # Mileage Segment preference (2 points max) - segment-based scoring
            car_mileage_segment = get_mileage_segment(car['kilometers'])
            if dealer_olx['kilometers'].notna().any() and car_mileage_segment is not None:
                # Check if dealer has OLX listings in the same mileage segment
                dealer_mileage_segments = dealer_olx['kilometers'].apply(get_mileage_segment)
                mileage_segment_listings = dealer_mileage_segments[dealer_mileage_segments == car_mileage_segment]
                if not mileage_segment_listings.empty:
                    # Full score if dealer has listings in this mileage segment
                    olx_score += 2

        score_breakdown['OLX Listings'] = olx_score
        total_score += olx_score

        return total_score, score_breakdown


    def generate_showroom_matches(inventory_df, showroom_performance_df, historical_df, recent_views_df,
                                  recent_filters_df, dealer_requests_df, olx_df, location_df, current_showroom_df,
                                  queue_position_df, displayed_cars_df, consignment_cars_df, discount_eligibility_df,
                                  discount_pricing_df):
        """Generate showroom matches for live cars only"""

        matches = []

        # Get all unique dealers from showroom performance data, excluding D-0200
        all_dealers = showroom_performance_df['dealer_code'].unique() if not showroom_performance_df.empty else []
        # Exclude dealer code D-0200
        all_dealers = [dealer for dealer in all_dealers if dealer != 'D-0200']

        # Filter for only live cars (same logic as Live Cars tab)
        live_cars = inventory_df.copy()

        # Apply the same filters as Live Cars tab to ensure consistency
        # Only include cars that are currently published and available
        if 'current_status' in live_cars.columns:
            live_cars = live_cars[live_cars['current_status'].isin(['Published', 'Being Sold'])]

        # Only include cars with valid publishing state
        if 'publishing_state' in live_cars.columns:
            live_cars = live_cars[live_cars['publishing_state'] == 'Wholesale']

        # Process each live car
        for _, car in live_cars.iterrows():
            # Check if car has valid data
            if (pd.notna(car['make']) and pd.notna(car['model']) and
                    pd.notna(car['year']) and pd.notna(car['kilometers']) and
                    pd.notna(car['App_price'])):

                # Get car location
                car_location = "Unknown"
                if not location_df.empty:
                    location_info = location_df[location_df['car_name'] == car['sf_vehicle_name']]
                    if not location_info.empty:
                        car_location = location_info['location_stage_name'].iloc[0]

                # Calculate car traction based on queue count
                queue_count = 0
                if not queue_position_df.empty:
                    car_queue_count = len(
                        queue_position_df[queue_position_df['sf_vehicle_name'] == car['sf_vehicle_name']])
                    queue_count = car_queue_count

                # Determine traction level
                if queue_count >= 3:
                    car_traction = "High"
                elif queue_count > 0:
                    car_traction = "Low"
                else:
                    car_traction = "No"

                # Get discount eligibility information
                is_discounted = False
                discount_price = None
                regular_price = car.get('App_price', 0)

                if not discount_eligibility_df.empty:
                    eligibility_info = discount_eligibility_df[
                        discount_eligibility_df['sf_vehicle_name'] == car['sf_vehicle_name']]
                    if not eligibility_info.empty:
                        eligibility_row = eligibility_info.iloc[0]
                        showroom_displayed_count = eligibility_row.get('showroom_displayed_count', 999)
                        days_in_consignment = eligibility_row.get('days_in_consignment', -1)

                        # Check if car is eligible for discount based on criteria
                        if (showroom_displayed_count < 2 and
                                days_in_consignment >= 0 and days_in_consignment <= 14):
                            is_discounted = True

                            # Get discount price from pricing table
                            if not discount_pricing_df.empty:
                                pricing_info = discount_pricing_df[
                                    discount_pricing_df['c_code'] == car['sf_vehicle_name']]
                                if not pricing_info.empty:
                                    pricing_row = pricing_info.iloc[0]
                                    speed_discount_price = pricing_row.get('speed_discount_price')
                                    consignment_price = pricing_row.get('consignment_price')

                                    # Use speed_discount_price if discounted, otherwise consignment_price
                                    if pd.notna(speed_discount_price):
                                        discount_price = speed_discount_price
                                    elif pd.notna(consignment_price):
                                        discount_price = consignment_price

                # Determine final price to display
                final_price = discount_price if is_discounted and discount_price is not None else regular_price

                # Calculate scores for each dealer
                for dealer_code in all_dealers:
                    if pd.isna(dealer_code):
                        continue

                    # Get dealer name
                    dealer_info = showroom_performance_df[showroom_performance_df['dealer_code'] == dealer_code]
                    dealer_name = dealer_info['dealer_name'].iloc[0] if not dealer_info.empty else "Unknown"

                    # Check if dealer has requested this car
                    requested = "No"
                    days_in_location = 0
                    queue_position = None
                    if not current_showroom_df.empty:
                        dealer_request = current_showroom_df[
                            (current_showroom_df['dealer_code'] == dealer_code) &
                            (current_showroom_df['car_name'] == car['sf_vehicle_name'])
                            ]
                        if not dealer_request.empty:
                            requested = "Yes"
                            days_on_hand = dealer_request['days_on_hand'].iloc[0]
                            days_in_location = days_on_hand if pd.notna(days_on_hand) else 0

                    # Get queue position if dealer is in queue
                    if not queue_position_df.empty:
                        queue_info = queue_position_df[
                            (queue_position_df['dealer_code'] == dealer_code) &
                            (queue_position_df['sf_vehicle_name'] == car['sf_vehicle_name'])
                            ]
                        if not queue_info.empty:
                            queue_position = queue_info['queue_position'].iloc[0]

                    # Get count of cars being displayed for this dealer
                    cars_displayed = 0
                    if not displayed_cars_df.empty:
                        displayed_info = displayed_cars_df[displayed_cars_df['dealer_code'] == dealer_code]
                        if not displayed_info.empty:
                            cars_displayed = displayed_info['cars_displayed_count'].iloc[0]

                    # Calculate match score
                    match_score, match_breakdown = calculate_inventory_match_score(
                        car, dealer_code, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df
                    )

                    # Calculate showroom score
                    showroom_score, showroom_breakdown = calculate_showroom_score(
                        dealer_code, showroom_performance_df
                    )

                    # Only include if there's some relevance
                    if match_score > 10 or showroom_score > 10:
                        matches.append({
                            'car_code': car['sf_vehicle_name'],
                            'price': car['App_price'],
                            'final_price': final_price,
                            'is_discounted': is_discounted,
                            'car_traction': car_traction,
                            'queue_count': queue_count,
                            'model': car['model'],
                            'make': car['make'],
                            'year': car['year'],
                            'kilometers': car['kilometers'],
                            'location': car_location,
                            'doa': car.get('DOA', 0),
                            'requests_count': car.get('Buy_now_requests_count', 0),
                            'requested': requested,
                            'days_in_location': days_in_location,
                            'queue_position': queue_position,
                            'cars_displayed': cars_displayed,
                            'dealer_name': dealer_name,
                            'dealer_code': dealer_code,
                            'match_score': match_score,
                            'showroom_score': showroom_score,
                            'total_score': match_score + showroom_score,
                            'match_breakdown': match_breakdown,
                            'showroom_breakdown': showroom_breakdown,
                            'car_group': get_car_group(car['make']),
                            'price_segment': get_price_segment(car['App_price']),
                            'year_segment': get_year_segment(car['year']),
                            'mileage_segment': get_mileage_segment(car['kilometers'])
                        })

        return matches


    def show_methodology():
        """Display methodology explanation in an expander"""
        with st.expander("🔍 **How the Showroom Matching Algorithm Works**", expanded=False):
            st.markdown("""
            ### 📊 **Scoring Methodology (Total Points: Match Score + Showroom Score)**

            #### **🚗 Live Cars Matching**
            **Only currently live and available cars are matched** - same cars shown in the Live Cars tab:
            - Status: "Published" or "Being Sold"
            - Publishing State: "Wholesale"
            - Valid make, model, year, kilometers, and price data

            #### **🚫 Dealer Exclusions**
            - **Dealer D-0200**: Excluded from all matching recommendations

            #### **📋 Queue Position Information**
            - **All dealers shown by default**: Both queued and non-queued dealers are displayed
            - **Queue position**: Shows "#1", "#2", etc. for dealers in queue, "-" for non-queued dealers
            - **Filtering options**: Can filter to show only queued, only non-queued, or specific queue positions

            #### **🏪 Cars Displayed Count**
            - **Shows current showroom inventory**: Count of cars each dealer currently has "Being Displayed"
            - **Real-time status**: Based on current showroom request status
            - **Default to 0**: Shows "0 cars" for dealers with no cars currently displayed

            #### **🚗 Car Traction Level**
            - **High Traction**: 3 or more dealers in queue for the car
            - **Low Traction**: 1-2 dealers in queue for the car
            - **No Traction**: No dealers currently in queue for the car
            - **Queue Count**: Shows exact number of dealers queued for each car

            #### **💰 Discount Pricing**
            - **Discount Eligibility**: Cars with showroom_displayed_count < 2 and days_in_consignment between 0-14 days
            - **Final Price**: Shows speed_discount_price if discounted, otherwise consignment_price
            - **Discounted Flag**: Yes/No indicator for discount eligibility
            - **Price Comparison**: Original price vs final discounted price displayed

            #### **🎯 Match Score (Historical Purchase Patterns)**
            - **Exact Model Match** (25 pts max): If dealer bought this exact model before
            - **Same Make Match** (15 pts max): If dealer bought this brand (when no exact model)
            - **Same Origin Group** (10 pts max): If dealer bought cars from same region (Japanese/German/etc.)
            - **Year Segment** (5 pts max): If dealer bought cars in same year segment (2010-2016, 2017-2019, 2020-2021, 2022-2024)
            - **Mileage Segment** (5 pts max): If dealer bought cars in same mileage segment (0-30K, 30K-60K, 60K-90K, 90K-120K, 120K+)

            #### **🏪 Showroom Performance Score (40 points total)**

            **Showroom Selling Rate (20 points)** - `sold_in_showroom / succ_showroom_requests`
            - 1-10%: 5 points
            - 10-15%: 10 points  
            - 15-20%: 15 points
            - >20%: 20 points

            **Days to Sell in Showroom (10 points)** - `days_to_sold_in_showroom`
            - 0-7 days: 10 points
            - 7-10 days: 7 points
            - 10-15 days: 5 points
            - >15 days: 0 points

            **Days from Last Request (10 points)** - `days_from_last_request` (higher = more available)
            - 0-14 days: 5 points
            - >14 days: 10 points

            ---

            #### **📅 **Data Timeframes:**
            - **Historical Purchases**: Last 12 months
            - **Showroom Performance**: In 2025
            - **Live Cars**: Currently published and available

            #### **🎨 **Result Categories:**
            - 🟢 **High Potential (60+)**: Strong showroom performance + excellent match
            - 🟡 **Medium Potential (40-59)**: Good potential with moderate performance
            - 🔴 **Low Potential (20-39)**: Some relevance but lower priority

            **Maximum Total Score: 160 points** (120 Match + 40 Showroom)
            """)


    def main():
        st.title("🏪 Showroom Matcher - Car to Dealer Showroom Matching")

        # Add methodology explanation
        show_methodology()

        # Track page view
        if "current_user" in st.session_state and POSTHOG_ENABLED and posthog:
            try:
                posthog.capture(
                    st.session_state["current_user"],
                    'page_view',
                    {
                        'page': 'showroom_matcher',
                        'timestamp': datetime.now().isoformat()
                    }
                )
            except Exception as e:
                print(f"PostHog page view tracking failed: {e}")

        # Initialize session state for caching expensive operations
        if 'data_loaded' not in st.session_state:
            st.session_state.data_loaded = False

        if 'matches_calculated' not in st.session_state:
            st.session_state.matches_calculated = False

        # Add refresh button and data status
        col1, col2, col3 = st.columns([1, 2, 3])
        with col1:
            if st.button("🔄 Refresh Data", help="Reload data from BigQuery"):
                # Clear all cached data
                for key in ['data_loaded', 'matches_calculated', 'live_cars_with_location',
                            'inventory_df', 'showroom_performance_df', 'historical_df',
                            'recent_views_df', 'recent_filters_df', 'dealer_requests_df',
                            'olx_df', 'location_df', 'current_showroom_df', 'queue_position_df', 'displayed_cars_df',
                            'consignment_cars_df', 'discount_eligibility_df', 'discount_pricing_df', 'matches_df']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.session_state.data_loaded = False
                st.session_state.matches_calculated = False
                st.rerun()

        with col2:
            # Show data freshness indicator
            if st.session_state.data_loaded and 'data_load_time' in st.session_state:
                load_time = st.session_state.data_load_time
                time_diff = datetime.now() - load_time
                if time_diff.total_seconds() < 3600:  # Less than 1 hour
                    minutes_ago = int(time_diff.total_seconds() / 60)
                    st.info(f"📊 Data loaded {minutes_ago}m ago - **Filtering is now instant!**")
                else:
                    hours_ago = int(time_diff.total_seconds() / 3600)
                    st.info(f"📊 Data loaded {hours_ago}h ago - **Filtering is now instant!**")

        # Load data only once and cache in session state
        if not st.session_state.data_loaded:
            with st.spinner("Loading inventory and showroom data... (This may take a moment)"):
                inventory_df, showroom_performance_df, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df, location_df, current_showroom_df, queue_position_df, displayed_cars_df, consignment_cars_df, discount_eligibility_df, discount_pricing_df = load_showroom_data()

                # Store in session state
                st.session_state.inventory_df = inventory_df
                st.session_state.showroom_performance_df = showroom_performance_df
                st.session_state.historical_df = historical_df
                st.session_state.recent_views_df = recent_views_df
                st.session_state.recent_filters_df = recent_filters_df
                st.session_state.dealer_requests_df = dealer_requests_df
                st.session_state.olx_df = olx_df
                st.session_state.location_df = location_df
                st.session_state.current_showroom_df = current_showroom_df
                st.session_state.queue_position_df = queue_position_df
                st.session_state.displayed_cars_df = displayed_cars_df
                st.session_state.consignment_cars_df = consignment_cars_df
                st.session_state.discount_eligibility_df = discount_eligibility_df
                st.session_state.discount_pricing_df = discount_pricing_df
                st.session_state.data_loaded = True
                st.session_state.data_load_time = datetime.now()
        else:
            # Use cached data
            inventory_df = st.session_state.inventory_df
            showroom_performance_df = st.session_state.showroom_performance_df
            historical_df = st.session_state.historical_df
            recent_views_df = st.session_state.recent_views_df
            recent_filters_df = st.session_state.recent_filters_df
            dealer_requests_df = st.session_state.dealer_requests_df
            olx_df = st.session_state.olx_df
            location_df = st.session_state.location_df
            current_showroom_df = st.session_state.current_showroom_df
            queue_position_df = st.session_state.queue_position_df
            displayed_cars_df = st.session_state.displayed_cars_df
            consignment_cars_df = st.session_state.consignment_cars_df
            discount_eligibility_df = st.session_state.discount_eligibility_df
            discount_pricing_df = st.session_state.discount_pricing_df

        if inventory_df.empty:
            st.warning("No inventory data available.")
            return

        # Display summary metrics
        with col2:
            # Count live cars (same logic as matching)
            live_cars_count = len(inventory_df)
            if 'current_status' in inventory_df.columns:
                live_cars_count = len(inventory_df[inventory_df['current_status'].isin(['Published', 'Being Sold'])])
            if 'publishing_state' in inventory_df.columns:
                live_cars_count = len(inventory_df[
                                          (inventory_df['current_status'].isin(['Published', 'Being Sold'])) &
                                          (inventory_df['publishing_state'] == 'Wholesale')
                                          ])
            st.metric("Live Cars Available", live_cars_count)
        with col3:
            # Count dealers excluding D-0200
            all_dealers = showroom_performance_df['dealer_code'].unique() if not showroom_performance_df.empty else []
            unique_dealers = len([dealer for dealer in all_dealers if dealer != 'D-0200'])
            st.metric("Matched Dealers", unique_dealers)

        # Generate showroom matches only once and cache in session state
        if not st.session_state.matches_calculated:
            with st.spinner("Calculating showroom matches... (One-time calculation)"):
                matches = generate_showroom_matches(inventory_df, showroom_performance_df, historical_df,
                                                    recent_views_df, recent_filters_df, dealer_requests_df, olx_df,
                                                    location_df, current_showroom_df, queue_position_df,
                                                    displayed_cars_df, consignment_cars_df, discount_eligibility_df,
                                                    discount_pricing_df)

                if not matches:
                    st.warning("No showroom matches found.")
                    return

                # Store matches in session state
                st.session_state.matches_df = pd.DataFrame(matches)
                st.session_state.matches_calculated = True

                # Count unique live cars being matched
                unique_live_cars = len(st.session_state.matches_df['car_code'].unique())

                # Show success message
                st.success(
                    f"✅ **All data loaded and matches calculated! Filtering is now instant.** 🚀\n📊 **{unique_live_cars} live cars** matched with dealers")

        # Use cached matches
        matches_df = st.session_state.matches_df

        # Create main tabs
        main_tab1, main_tab2 = st.tabs(["🎯 Showroom Matching", "🏪 Consignment Cars"])

        with main_tab1:
            st.subheader("🎯 Showroom Matching Results")
           
            # Filtering section
            st.write("**Filter Options:** ⚡ *Instant filtering on cached data*")
            col1, col2, col3, col4, col5 = st.columns(5)

            # Additional filter row for new features
            filter_row2_col1, filter_row2_col2, filter_row2_col3, filter_row2_col4, filter_row2_col5 = st.columns(5)

            with col1:
                # Car filter with enhanced display format
                car_options = ["All Cars"]
                car_display_map = {}

                for _, row in matches_df.drop_duplicates('car_code').iterrows():
                    car_display = f"{row['car_code']} - {row['make']} {row['model']} {row['year']} ({row['doa']:.1f} days, {row['requests_count']} requests)"
                    car_options.append(car_display)
                    car_display_map[car_display] = row['car_code']

                selected_car_display = st.selectbox("Select Car:", car_options)
                selected_car = car_display_map.get(selected_car_display, "All Cars")

            with col2:
                # Location filter
                unique_locations = matches_df['location'].dropna().unique()
                location_options = ["All Locations"] + sorted(list(unique_locations))
                selected_location = st.selectbox("Select Location:", location_options)

            with col3:
                # Dealer filter
                unique_dealers = matches_df['dealer_code'].dropna().unique()
                dealer_options = ["All Dealers"] + sorted(list(unique_dealers))
                selected_dealer = st.selectbox("Select Dealer:", dealer_options)

            with col4:
                # Days in location filter
                # Get valid days in location values (exclude NaN and negative values)
                valid_days = matches_df['days_in_location'].dropna()
                valid_days = valid_days[valid_days >= 0]

                if not valid_days.empty:
                    min_days = int(valid_days.min())
                    max_days = int(valid_days.max())
                    days_range = st.slider(
                        "Days in Location",
                        min_value=min_days,
                        max_value=max_days,
                        value=(min_days, max_days),
                        help="Filter by how long cars have been in their current location"
                    )

                    # Quick filter for requested cars only
                    show_requested_only = st.checkbox(
                        "Show Requested Cars Only",
                        value=False,
                        help="Show only cars that have been requested by dealers"
                    )
                else:
                    days_range = (0, 0)
                    show_requested_only = False
                    st.info("No days in location data available")

            with col5:
                # Queue position filter
                # Get valid queue position values (exclude NaN)
                valid_queue_positions = matches_df['queue_position'].dropna()

                if not valid_queue_positions.empty:
                    min_queue = int(valid_queue_positions.min())
                    max_queue = int(valid_queue_positions.max())
                    queue_range = st.slider(
                        "Queue Position",
                        min_value=min_queue,
                        max_value=max_queue,
                        value=(min_queue, max_queue),
                        help="Filter by dealer's position in queue (lower = higher priority)"
                    )

                    # Quick filter for queued dealers only
                    show_queued_only = st.checkbox(
                        "Show Queued Dealers Only",
                        value=False,
                        help="Show only dealers who are in queue for cars"
                    )

                    # Quick filter for non-queued dealers only
                    show_non_queued_only = st.checkbox(
                        "Show Non-Queued Dealers Only",
                        value=False,
                        help="Show only dealers who are NOT in queue for cars"
                    )
                else:
                    queue_range = (1, 1)
                    show_queued_only = False
                    show_non_queued_only = False
                    st.info("No queue data available")

            # Second row of filters for new features
            with filter_row2_col1:
                # Traction filter
                unique_traction = matches_df['car_traction'].dropna().unique()
                traction_options = ["All Traction Levels"] + sorted(list(unique_traction))
                selected_traction = st.selectbox("Car Traction:", traction_options)

            with filter_row2_col2:
                # Discount filter
                discount_options = ["All Cars", "Discounted Only", "Non-Discounted Only"]
                selected_discount = st.selectbox("Discount Status:", discount_options)

            with filter_row2_col3:
                # Queue count range filter
                if not matches_df['queue_count'].dropna().empty:
                    min_queue_count = int(matches_df['queue_count'].min())
                    max_queue_count = int(matches_df['queue_count'].max())
                    queue_count_range = st.slider(
                        "Queue Count Range",
                        min_value=min_queue_count,
                        max_value=max_queue_count,
                        value=(min_queue_count, max_queue_count),
                        help="Filter by number of dealers in queue for each car"
                    )
                else:
                    queue_count_range = (0, 0)

            # Apply filters
            filtered_df = matches_df.copy()

            if selected_car != "All Cars":
                filtered_df = filtered_df[filtered_df['car_code'] == selected_car]

            if selected_location != "All Locations":
                filtered_df = filtered_df[filtered_df['location'] == selected_location]

            if selected_dealer != "All Dealers":
                filtered_df = filtered_df[filtered_df['dealer_code'] == selected_dealer]

            # Apply days in location filter
            if 'days_range' in locals() and days_range != (0, 0):
                filtered_df = filtered_df[
                    (filtered_df['days_in_location'] >= days_range[0]) &
                    (filtered_df['days_in_location'] <= days_range[1])
                    ]

            # Apply requested cars filter
            if 'show_requested_only' in locals() and show_requested_only:
                filtered_df = filtered_df[filtered_df['requested'] == 'Yes']

            # Apply queue position filter (only for dealers who are in queue)
            if 'queue_range' in locals() and queue_range != (1, 1):
                # Filter only dealers who are in queue within the specified range
                queued_in_range = filtered_df[
                    (pd.notna(filtered_df['queue_position'])) &
                    (filtered_df['queue_position'] >= queue_range[0]) &
                    (filtered_df['queue_position'] <= queue_range[1])
                    ]
                # Get all non-queued dealers
                non_queued = filtered_df[pd.isna(filtered_df['queue_position'])]
                # Combine results
                filtered_df = pd.concat([queued_in_range, non_queued], ignore_index=True)

            # Apply queued dealers filter
            if 'show_queued_only' in locals() and show_queued_only:
                filtered_df = filtered_df[pd.notna(filtered_df['queue_position'])]

            # Apply non-queued dealers filter
            if 'show_non_queued_only' in locals() and show_non_queued_only:
                filtered_df = filtered_df[pd.isna(filtered_df['queue_position'])]

            # Apply traction filter
            if selected_traction != "All Traction Levels":
                filtered_df = filtered_df[filtered_df['car_traction'] == selected_traction]

            # Apply discount filter
            if selected_discount == "Discounted Only":
                filtered_df = filtered_df[filtered_df['is_discounted'] == True]
            elif selected_discount == "Non-Discounted Only":
                filtered_df = filtered_df[filtered_df['is_discounted'] == False]

            # Apply queue count range filter
            if 'queue_count_range' in locals() and queue_count_range != (0, 0):
                filtered_df = filtered_df[
                    (filtered_df['queue_count'] >= queue_count_range[0]) &
                    (filtered_df['queue_count'] <= queue_count_range[1])
                    ]

            # Sort by total score
            filtered_df = filtered_df.sort_values('total_score', ascending=False)

            # Display results
            unique_cars_matched = len(filtered_df['car_code'].unique())
            queued_dealers = len(filtered_df[pd.notna(filtered_df['queue_position'])])
            non_queued_dealers = len(filtered_df[pd.isna(filtered_df['queue_position'])])
            st.write(
                f"**Showing {len(filtered_df)} matches from {unique_cars_matched} live cars:** ({queued_dealers} queued, {non_queued_dealers} non-queued dealers)")

            if not filtered_df.empty:
                # Prepare display DataFrame
                display_df = filtered_df[[
                    'car_code', 'price', 'final_price', 'is_discounted', 'car_traction', 'queue_count', 'model', 'make',
                    'year', 'kilometers', 'location',
                    'requested', 'days_in_location', 'queue_position', 'cars_displayed', 'dealer_name', 'dealer_code',
                    'match_score', 'showroom_score', 'total_score'
                ]].copy()

                # Add match level
                display_df['match_level'] = display_df['total_score'].apply(
                    lambda x: '🟢 High' if x >= 60 else '🟡 Medium' if x >= 40 else '🔴 Low'
                )

                # Format columns
                display_df['price'] = display_df['price'].apply(lambda x: f"EGP {x:,.0f}")
                display_df['final_price'] = display_df['final_price'].apply(lambda x: f"EGP {x:,.0f}")
                display_df['is_discounted'] = display_df['is_discounted'].apply(lambda x: "Yes" if x else "No")
                display_df['queue_count'] = display_df['queue_count'].apply(
                    lambda x: f"{int(x)}" if pd.notna(x) else "0")
                display_df['kilometers'] = display_df['kilometers'].apply(lambda x: f"{x:,.0f} km")
                display_df['days_in_location'] = display_df['days_in_location'].apply(
                    lambda x: f"{int(x)} days" if pd.notna(x) and x > 0 else "-")
                display_df['queue_position'] = display_df['queue_position'].apply(
                    lambda x: f"#{int(x)}" if pd.notna(x) else "-")
                display_df['cars_displayed'] = display_df['cars_displayed'].apply(
                    lambda x: f"{int(x)} cars" if pd.notna(x) and x > 0 else "0 cars")
                display_df['match_score'] = display_df['match_score'].round(1)
                display_df['showroom_score'] = display_df['showroom_score'].round(1)
                display_df['total_score'] = display_df['total_score'].round(1)

                # Rename columns for display
                display_df = display_df.rename(columns={
                    'car_code': 'Car Code',
                    'price': 'Original Price',
                    'final_price': 'Final Price',
                    'is_discounted': 'Discounted',
                    'car_traction': 'Traction',
                    'queue_count': 'Queue Count',
                    'model': 'Model',
                    'make': 'Make',
                    'year': 'Year',
                    'kilometers': 'Kilometers',
                    'location': 'Location',
                    'requested': 'Requested',
                    'days_in_location': 'Days in Location',
                    'queue_position': 'Queue Position',
                    'cars_displayed': 'Cars Displayed',
                    'dealer_name': 'Dealer Name',
                    'dealer_code': 'Dealer Code',
                    'match_score': 'Match Score',
                    'showroom_score': 'Showroom Score',
                    'total_score': 'Total Score',
                    'match_level': 'Level'
                })

                # Color coding function - only color specific cells, not entire rows
                def highlight_scores(val):
                    if val == '🟢 High':
                        return 'background-color: #90EE90'
                    elif val == '🟡 Medium':
                        return 'background-color: #FFE4B5'
                    elif val == '🔴 Low':
                        return 'background-color: #FFCCCB'
                    else:
                        return ''

                # Display styled dataframe
                st.dataframe(
                    display_df.style.applymap(highlight_scores, subset=['Level']),
                    use_container_width=True
                )

                st.info("🟢 High Potential (60+) | 🟡 Medium Potential (40-59) | 🔴 Low Potential (20-39)")

                # Export functionality
                st.subheader("📥 Export Results")

                # Create CSV
                csv_data = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv_data,
                    file_name=f"showroom_matches_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv"
                )

                # Detailed analysis for top matches
                st.subheader("📋 Top Matches Analysis")

                top_matches = filtered_df.head(10)

                for i, (_, match) in enumerate(top_matches.iterrows(), 1):
                    with st.expander(
                            f"{i}. {match['car_code']} → {match['dealer_name']} (Total: {match['total_score']:.1f})"):
                        col1, col2 = st.columns(2)

                        with col1:
                            st.write("**Car Details:**")
                            st.write(f"• Code: {match['car_code']}")
                            st.write(f"• Make/Model: {match['make']} {match['model']}")
                            st.write(f"• Year: {match['year']}")
                            st.write(f"• Kilometers: {match['kilometers']:,.0f} km")
                            st.write(f"• Original Price: EGP {match['price']:,.0f}")
                            st.write(f"• Final Price: EGP {match['final_price']:,.0f}")
                            st.write(f"• Discounted: {'Yes' if match['is_discounted'] else 'No'}")
                            st.write(f"• Traction: {match['car_traction']} ({match['queue_count']} in queue)")
                            st.write(f"• Location: {match['location']}")
                            st.write(f"• Requested: {match['requested']}")
                            if pd.notna(match['days_in_location']) and match['days_in_location'] > 0:
                                st.write(f"• Days in Location: {match['days_in_location']} days")
                            if pd.notna(match['queue_position']):
                                st.write(f"• Queue Position: #{int(match['queue_position'])}")
                            cars_displayed_count = match.get('cars_displayed', 0)
                            st.write(f"• Cars in Showroom: {int(cars_displayed_count)}")
                            st.write(f"• Group: {match['car_group']}")

                        with col2:
                            st.write("**Score Breakdown:**")
                            st.write(f"• Match Score: {match['match_score']:.1f}")
                            for component, score in match['match_breakdown'].items():
                                st.write(f"  - {component}: {score:.1f}")
                            st.write(f"• Showroom Score: {match['showroom_score']:.1f}")
                            for component, score in match['showroom_breakdown'].items():
                                st.write(f"  - {component}: {score:.1f}")
                            st.write(f"**Total Score: {match['total_score']:.1f}**")

            else:
                st.warning("No matches found with the current filters. Try adjusting the filter criteria.")

            # Summary statistics
            if not matches_df.empty:
                st.subheader("📊 Summary Statistics")

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.write("**Score Distribution:**")
                    high_matches = len(matches_df[matches_df['total_score'] >= 60])
                    medium_matches = len(
                        matches_df[(matches_df['total_score'] >= 40) & (matches_df['total_score'] < 60)])
                    low_matches = len(matches_df[(matches_df['total_score'] >= 20) & (matches_df['total_score'] < 40)])

                    st.write(f"🟢 High (60+): {high_matches}")
                    st.write(f"🟡 Medium (40-59): {medium_matches}")
                    st.write(f"🔴 Low (20-39): {low_matches}")

                with col2:
                    st.write("**Top Car Groups:**")
                    group_counts = matches_df['car_group'].value_counts().head(5)
                    for group, count in group_counts.items():
                        st.write(f"• {group}: {count}")

                with col3:
                    st.write("**Average Scores:**")
                    avg_match = matches_df['match_score'].mean()
                    avg_showroom = matches_df['showroom_score'].mean()
                    avg_total = matches_df['total_score'].mean()

                    st.write(f"• Match Score: {avg_match:.1f}")
                    st.write(f"• Showroom Score: {avg_showroom:.1f}")
                    st.write(f"• Total Score: {avg_total:.1f}")

        with main_tab2:
            # Live Cars tab content - using cached data for fast filtering
            show_live_cars_tab(st.session_state.inventory_df, st.session_state.location_df,
                               st.session_state.consignment_cars_df)


    def show_live_cars_tab(inventory_df, location_df, consignment_cars_df):
        """Display the Live Cars tab with filtering functionality - only consignment available cars"""
        st.subheader("🚗 Live Cars Inventory - Consignment Available Only")

        # Track tab view
        if "current_user" in st.session_state and POSTHOG_ENABLED and posthog:
            try:
                posthog.capture(
                    st.session_state["current_user"],
                    'tab_view',
                    {
                        'tab': 'live_cars',
                        'timestamp': datetime.now().isoformat()
                    }
                )
            except Exception as e:
                print(f"PostHog tab view tracking failed: {e}")

        if inventory_df.empty:
            st.warning("No live cars data available.")
            return

        # Add info about consignment filtering
        st.info("🏪 **Consignment Available Only**: Showing only cars available for consignment (not sold via Buy Now)")

        if consignment_cars_df.empty:
            st.warning("No cars available for consignment at the moment.")
            return

        try:
            # Cache the merged live cars data to avoid re-processing
            if 'live_cars_with_location' not in st.session_state or not st.session_state.data_loaded:
                # Filter for only consignment available cars first
                if not consignment_cars_df.empty:
                    # Get list of consignment available car names
                    consignment_car_names = consignment_cars_df['sf_vehicle_name'].tolist()
                    # Filter inventory to only include consignment available cars
                    live_cars_with_location = inventory_df[
                        inventory_df['sf_vehicle_name'].isin(consignment_car_names)].copy()

                    # Merge with consignment data to get flash_sale_flag
                    live_cars_with_location = live_cars_with_location.merge(
                        consignment_cars_df[['sf_vehicle_name', 'flash_sale_flag']],
                        on='sf_vehicle_name',
                        how='left'
                    )
                else:
                    # If no consignment data, show empty dataframe
                    live_cars_with_location = pd.DataFrame()

                # Merge location data if we have consignment cars
                if not live_cars_with_location.empty:
                    if not location_df.empty:
                        live_cars_with_location = live_cars_with_location.merge(
                            location_df,
                            left_on='sf_vehicle_name',
                            right_on='car_name',
                            how='left'
                        )
                        # Fill NaN values in location
                        live_cars_with_location['location_stage_name'] = live_cars_with_location[
                            'location_stage_name'].fillna('Unknown')
                    else:
                        live_cars_with_location['location_stage_name'] = 'Unknown'

                # Cache the processed data
                st.session_state.live_cars_with_location = live_cars_with_location
            else:
                # Use cached processed data
                live_cars_with_location = st.session_state.live_cars_with_location

            # Display summary metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Cars", len(live_cars_with_location))
            with col2:
                if 'flash_sale_flag' in live_cars_with_location.columns:
                    flash_sale_count = len(
                        live_cars_with_location[live_cars_with_location['flash_sale_flag'] == 'Flash sale'])
                    st.metric("Flash Sale", flash_sale_count)
                else:
                    st.metric("Flash Sale", 0)
            with col3:
                if 'flash_sale_flag' in live_cars_with_location.columns:
                    consignment_count = len(
                        live_cars_with_location[live_cars_with_location['flash_sale_flag'] == 'Consignment'])
                    st.metric("Consignment", consignment_count)
                else:
                    st.metric("Consignment", 0)
            with col4:
                avg_doa = live_cars_with_location['DOA'].mean()
                st.metric("Avg Days on App", f"{avg_doa:.1f}")
            with col5:
                avg_price = live_cars_with_location['App_price'].mean()
                st.metric("Avg Price", f"EGP {avg_price:,.0f}")

            # Filters
            st.write("**Filter Options:** ⚡ *Instant filtering on cached data*")
            filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

            with filter_col1:
                # Make filter
                unique_makes = live_cars_with_location['make'].dropna().unique()
                makes = ["All Makes"] + sorted(unique_makes.tolist())
                selected_make = st.selectbox("Make", makes)

            with filter_col2:
                # Model filter (filtered based on make selection)
                if selected_make != "All Makes":
                    available_models = live_cars_with_location[live_cars_with_location['make'] == selected_make][
                        'model'].dropna().unique()
                else:
                    available_models = live_cars_with_location['model'].dropna().unique()
                models = ["All Models"] + sorted(available_models.tolist())
                selected_model = st.selectbox("Model", models)

            with filter_col3:
                # Price range filter
                min_price = int(live_cars_with_location['App_price'].min())
                max_price = int(live_cars_with_location['App_price'].max())
                price_range = st.slider(
                    "Price Range (EGP)",
                    min_value=min_price,
                    max_value=max_price,
                    value=(min_price, max_price),
                    step=10000
                )

            with filter_col4:
                # Location filter
                unique_locations = live_cars_with_location['location_stage_name'].dropna().unique()
                locations = ["All Locations"] + sorted(unique_locations.tolist())
                selected_location = st.selectbox("Location", locations)

            # Additional filters row with sale type
            filter_col8, filter_col9 = st.columns(2)

            with filter_col8:
                # Sale type filter
                if 'flash_sale_flag' in live_cars_with_location.columns:
                    unique_sale_types = live_cars_with_location['flash_sale_flag'].dropna().unique()
                    sale_types = ["All Sale Types"] + sorted(unique_sale_types.tolist())
                    selected_sale_type = st.selectbox("Sale Type", sale_types)
                else:
                    selected_sale_type = "All Sale Types"

            # Additional filters
            filter_col5, filter_col6, filter_col7 = st.columns(3)

            with filter_col5:
                # Publishing date filter
                min_date = live_cars_with_location['publishing_date'].min().date()
                max_date = live_cars_with_location['publishing_date'].max().date()
                date_range = st.date_input(
                    "Publishing Date Range",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date
                )

            with filter_col6:
                # DOA filter
                min_doa = 0
                max_doa = int(live_cars_with_location['DOA'].max())
                doa_range = st.slider(
                    "Days on App",
                    min_value=min_doa,
                    max_value=max_doa,
                    value=(min_doa, max_doa)
                )

            with filter_col7:
                # Year filter
                min_year = int(live_cars_with_location['year'].min())
                max_year = int(live_cars_with_location['year'].max())
                year_range = st.slider(
                    "Year Range",
                    min_value=min_year,
                    max_value=max_year,
                    value=(min_year, max_year)
                )

            # Apply filters
            filtered_cars = live_cars_with_location.copy()

            if selected_make != "All Makes":
                filtered_cars = filtered_cars[filtered_cars['make'] == selected_make]

            if selected_model != "All Models":
                filtered_cars = filtered_cars[filtered_cars['model'] == selected_model]

            if selected_location != "All Locations":
                filtered_cars = filtered_cars[filtered_cars['location_stage_name'] == selected_location]

            # Apply sale type filter
            if selected_sale_type != "All Sale Types":
                filtered_cars = filtered_cars[filtered_cars['flash_sale_flag'] == selected_sale_type]

            # Apply price filter
            filtered_cars = filtered_cars[
                (filtered_cars['App_price'] >= price_range[0]) &
                (filtered_cars['App_price'] <= price_range[1])
                ]

            # Apply DOA filter
            filtered_cars = filtered_cars[
                (filtered_cars['DOA'] >= doa_range[0]) &
                (filtered_cars['DOA'] <= doa_range[1])
                ]

            # Apply year filter
            filtered_cars = filtered_cars[
                (filtered_cars['year'] >= year_range[0]) &
                (filtered_cars['year'] <= year_range[1])
                ]

            # Apply date filter
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
                filtered_cars = filtered_cars[
                    (filtered_cars['publishing_date'].dt.date >= start_date) &
                    (filtered_cars['publishing_date'].dt.date <= end_date)
                    ]

            # Display filtered results count
            st.write(f"**Showing {len(filtered_cars)} cars (filtered from {len(live_cars_with_location)} total)**")

            # Display filtered cars
            if not filtered_cars.empty:
                display_columns = ['sf_vehicle_name', 'make', 'model', 'year', 'kilometers',
                                   'publishing_date', 'DOA', 'App_price', 'flash_sale_flag', 'Buy_now_requests_count',
                                   'showroom_requests_count', 'Buy_now_visits_count', 'location_stage_name']

                # Ensure all required columns exist
                missing_columns = [col for col in display_columns if col not in filtered_cars.columns]
                if missing_columns:
                    st.error(f"Missing columns in data: {', '.join(missing_columns)}")
                    st.write("Available columns:", filtered_cars.columns.tolist())
                else:
                    # Sort by most recent publishing date and highest requests
                    filtered_cars = filtered_cars.sort_values(['publishing_date', 'Buy_now_requests_count'],
                                                              ascending=[False, False])

                    st.dataframe(
                        filtered_cars[display_columns],
                        column_config={
                            "sf_vehicle_name": "Vehicle Name",
                            "make": "Make",
                            "model": "Model",
                            "year": "Year",
                            "kilometers": st.column_config.NumberColumn(
                                "Kilometers",
                                format="%d km"
                            ),
                            "publishing_date": "Published Date",
                            "DOA": st.column_config.NumberColumn(
                                "Days on App",
                                format="%d days"
                            ),
                            "App_price": st.column_config.NumberColumn(
                                "Price",
                                format="EGP %d"
                            ),
                            "flash_sale_flag": "Sale Type",
                            "Buy_now_requests_count": st.column_config.NumberColumn(
                                "Buy Now Requests",
                                format="%d"
                            ),
                            "showroom_requests_count": st.column_config.NumberColumn(
                                "Showroom Requests",
                                format="%d"
                            ),
                            "Buy_now_visits_count": st.column_config.NumberColumn(
                                "Visits",
                                format="%d"
                            ),
                            "location_stage_name": "Location"
                        },
                        use_container_width=True
                    )

                    # Export functionality for live cars
                    st.subheader("📥 Export Live Cars Data")
                    csv_data = filtered_cars[display_columns].to_csv(index=False)
                    st.download_button(
                        label="📥 Download Live Cars as CSV",
                        data=csv_data,
                        file_name=f"live_cars_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv"
                    )
            else:
                st.warning("No cars match the selected filters")

        except Exception as e:
            st.error(f"Error in Live Cars tab: {str(e)}")
            st.write("Debug info:")
            st.write("inventory_df shape:", inventory_df.shape if inventory_df is not None else "None")
            st.write("inventory_df columns:", inventory_df.columns.tolist() if inventory_df is not None else "None")


    if __name__ == "__main__":
        main()
