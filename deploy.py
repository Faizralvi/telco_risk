import streamlit as st
import pandas as pd
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from io import StringIO
import time

###############################################################################
# Helper functions
###############################################################################

def connect_mongodb(uri: str, db_name: str, collection_name: str):
    """Return a MongoDB collection handle."""
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    # create useful indexes if they don't exist
    collection.create_index("City")
    collection.create_index("Phone Number")
    collection.create_index("Age")
    collection.create_index([("City", 1), ("Age", -1)])  # Compound index for city and age
    collection.create_index("Marital Status")
    collection.create_index("Gender")
    return collection


def connect_cassandra(hosts: list[str], keyspace: str):
    """Return an active Cassandra session, creating keyspace & table when needed."""
    cluster = Cluster(hosts)
    session = cluster.connect()

    # Create keyspace if missing
    ks_stmt = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    """
    session.execute(ks_stmt)
    session.set_keyspace(keyspace)

    # Create table if missing
    tbl_stmt = """
    CREATE TABLE IF NOT EXISTS call_records (
        phone_number text PRIMARY KEY,
        account_length int,
        vmail_message int,
        day_mins float,
        day_calls int,
        day_charge float,
        eve_mins float,
        eve_calls int,
        eve_charge float,
        night_mins float,
        night_calls int,
        night_charge float,
        intl_mins float,
        intl_calls int,
        intl_charge float,
        custserv_calls int,
        churn boolean
    )
    """
    session.execute(tbl_stmt)

    # Helpful secondary index (if not yet created)
    session.execute("CREATE INDEX IF NOT EXISTS intl_charge_idx ON call_records (intl_charge)")
    session.execute("CREATE INDEX IF NOT EXISTS churn_idx ON call_records (churn)")
    return session


def insert_customer_csv(df: pd.DataFrame, collection):
    """Bulk‑insert customer DataFrame into MongoDB."""
    if not df.empty:
        collection.insert_many(df.to_dict(orient="records"))


def insert_call_csv(df: pd.DataFrame, session):
    """Bulk‑insert call log DataFrame into Cassandra."""
    if df.empty:
        return

    # Normalise column names → lowercase_no_space
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    ins_stmt = SimpleStatement(
        """
        INSERT INTO call_records (
            phone_number, account_length, vmail_message, day_mins, day_calls, day_charge,
            eve_mins, eve_calls, eve_charge, night_mins, night_calls, night_charge,
            intl_mins, intl_calls, intl_charge, custserv_calls, churn
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    )

    for _, row in df.iterrows():
        session.execute(
            ins_stmt,
            (
                str(row["phone_number"]),
                int(row["account_length"]),
                int(row["vmail_message"]),
                float(row["day_mins"]),
                int(row["day_calls"]),
                float(row["day_charge"]),
                float(row["eve_mins"]),
                int(row["eve_calls"]),
                float(row["eve_charge"]),
                float(row["night_mins"]),
                int(row["night_calls"]),
                float(row["night_charge"]),
                float(row["intl_mins"]),
                int(row["intl_calls"]),
                float(row["intl_charge"]),
                int(row["custserv_calls"]),
                bool(row["churn"]),
            ),
        )


def query_cassandra(session, intl_charge_threshold: float, min_day_mins: float = None, 
                   max_custserv_calls: int = None, churn_filter: bool = None,
                   risk_factors: list = None):
    """Return list[dict] of call_records with risk-based filtering."""
    base_query = "SELECT * FROM call_records WHERE intl_charge >= %s"
    params = [intl_charge_threshold]
    
    if min_day_mins is not None:
        base_query += " AND day_mins >= %s"
        params.append(min_day_mins)
    
    if max_custserv_calls is not None:
        base_query += " AND custserv_calls >= %s"  # Changed to >= to find problematic cases
        params.append(max_custserv_calls)
        
    if churn_filter is not None:
        base_query += " AND churn = %s"
        params.append(churn_filter)
    
    base_query += " ALLOW FILTERING;"
    
    rows = session.execute(base_query, params)
    results = [r._asdict() for r in rows]
    
    # Apply additional risk factor filtering
    if risk_factors:
        filtered_results = []
        for record in results:
            risk_score = 0
            
            # High Usage + High Service Calls
            if "High Usage + High Service Calls" in risk_factors:
                if record['day_mins'] > 300 and record['custserv_calls'] >= 3:
                    risk_score += 1
                    
            # International User + High Charges
            if "International User + High Charges" in risk_factors:
                if record['intl_calls'] > 0 and record['intl_charge'] > 10:
                    risk_score += 1
                    
            # Multiple Service Issues
            if "Multiple Service Issues" in risk_factors:
                service_issues = 0
                if record['custserv_calls'] >= 3: service_issues += 1
                if record['intl_charge'] > record['day_charge'] * 0.5: service_issues += 1
                if record['day_mins'] > 300: service_issues += 1
                if service_issues >= 2:
                    risk_score += 1
            
            if risk_score > 0:
                record['risk_score'] = risk_score
                filtered_results.append(record)
        
        return filtered_results
    
    return results


def query_mongodb_aggregation(collection, min_age: int = 0, group_by: str = "City"):
    """Perform MongoDB aggregation to get customer demographics statistics."""
    pipeline = [
        {"$match": {"Age": {"$gte": min_age}}},
        {"$group": {
            "_id": f"${group_by}",
            "total_customers": {"$sum": 1},
            "avg_age": {"$avg": "$Age"},
            "min_age": {"$min": "$Age"},
            "max_age": {"$max": "$Age"},
            "gender_distribution": {
                "$push": "$Gender"
            },
            "marital_status_distribution": {
                "$push": "$Marital Status"
            }
        }},
        {"$addFields": {
            "gender_stats": {
                "$reduce": {
                    "input": "$gender_distribution",
                    "initialValue": {"Male": 0, "Female": 0},
                    "in": {
                        "Male": {
                            "$add": [
                                "$$value.Male",
                                {"$cond": [{"$eq": ["$$this", "Male"]}, 1, 0]}
                            ]
                        },
                        "Female": {
                            "$add": [
                                "$$value.Female",
                                {"$cond": [{"$eq": ["$$this", "Female"]}, 1, 0]}
                            ]
                        }
                    }
                }
            },
            "marital_stats": {
                "$reduce": {
                    "input": "$marital_status_distribution",
                    "initialValue": {"Single": 0, "Married": 0, "Divorced": 0},
                    "in": {
                        "Single": {
                            "$add": [
                                "$$value.Single",
                                {"$cond": [{"$eq": ["$$this", "Single"]}, 1, 0]}
                            ]
                        },
                        "Married": {
                            "$add": [
                                "$$value.Married",
                                {"$cond": [{"$eq": ["$$this", "Married"]}, 1, 0]}
                            ]
                        },
                        "Divorced": {
                            "$add": [
                                "$$value.Divorced",
                                {"$cond": [{"$eq": ["$$this", "Divorced"]}, 1, 0]}
                            ]
                        }
                    }
                }
            }
        }},
        {"$project": {
            "_id": 1,
            "total_customers": 1,
            "avg_age": 1,
            "min_age": 1,
            "max_age": 1,
            "gender_stats": 1,
            "marital_stats": 1
        }},
        {"$sort": {"total_customers": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))


def test_query_performance(collection, session, with_indexes=True):
    """Test query performance with and without indexes."""
    if not with_indexes:
        # Drop all indexes for MongoDB
        collection.drop_indexes()
    
    # Test MongoDB query performance
    start_time = time.time()
    mongo_query = {"Age": {"$gte": 25}, "City": "Yogyakarta"}
    mongo_result = list(collection.find(mongo_query))
    mongo_time = time.time() - start_time
    
    # Test Cassandra query performance
    start_time = time.time()
    cass_query = "SELECT * FROM call_records WHERE intl_charge >= 5.0 AND custserv_calls <= 5 ALLOW FILTERING;"
    cass_result = list(session.execute(cass_query))
    cass_time = time.time() - start_time
    
    if not with_indexes:
        # Recreate indexes for MongoDB
        collection.create_index("City")
        collection.create_index("Phone Number")
        collection.create_index("Age")
        collection.create_index([("City", 1), ("Age", -1)])
        collection.create_index("Marital Status")
        collection.create_index("Gender")
    
    return {
        "mongodb_time": mongo_time,
        "cassandra_time": cass_time,
        "mongodb_results": len(mongo_result),
        "cassandra_results": len(cass_result)
    }


def joined_aggregate_query(collection, session):
    """Perform a joined aggregate query between MongoDB and Cassandra."""
    # Get high-value customers from Cassandra (customers with high international charges)
    cass_query = """
    SELECT phone_number, intl_charge, custserv_calls, churn 
    FROM call_records 
    WHERE intl_charge >= 5.0 
    ALLOW FILTERING;
    """
    high_value_customers = list(session.execute(cass_query))
    
    # Get phone numbers of high-value customers
    phone_numbers = [str(record.phone_number) for record in high_value_customers]
    
    # Get customer details from MongoDB
    mongo_pipeline = [
        {"$match": {"Phone Number": {"$in": phone_numbers}}},
        {"$group": {
            "_id": "$City",
            "total_customers": {"$sum": 1},
            "avg_age": {"$avg": "$Age"},
            "customers": {
                "$push": {
                    "phone": "$Phone Number",
                    "age": "$Age",
                    "gender": "$Gender",
                    "marital_status": "$Marital Status"
                }
            }
        }},
        {"$sort": {"total_customers": -1}}
    ]
    
    mongo_results = list(collection.aggregate(mongo_pipeline))
    
    # Create a mapping of phone numbers to Cassandra data
    cass_data = {
        str(r.phone_number): {
            "intl_charge": float(r.intl_charge),
            "custserv_calls": int(r.custserv_calls),
            "churn": bool(r.churn)
        } for r in high_value_customers
    }
    
    # Combine the results
    joined_results = []
    for city_group in mongo_results:
        city_data = {
            "city": city_group["_id"],
            "total_customers": city_group["total_customers"],
            "avg_age": round(city_group["avg_age"], 2),
            "customers": []
        }
        
        for customer in city_group["customers"]:
            phone = customer["phone"]
            if phone in cass_data:
                customer_data = {
                    **customer,
                    **cass_data[phone]
                }
                city_data["customers"].append(customer_data)
        
        joined_results.append(city_data)
    
    return joined_results

###############################################################################
# Streamlit UI
###############################################################################

def main():
    st.title("📞 Telco NoSQL Demo (MongoDB + Cassandra)")

    with st.sidebar:
        st.header("🔌 Connection Settings")
        mongo_uri = st.text_input("MongoDB URI", value="mongodb+srv://root:root@cluster0.8cymfca.mongodb.net/")
        mongodb_name = st.text_input("MongoDB Database", value="telco_db")
        mongo_collection_name = st.text_input("MongoDB Collection", value="customers")

        cass_hosts_raw = st.text_input("Cassandra Hosts (comma separated)", value="127.0.0.1")
        cass_hosts = [h.strip() for h in cass_hosts_raw.split(",") if h.strip()]
        cass_keyspace = st.text_input("Cassandra Keyspace", value="call_data")

        st.markdown("---")
        st.caption("⬆️ Use the widgets below to upload data and run queries.")

    # File uploaders in main area
    st.subheader("📂 Upload & Load Data")
    uploaded_cust = st.file_uploader("Upload customer CSV", type="csv")
    uploaded_call = st.file_uploader("Upload call logs CSV", type="csv")

    if st.button("🚀 Load & Insert Data"):
        with st.spinner("Connecting to databases & inserting rows…"):
            # Connect
            collection = connect_mongodb(mongo_uri, mongodb_name, mongo_collection_name)
            session = connect_cassandra(cass_hosts, cass_keyspace)

            if uploaded_cust is not None:
                df_cust = pd.read_csv(uploaded_cust)
                insert_customer_csv(df_cust, collection)
                st.success(f"Inserted {len(df_cust):,} customer rows into MongoDB")

            if uploaded_call is not None:
                df_call = pd.read_csv(uploaded_call)
                insert_call_csv(df_call, session)
                st.success(f"Inserted {len(df_call):,} call records into Cassandra")

    # Enhanced Query Section
    st.subheader("🔎 Advanced Query & Analytics")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Basic Query", "Advanced Analytics", "Performance Test", "Joined Query"])
    
    with tab1:
        st.markdown("### 🚨 High-Risk Customer Analysis")
        st.markdown("""
        This query helps identify high-risk customers based on key telco metrics:
        - High international charges (potential bill shock)
        - Excessive customer service calls (satisfaction issues)
        - High daily usage but low satisfaction
        - Location-based analysis
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            intl_charge = st.number_input(
                "High International Charge (≥)",
                help="Customers with international charges above this threshold might experience bill shock",
                min_value=0.0,
                step=0.5,
                value=7.0
            )
            min_day_mins = st.number_input(
                "Heavy Usage (Day minutes ≥)",
                help="Heavy users who might be prone to churning if unsatisfied",
                min_value=0.0,
                step=10.0,
                value=200.0
            )
        with col2:
            city = st.text_input(
                "City for Geo Analysis",
                help="Focus on specific geographic location for targeted intervention",
                value="Yogyakarta"
            )
            max_custserv_calls = st.number_input(
                "Customer Service Calls Threshold",
                help="Customers with many service calls indicate potential dissatisfaction",
                min_value=0,
                value=3
            )
        
        risk_factors = st.multiselect(
            "Additional Risk Factors",
            ["High Usage + High Service Calls", "International User + High Charges", "Multiple Service Issues"],
            default=["High Usage + High Service Calls"]
        )
        
        churn_filter = st.radio(
            "Churn Risk Status",
            [None, True, False],
            help="Filter by current churn status to analyze patterns",
            horizontal=True
        )

    with tab2:
        col3, col4 = st.columns(2)
        with col3:
            min_age = st.number_input("Minimum Age for Aggregation", min_value=0, value=18)
        with col4:
            group_by = st.selectbox("Group By", ["City", "Gender", "Marital Status"])

    with tab3:
        st.subheader("📊 Index Performance Analysis")
        if st.button("Run Performance Test"):
            with st.spinner("Testing query performance..."):
                collection = connect_mongodb(mongo_uri, mongodb_name, mongo_collection_name)
                session = connect_cassandra(cass_hosts, cass_keyspace)

                # Test with indexes
                with_indexes = test_query_performance(collection, session, with_indexes=True)
                # Test without indexes
                without_indexes = test_query_performance(collection, session, with_indexes=False)

                # Display results
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("### With Indexes")
                    st.metric("MongoDB Query Time", f"{with_indexes['mongodb_time']:.4f}s")
                    st.metric("Cassandra Query Time", f"{with_indexes['cassandra_time']:.4f}s")
                    st.metric("MongoDB Results", with_indexes['mongodb_results'])
                    st.metric("Cassandra Results", with_indexes['cassandra_results'])

                with col2:
                    st.markdown("### Without Indexes")
                    st.metric("MongoDB Query Time", f"{without_indexes['mongodb_time']:.4f}s")
                    st.metric("Cassandra Query Time", f"{without_indexes['cassandra_time']:.4f}s")
                    st.metric("MongoDB Results", without_indexes['mongodb_results'])
                    st.metric("Cassandra Results", without_indexes['cassandra_results'])

                # Calculate improvements
                mongo_improvement = ((without_indexes['mongodb_time'] - with_indexes['mongodb_time']) / 
                                  without_indexes['mongodb_time'] * 100)
                cass_improvement = ((without_indexes['cassandra_time'] - with_indexes['cassandra_time']) / 
                                 without_indexes['cassandra_time'] * 100)

                st.markdown("### Performance Improvement with Indexes")
                st.info(f"MongoDB query was {mongo_improvement:.1f}% faster with indexes")
                st.info(f"Cassandra query was {cass_improvement:.1f}% faster with indexes")

    with tab4:
        st.subheader("🔄 Joined Query Results")
        if st.button("Run Joined Query"):
            with st.spinner("Performing joined query across MongoDB and Cassandra..."):
                collection = connect_mongodb(mongo_uri, mongodb_name, mongo_collection_name)
                session = connect_cassandra(cass_hosts, cass_keyspace)

                joined_results = joined_aggregate_query(collection, session)
                
                if joined_results:
                    for city_data in joined_results:
                        st.markdown(f"### {city_data['city']}")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("Total Customers", city_data['total_customers'])
                            st.metric("Average Age", f"{city_data['avg_age']:.1f}")
                        
                        with col2:
                            st.markdown("#### Customer Details")
                            customer_df = pd.DataFrame(city_data['customers'])
                            if not customer_df.empty:
                                st.dataframe(customer_df)
                            else:
                                st.info("No customers found in this city")
                        
                        st.divider()
                else:
                    st.warning("No results found for the joined query")

    if st.button("⚙️ Run Analysis"):
        with st.spinner("Running advanced queries and analytics..."):
            collection = connect_mongodb(mongo_uri, mongodb_name, mongo_collection_name)
            session = connect_cassandra(cass_hosts, cass_keyspace)

            # Basic Query
            cass_rows = query_cassandra(
                session, 
                intl_charge, 
                min_day_mins=min_day_mins,
                max_custserv_calls=max_custserv_calls,
                churn_filter=churn_filter,
                risk_factors=risk_factors
            )
            premium_users_cursor = collection.find({"City": city}, {"_id": 0, "Phone Number": 1})
            premium_ids = {str(doc["Phone Number"]) for doc in premium_users_cursor}
            result = [r for r in cass_rows if str(r["phone_number"]) in premium_ids]

            # Advanced Analytics
            mongo_stats = query_mongodb_aggregation(collection, min_age, group_by)

            # Display results in tabs
            with tab1:
                if result:
                    st.success(f"Found {len(result):,} high-risk customers")
                    
                    # Create risk analysis summary
                    total_customers = len(result)
                    high_risk = len([r for r in result if r.get('risk_score', 0) >= 2])
                    medium_risk = len([r for r in result if r.get('risk_score', 0) == 1])
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total High-Risk Customers", total_customers)
                    with col2:
                        st.metric("Severe Risk (Score ≥ 2)", high_risk)
                    with col3:
                        st.metric("Moderate Risk (Score = 1)", medium_risk)
                    
                    # Convert to DataFrame for better display
                    df = pd.DataFrame(result)
                    if 'risk_score' in df.columns:
                        df['risk_level'] = df['risk_score'].apply(
                            lambda x: 'Severe' if x >= 2 else 'Moderate'
                        )
                        
                    # Reorder columns for better presentation
                    cols_order = ['phone_number', 'risk_score', 'risk_level', 
                                'custserv_calls', 'intl_charge', 'day_mins', 
                                'intl_calls', 'churn']
                    display_cols = [col for col in cols_order if col in df.columns]
                    df = df[display_cols]
                    
                    st.markdown("### Detailed Risk Analysis")
                    st.dataframe(df.style.highlight_max(subset=['risk_score', 'custserv_calls', 'intl_charge'], 
                                                      color='red'))
                    
                    if high_risk > 0:
                        st.warning(f"⚠️ {high_risk} customers require immediate attention due to multiple risk factors!")
                else:
                    st.info("No high-risk customers found matching the criteria.")

            with tab2:
                if mongo_stats:
                    st.success(f"Found statistics grouped by {group_by}")
                    stats_df = pd.DataFrame(mongo_stats)
                    
                    # Create more readable view of the statistics
                    for stat in stats_df.to_dict('records'):
                        st.subheader(f"{group_by}: {stat['_id']}")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("Total Customers", stat['total_customers'])
                            st.metric("Average Age", f"{stat['avg_age']:.1f}")
                            st.metric("Age Range", f"{stat['min_age']} - {stat['max_age']}")
                            
                        with col2:
                            st.write("Gender Distribution:")
                            st.json(stat['gender_stats'])
                            st.write("Marital Status Distribution:")
                            st.json(stat['marital_stats'])
                        
                        st.divider()
                else:
                    st.info("No aggregation results found.")


if __name__ == "__main__":
    main()
