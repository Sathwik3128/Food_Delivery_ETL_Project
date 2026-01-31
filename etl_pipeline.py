import pandas as pd
import sqlite3
import os

def run_etl_pipeline():
    print("🚀 Starting Data Merge Process...")

    # --- STEP 1: LOAD FILES ---
    
    # 1. Load CSV (Orders)
    if not os.path.exists('orders.csv'):
        print("❌ Error: orders.csv is missing!")
        return
    df_orders = pd.read_csv('orders.csv')
    print(f"✅ Loaded {len(df_orders)} orders.")

    # 2. Load JSON (Users)
    if not os.path.exists('users.json'):
        print("❌ Error: users.json is missing!")
        return
    # Try loading as standard JSON, fall back to line-delimited if needed
    try:
        df_users = pd.read_json('users.json')
    except ValueError:
        df_users = pd.read_json('users.json', lines=True)
    print(f"✅ Loaded {len(df_users)} users.")

    # 3. Load SQL (Restaurants)
    if not os.path.exists('restaurants.sql'):
        print("❌ Error: restaurants.sql is missing!")
        return
    
    # Create a temporary database to read the SQL file
    conn = sqlite3.connect(':memory:')
    with open('restaurants.sql', 'r') as f:
        sql_script = f.read()
        conn.executescript(sql_script)
    
    # Extract table to pandas (Assumes table name is 'restaurants')
    df_restaurants = pd.read_sql("SELECT * FROM restaurants", conn)
    print(f"✅ Loaded {len(df_restaurants)} restaurants.")


    # --- STEP 2: MERGE DATA ---
    
    print("\n🔗 Merging Data...")

    # Merge 1: Orders + Users (Left Join on 'user_id')
    # We use 'left' to keep every order, even if user info is missing
    df_step1 = pd.merge(df_orders, df_users, on='user_id', how='left')

    # Merge 2: (Orders+Users) + Restaurants (Left Join on 'restaurant_id')
    df_final = pd.merge(df_step1, df_restaurants, on='restaurant_id', how='left')


    # --- STEP 3: SAVE OUTPUT ---
    
    output_file = 'final_food_delivery_dataset.csv'
    df_final.to_csv(output_file, index=False)
    
    print(f"\n🎉 Success! File saved as: {output_file}")
    print(f"📊 Final Dataset Shape: {df_final.shape}")

if __name__ == "__main__":
    run_etl_pipeline()