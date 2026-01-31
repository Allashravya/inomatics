import pandas as pd
orders = pd.read_csv('orders.csv')
print(orders.head())
users = pd.read_json("users.json")
print(users.head())

import sqlite3
conn = sqlite3.connect("food.db")
with open("restaurants.sql", "r") as f:
    sql_script = f.read()
conn.executescript(sql_script)
restaurants = pd.read_sql("SELECT * FROM restaurants", conn)
print(restaurants.head())

merged_data = pd.merge(orders, users, on="user_id", how="left")

final_data = pd.merge(merged_data, restaurants, on="restaurant_id", how="left")

final_data.to_csv("final_fooddelivery.csv", index=False)
print("final_fooddelivery.csv created successfully.")

final_data[final_data["membership"] == "gold"] .groupby("city") ["total_amount"] .sum() .sort_values(ascending=False)

gold_city_amount = (
    final_data[final_data["membership"] == "Gold"]
    .sort_values(by="total_amount", ascending=True)
)

for _, row in gold_city_amount.iterrows():
    print(
        "City:", row["city"],
        "| Amount:", row["total_amount"]
    )

    avg_order_value = (
    final_data
    .groupby("cuisine")["total_amount"]
    .mean()
    .sort_values(ascending=False)
)

print(avg_order_value)

user_total_amount = (
    final_data
    .groupby("user_id")["total_amount"]
    .sum()
)
users_above_1000 = user_total_amount[user_total_amount > 1000]
count_users = users_above_1000.count()
print(count_users)

bins = [3.0, 3.5, 4.0, 4.5, 5.0]
labels = ["3.0-3.5", "3.6-4.0", "4.1-4.5", "4.6-5.0"]

final_data["rating_range"] = pd.cut(final_data["rating"], bins=bins, labels=labels, right=True)
revenue_by_rating = final_data.groupby("rating_range")["total_amount"].sum()
revenue_by_rating_sorted = revenue_by_rating.sort_values(ascending=False)
print(revenue_by_rating_sorted)
top_rating_range = revenue_by_rating.idxmax()
print("Rating range with highest revenue:", top_rating_range)

gold_data = final_data[final_data["membership"] == "Gold"]
avg_order_by_city = gold_data.groupby("city")["total_amount"].mean()
top_city = avg_order_by_city.idxmax()
print("City with highest average order value among Gold members:", top_city)

cuisine_stats = (
    final_data
    .groupby("cuisine")
    .agg(
        distinct_restaurants=("restaurant_id", "nunique"),
        total_revenue=("total_amount", "sum")
    )
    .sort_values("distinct_restaurants")
)
print(cuisine_stats)

gold_orders = final_data[final_data["membership"] == "Gold"].shape[0]
total_orders = final_data.shape[0]
percentage = round((gold_orders / total_orders) * 100)
print(f"Percentage of orders by Gold members: {percentage}%")

restaurant_stats = (
    final_data
    .groupby("restaurant_id")
    .agg(
        avg_order_value=("total_amount", "mean"),
        total_orders=("total_amount", "count")
    )
)

combo_revenue = final_data.groupby(["membership", "cuisine"])["total_amount"].sum()
combo_revenue_sorted = combo_revenue.sort_values(ascending=False)
print(combo_revenue_sorted)
top_combo = combo_revenue.idxmax()
print("Highest revenue combination:", top_combo)

gold_orders_count = final_data[final_data["membership"] == "Gold"].shape[0]
print("Number of orders placed by Gold members:", gold_orders_count)

hyderabad_revenue = final_data[final_data["city"] == "Hyderabad"]["total_amount"].sum()
hyderabad_revenue_rounded = round(hyderabad_revenue)
print(f"Total revenue from Hyderabad: {hyderabad_revenue_rounded}")
distinct_users = final_data["user_id"].nunique()
print(f"Number of distinct users who placed at least one order: {distinct_users}")

gold_avg_order_value = final_data[final_data["membership"] == "Gold"]["total_amount"].mean()
gold_avg_order_value_rounded = round(gold_avg_order_value, 2)
print(f"Average order value for Gold members: {gold_avg_order_value_rounded}")

high_rated_orders = final_data[final_data["rating"] >= 4.5].shape[0]
print(f"Number of orders for restaurants with rating ≥ 4.5: {high_rated_orders}")

top_city_gold_orders = final_data[(final_data["membership"] == "Gold") & (final_data["city"] == top_city)].shape[0]
print(f"Number of orders placed in {top_city} by Gold members: {top_city_gold_orders}")

top_revenue_city_gold = final_data[final_data["membership"] == "Gold"].groupby("city")["total_amount"].sum().idxmax()
top_revenue_city_gold_orders = final_data[(final_data["membership"] == "Gold") & (final_data["city"] == top_revenue_city_gold)].shape[0]
print(f"Number of orders placed in {top_revenue_city_gold} by Gold members: {top_revenue_city_gold_orders}")

