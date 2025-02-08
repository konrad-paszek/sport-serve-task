import csv
import time
from difflib import SequenceMatcher

import pandas as pd
from typing import Any
from sqlalchemy import create_engine
import requests
import sqlite3
import difflib
import matplotlib.pyplot as plt
import networkx as nx

URL = "https://random-data-api.com/api/v2/users"
SQL_QUERY_FOR_MOST_COMMON_USER_PROPERTIES = """SELECT '{}' AS property, {} AS value, COUNT(*) AS frequency
 FROM users
 GROUP BY {}
 ORDER BY frequency DESC
 LIMIT 1"""

def fetch_users() -> list[dict[str, Any]]:
    users = []
    payload = {"size": "100", "is_json": True}
    for i in range(10):
        response = requests.get(URL, params=payload)
        users.extend(response.json())
        time.sleep(5)
    return users

def save_to_csv(filename: str, data: list) -> None:
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

def load_csv_data_to_db(filename: str, db_name: str):
    df = pd.read_csv(filename)
    engine = create_engine(f'sqlite:///{db_name}')
    df.to_sql('users', con=engine, if_exists='replace', index=False)

def get_most_common_user_properties(db_name: str, tables: list) -> list[dict[str, Any]]:
    most_common_user_properties = []
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(SQL_QUERY_FOR_MOST_COMMON_USER_PROPERTIES.format(table, table, table))
        most_common_user_properties.extend(cursor.fetchall())
    conn.close()
    return most_common_user_properties

def visualization_for_user_properties(user_properties: list) -> None:
    properties = [item[0] for item in user_properties]
    values = [item[1] for item in user_properties]
    frequencies = [item[2] for item in user_properties]
    plt.figure(figsize=(10, 6))
    bars = plt.bar(properties, frequencies, color='skyblue')
    for bar, common_value in zip(bars, values):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            common_value,
            ha='center',
            va='bottom',
            fontsize=9,
            rotation=45
        )
    plt.xlabel("Property")
    plt.ylabel("Frequency")
    plt.title("Most Common User Properties")
    plt.ylim(0, max(frequencies) + 1)
    plt.tight_layout()
    plt.show()


def similarity(a: str, b: str) -> SequenceMatcher.ratio:
    return difflib.SequenceMatcher(None, a, b).ratio()


def fuzzy_and_strong_connection(db_name: str) -> None:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, first_name, last_name, email, phone_number, social_insurance_number, date_of_birth, employment
        FROM users
    """)
    rows = cursor.fetchall()
    conn.close()
    users = []
    for row in rows:
        user = {
            'id': row[0],
            'first_name': row[1],
            'last_name': row[2],
            'email': row[3],
            'phone_number': row[4],
            'social_insurance_number': row[5],
            'date_of_birth': row[6],
            'employment': row[7]
        }
        users.append(user)
    strong_connections = []
    fuzzy_connections = []
    for i in range(len(users)):
        for j in range(i + 1, len(users)):
            u1 = users[i]
            u2 = users[j]
            if u1['email'] and u2['email'] and u1['email'] == u2['email']:
                strong_connections.append((u1['id'], u2['id'], 'email', u1['email']))
            if u1['phone_number'] and u2['phone_number'] and u1['phone_number'] == u2['phone_number']:
                strong_connections.append((u1['id'], u2['id'], 'phone_number', u1['phone_number']))
            if u1['social_insurance_number'] and u2['social_insurance_number'] and u1['social_insurance_number'] == u2[
                'social_insurance_number']:
                strong_connections.append(
                    (u1['id'], u2['id'], 'social_insurance_number', u1['social_insurance_number']))
            if (u1['first_name'] and u2['first_name'] and
                    u1['last_name'] and u2['last_name'] and
                    u1['date_of_birth'] and u2['date_of_birth']):
                if (u1['first_name'] == u2['first_name'] and
                        u1['last_name'] == u2['last_name'] and
                        u1['date_of_birth'] == u2['date_of_birth']):
                    combined = f"{u1['first_name']} {u1['last_name']} {u1['date_of_birth']}"
                    strong_connections.append((u1['id'], u2['id'], 'name+dob', combined))
            fuzzy_score = 0
            fuzzy_reasons = []
            if u1['first_name'] and u2['first_name'] and u1['first_name'] != u2['first_name']:
                sim_fn = similarity(u1['first_name'].lower(), u2['first_name'].lower())
                if sim_fn >= 0.85:
                    fuzzy_score += 1
                    fuzzy_reasons.append(f"first_name {sim_fn:.2f}")
            if u1['last_name'] and u2['last_name'] and u1['last_name'] != u2['last_name']:
                sim_ln = similarity(u1['last_name'].lower(), u2['last_name'].lower())
                if sim_ln >= 0.85:
                    fuzzy_score += 1
                    fuzzy_reasons.append(f"last_name {sim_ln:.2f}")
            if u1['employment'] and u2['employment'] and u1['employment'] != u2['employment']:
                sim_emp = similarity(u1['employment'].lower(), u2['employment'].lower())
                if sim_emp >= 0.75:
                    fuzzy_score += 1
                    fuzzy_reasons.append(f"employment {sim_emp:.2f}")
            if fuzzy_score >= 2:
                fuzzy_connections.append((u1['id'], u2['id'], fuzzy_score, ", ".join(fuzzy_reasons)))
    G_strong = nx.Graph()
    for conn in strong_connections:
        id1, id2, field, value = conn
        G_strong.add_edge(id1, id2, label=f"{field}: {value}")
    G_fuzzy = nx.Graph()
    for conn in fuzzy_connections:
        id1, id2, score, reasons = conn
        G_fuzzy.add_edge(id1, id2, label=f"score: {score} ({reasons})")
    plt.figure(figsize=(14, 6))
    plt.subplot(1, 2, 1)
    pos_strong = nx.spring_layout(G_strong, seed=42)
    nx.draw(G_strong, pos_strong, with_labels=True, node_color='lightblue', edge_color='green', node_size=500,
            font_size=10)
    edge_labels_strong = nx.get_edge_attributes(G_strong, 'label')
    nx.draw_networkx_edge_labels(G_strong, pos_strong, edge_labels=edge_labels_strong, font_color='red')
    plt.title("Strong Connections")
    plt.subplot(1, 2, 2)
    pos_fuzzy = nx.spring_layout(G_fuzzy, seed=42)
    nx.draw(G_fuzzy, pos_fuzzy, with_labels=True, node_color='lightyellow', edge_color='orange', node_size=500,
            font_size=10)
    edge_labels_fuzzy = nx.get_edge_attributes(G_fuzzy, 'label')
    nx.draw_networkx_edge_labels(G_fuzzy, pos_fuzzy, edge_labels=edge_labels_fuzzy, font_color='blue')
    plt.title("Fuzzy Connections")
    plt.tight_layout()
    plt.show()
