from utils import fetch_users, save_to_csv, load_csv_data_to_db, get_most_common_user_properties, \
    visualization_for_user_properties, fuzzy_and_strong_connection

CSV_FILENAME = "users.csv"
DB_NAME = "users.db"
TABLES_FOR_MOST_COMMON_USER_PROPERTIES = ["first_name", "last_name", "gender", "date_of_birth", "employment"]


if __name__ == '__main__':
    users = fetch_users()
    save_to_csv(CSV_FILENAME, users)
    load_csv_data_to_db(CSV_FILENAME, DB_NAME)
    most_common_user_properties = get_most_common_user_properties(DB_NAME, TABLES_FOR_MOST_COMMON_USER_PROPERTIES)
    visualization_for_user_properties(most_common_user_properties)
    fuzzy_and_strong_connection(DB_NAME)
