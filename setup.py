from modules import db_fetcher
import random

TABLE = "notification"
KEY="notification_id"
FIELD = "is_read"

# step 1:  id 조회
print(f"[1] Fetching notification ids...")
total_ids = [row[0] for row in db_fetcher.fetch_columns(TABLE, [KEY])]
sampled_ids=random.sample(total_ids,len(total_ids)//4)

print(len(sampled_ids))

conn, cursor = db_fetcher.get_connection_and_cursor()

placeholders = ', '.join(['%s'] * len(sampled_ids))
query = f"""
    UPDATE notification
    SET is_read = %s
    WHERE notification_id IN ({placeholders})
"""
params = [True] + sampled_ids

cursor.execute(query, params)
conn.commit()
cursor.close()
conn.close()