def load_existing_phones(campaign_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = 
        SELECT DISTINCT phone_number
        FROM vicidial_list
        WHERE phone_number IS NOT NULL
          AND campaign_id = %s
    
    cursor.execute(query, (campaign_id,))

    phones = set()
    for (phone,) in cursor.fetchall():
        cleaned = clean_phone(phone)
        if cleaned:
            phones.add(cleaned)

    cursor.close()
    conn.close()
    return phones
