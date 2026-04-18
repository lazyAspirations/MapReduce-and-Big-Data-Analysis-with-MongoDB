import csv
from pathlib import Path
from pymongo import MongoClient

base_dir = Path(__file__).parent
client = MongoClient('mongodb://127.0.0.1:27017')
db = client['universite']

# Import pet_supplies.csv into sales_logs
sales_path = base_dir / 'pet_supplies.csv'
with sales_path.open(newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    docs = []
    for row in reader:
        docs.append({
            'title': row['title'],
            'averageStar': float(row['averageStar']) if row['averageStar'] else None,
            'quantity': int(row['quantity']) if row['quantity'].isdigit() else None,
            'tradeAmount': row['tradeAmount'],
            'wishedCount': int(row['wishedCount']) if row['wishedCount'].isdigit() else None,
        })
    db.sales_logs.drop()
    if docs:
        db.sales_logs.insert_many(docs)
    print(f'Imported {len(docs)} documents into sales_logs')

# Import customer_shopping_behavior.csv into dirty_data and transactions
behavior_path = base_dir / 'customer_shopping_behavior.csv'
with behavior_path.open(newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    dirty_docs = []
    for row in reader:
        def to_int(value):
            try:
                return int(value)
            except Exception:
                return None
        def to_float(value):
            try:
                return float(value)
            except Exception:
                return None
        dirty_docs.append({
            'customer_id': to_int(row.get('Customer ID', '').strip()),
            'age': to_int(row.get('Age', '').strip()),
            'gender': row.get('Gender', '').strip(),
            'item_purchased': row.get('Item Purchased', '').strip(),
            'category': row.get('Category', '').strip(),
            'purchase_amount_usd': to_float(row.get('Purchase Amount (USD)', '').strip()),
            'location': row.get('Location', '').strip(),
            'size': row.get('Size', '').strip(),
            'color': row.get('Color', '').strip(),
            'season': row.get('Season', '').strip(),
            'review_rating': to_float(row.get('Review Rating', '').strip()),
            'subscription_status': row.get('Subscription Status', '').strip(),
            'shipping_type': row.get('Shipping Type', '').strip(),
            'discount_applied': row.get('Discount Applied', '').strip(),
            'previous_purchases': to_int(row.get('Previous Purchases', '').strip()),
            'payment_method': row.get('Payment Method', '').strip(),
            'frequency_of_purchases': row.get('Frequency of Purchases', '').strip(),
        })
    db.dirty_data.drop()
    db.transactions.drop()
    if dirty_docs:
        db.dirty_data.insert_many(dirty_docs)
        db.transactions.insert_many(dirty_docs)
    print(f'Imported {len(dirty_docs)} documents into dirty_data and transactions')

# Import 01_company_info.csv into companies
companies_path = base_dir / '01_company_info.csv'
with companies_path.open(newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    company_docs = []
    for row in reader:
        company_docs.append({
            'symbol': row.get('symbol', '').strip(),
            'city': row.get('city', '').strip(),
            'country': row.get('country', '').strip(),
            'industry': row.get('industry', '').strip(),
            'sector': row.get('sector', '').strip(),
            'industryKey': row.get('industryKey', '').strip(),
            'sectorKey': row.get('sectorKey', '').strip(),
            'marketCap': float(row['marketCap']) if row.get('marketCap') else None,
            'currentPrice': float(row['currentPrice']) if row.get('currentPrice') else None,
        })
    db.companies.drop()
    if company_docs:
        db.companies.insert_many(company_docs)
    print(f'Imported {len(company_docs)} documents into companies')
