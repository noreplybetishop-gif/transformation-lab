/**
 * Shared datasets for Apache Spark & PySpark labs.
 */

export const USERS_CSV = `id,name,age,city,country,signup_date
1,Alice Johnson,28,New York,US,2023-01-15
2,Bob Smith,34,Toronto,CA,2023-02-20
3,Carol Williams,22,London,UK,2023-03-05
4,Dave Brown,45,Berlin,DE,2023-04-12
5,Eve Davis,31,Sydney,AU,2023-05-18
6,Frank Miller,29,Chicago,US,2023-06-25
7,Grace Wilson,38,Vancouver,CA,2023-07-01
8,Heidi Clark,26,Paris,FR,2023-08-14`

export const ORDERS_CSV = `order_id,user_id,amount,status,created_at
101,1,142.50,COMPLETED,2024-01-10 10:00:00
102,2,89.00,COMPLETED,2024-01-12 11:30:00
103,1,220.00,PENDING,2024-01-15 14:15:00
104,3,45.25,COMPLETED,2024-01-18 09:45:00
105,4,310.00,CANCELLED,2024-01-20 16:20:00
106,2,65.50,COMPLETED,2024-01-22 13:10:00
107,5,180.00,COMPLETED,2024-01-25 15:40:00
108,1,95.00,COMPLETED,2024-01-28 17:05:00`

export const WEB_LOGS_CSV = `log_id,ip_address,status_code,endpoint,response_ms
1,192.168.1.10,200,/api/v1/products,42
2,10.0.0.15,404,/api/v1/missing,18
3,192.168.1.10,200,/api/v1/cart,65
4,172.16.0.4,500,/api/v1/checkout,320
5,10.0.0.15,200,/api/v1/products,35
6,192.168.1.10,200,/api/v1/orders,88
7,172.16.0.4,200,/api/v1/products,40
8,192.168.1.25,200,/api/v1/login,55`

export const DIRTY_USERS_CSV = `id,name,email,country
1,Alice Johnson,alice@example.com,US
2,Bob Smith,,CA
3,,carol@example.com,UK
4,Dave Brown,dave@example.com,
5,Eve Davis,eve@example.com,AU
6,Frank Miller,,US`

export const EVENTS_JSON = `{"event_id": 1, "user_id": 101, "event_type": "page_view", "timestamp": "2024-02-01T10:00:00Z", "attributes": {"browser": "Chrome", "duration_sec": 120}}
{"event_id": 2, "user_id": 102, "event_type": "add_to_cart", "timestamp": "2024-02-01T10:05:00Z", "attributes": {"product_id": 405, "price": 49.99}}
{"event_id": 3, "user_id": 101, "event_type": "checkout", "timestamp": "2024-02-01T10:12:00Z", "attributes": {"order_total": 49.99, "payment_method": "credit_card"}}
{"event_id": 4, "user_id": 103, "event_type": "page_view", "timestamp": "2024-02-01T10:15:00Z", "attributes": {"browser": "Firefox", "duration_sec": 45}}
{"event_id": 5, "user_id": 104, "event_type": "search", "timestamp": "2024-02-01T10:20:00Z", "attributes": {"query": "wireless headphones", "results_count": 14}}`

export const DIM_STORE_CSV = `store_id,store_name,region,manager
S01,Downtown Flagship,East,Sarah Connor
S02,Metro Mall Hub,West,John Matrix
S03,Riverside Retail,Central,Ellen Ripley
S04,Airport Express,East,Kyle Reese`

export const DAILY_SALES_CSV = `date,store_id,sales_amount
2024-03-01,S01,4200.00
2024-03-01,S02,3800.50
2024-03-02,S01,4500.00
2024-03-02,S02,3900.00
2024-03-03,S01,5100.25
2024-03-03,S02,4100.00`

export const NESTED_USERS_JSON = `{"id": 1, "name": "Alice", "tags": ["premium", "engineer"], "address": {"city": "New York", "zip": "10001"}, "prefs": {"newsletter": "true", "theme": "dark"}}
{"id": 2, "name": "Bob", "tags": ["trial"], "address": {"city": "Toronto", "zip": "M5V2T6"}, "prefs": {"newsletter": "false", "theme": "light"}}
{"id": 3, "name": "Carol", "tags": ["premium", "designer", "vip"], "address": {"city": "London", "zip": "EC1A1BB"}, "prefs": {"newsletter": "true", "theme": "dark"}}
{"id": 4, "name": "Dave", "tags": [], "address": {"city": "Berlin", "zip": "10115"}, "prefs": {"newsletter": "false", "theme": "system"}}`

export const PRODUCTS_CSV = `product_id,name,category,price,inventory_count
101,Ultra Wireless Headphones,Electronics,129.99,450
102,Ergonomic Mechanical Keyboard,Electronics,89.50,1200
103,Ceramic Pour-Over Kettle,Kitchen,45.00,800
104,Organic Cold Brew 12-Pack,Groceries,29.99,2500
105,Noise Cancelling Earbuds,Electronics,79.99,650
106,Cast Iron Skillet 10-inch,Kitchen,38.50,300`

export const STREAMING_TRANSACTIONS_CSV = `tx_id,user_id,card_id,amount,merchant,location,timestamp
TX1001,1,C401,35.50,CoffeeShop,New York,2024-04-01 12:00:10
TX1002,2,C402,1200.00,LuxuryJewelry,Miami,2024-04-01 12:01:25
TX1003,1,C401,14.20,SubwayTransit,New York,2024-04-01 12:03:00
TX1004,3,C403,850.00,ElectronicsSuperstore,Las Vegas,2024-04-01 12:04:15
TX1005,4,C404,4.50,CornerMart,Chicago,2024-04-01 12:05:40
TX1006,2,C402,1450.00,LuxuryJewelry,Miami,2024-04-01 12:06:10
TX1007,5,C405,62.00,GasStation,Austin,2024-04-01 12:07:05
TX1008,1,C401,2500.00,OffshoreTransfer,Unknown,2024-04-01 12:08:30`

export const FRAUD_BLACKLIST_CSV = `merchant,risk_score,category
OffshoreTransfer,0.98,HIGH_RISK
LuxuryJewelry,0.75,MONITORED
CryptoExchange,0.95,HIGH_RISK
UnknownKiosk,0.85,SUSPICIOUS`

