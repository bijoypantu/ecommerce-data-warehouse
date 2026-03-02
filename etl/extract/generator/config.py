# etl/extract/generator/config.py
# ============================================================
# All hardcoded constants, lookup tables, and configuration
# that the generator scripts depend on.
# ============================================================

from datetime import date

# ------------------------------------------------------------
# Date boundaries
# ------------------------------------------------------------
START_DATE = date(2020, 1, 1)
END_DATE   = date(2026, 3, 1)
SCD2_CUTOFF = date(2026, 1, 1)  # orders before this → 85/15 split

# ------------------------------------------------------------
# Volume constants
# ------------------------------------------------------------
NUM_CUSTOMERS   = 10_000
NUM_PRODUCTS    = 1_000
NUM_ORDERS      = 100_000
REFUND_RATE     = 0.10   # 10% of delivered orders get refunds
SCD2_CUST_RATE  = 0.05   # 5% of customers get a second version
SCD2_PROD_RATE  = 0.20   # 20% of products get discontinued

# ------------------------------------------------------------
# Discount details
# ------------------------------------------------------------
DISCOUNT_TIERS = [0.0, 0.05, 0.10, 0.15, 0.20, 0.30, 0.40, 0.50]
DISCOUNT_WEIGHTS = [0.30, 0.25, 0.20, 0.10, 0.07, 0.04, 0.02, 0.02]

# ------------------------------------------------------------
# Categories — (category_id, category_name, parent_id)
# parent_id = None means it's a top-level category
# ------------------------------------------------------------
CATEGORIES = [
    # Parents
    ("CAT-00001", "Electronics",              None),
    ("CAT-00002", "Fashion",                  None),
    ("CAT-00003", "Footwear",                 None),
    ("CAT-00004", "Home & Kitchen",           None),
    ("CAT-00005", "Home Appliances",          None),
    ("CAT-00006", "Beauty & Personal Care",   None),
    ("CAT-00007", "Sports & Fitness",         None),
    ("CAT-00008", "Books & Stationery",       None),
    ("CAT-00009", "Toys & Games",             None),
    ("CAT-00010", "Accessories",              None),

    # Electronics sub-categories
    ("CAT-00011", "Smartphones",        "CAT-00001"),
    ("CAT-00012", "Laptops",            "CAT-00001"),
    ("CAT-00013", "Tablets",            "CAT-00001"),
    ("CAT-00014", "Cameras",            "CAT-00001"),
    ("CAT-00015", "Televisions",        "CAT-00001"),

    # Fashion sub-categories
    ("CAT-00016", "Men's Clothing",     "CAT-00002"),
    ("CAT-00017", "Women's Clothing",   "CAT-00002"),
    ("CAT-00018", "Kid's Clothing",     "CAT-00002"),
    ("CAT-00019", "Ethnic Wear",        "CAT-00002"),
    ("CAT-00020", "Activewear",         "CAT-00002"),

    # Footwear sub-categories
    ("CAT-00021", "Men's Footwear",     "CAT-00003"),
    ("CAT-00022", "Women's Footwear",   "CAT-00003"),
    ("CAT-00023", "Kid's Footwear",     "CAT-00003"),
    ("CAT-00024", "Sports Shoes",       "CAT-00003"),
    ("CAT-00025", "Sandals & Slippers", "CAT-00003"),

    # Home & Kitchen sub-categories
    ("CAT-00026", "Cookware",               "CAT-00004"),
    ("CAT-00027", "Storage & Organisation", "CAT-00004"),
    ("CAT-00028", "Bedding",                "CAT-00004"),
    ("CAT-00029", "Bathroom",               "CAT-00004"),
    ("CAT-00030", "Dining",                 "CAT-00004"),

    # Home Appliances sub-categories
    ("CAT-00031", "Washing Machines",   "CAT-00005"),
    ("CAT-00032", "Refrigerators",      "CAT-00005"),
    ("CAT-00033", "Air Conditioners",   "CAT-00005"),
    ("CAT-00034", "Microwaves",         "CAT-00005"),
    ("CAT-00035", "Vacuum Cleaners",    "CAT-00005"),

    # Beauty & Personal Care sub-categories
    ("CAT-00036", "Skincare",           "CAT-00006"),
    ("CAT-00037", "Haircare",           "CAT-00006"),
    ("CAT-00038", "Fragrances",         "CAT-00006"),
    ("CAT-00039", "Men's Grooming",     "CAT-00006"),
    ("CAT-00040", "Makeup",             "CAT-00006"),

    # Sports & Fitness sub-categories
    ("CAT-00041", "Exercise Equipment", "CAT-00007"),
    ("CAT-00042", "Outdoor Sports",     "CAT-00007"),
    ("CAT-00043", "Yoga & Meditation",  "CAT-00007"),
    ("CAT-00044", "Cycling",            "CAT-00007"),
    ("CAT-00045", "Swimming",           "CAT-00007"),

    # Books & Stationery sub-categories
    ("CAT-00046", "Fiction",            "CAT-00008"),
    ("CAT-00047", "Non-Fiction",        "CAT-00008"),
    ("CAT-00048", "Academic",           "CAT-00008"),
    ("CAT-00049", "Office Supplies",    "CAT-00008"),
    ("CAT-00050", "Art Supplies",       "CAT-00008"),

    # Toys & Games sub-categories
    ("CAT-00051", "Board Games",        "CAT-00009"),
    ("CAT-00052", "Action Figures",     "CAT-00009"),
    ("CAT-00053", "Educational Toys",   "CAT-00009"),
    ("CAT-00054", "Outdoor Toys",       "CAT-00009"),
    ("CAT-00055", "Puzzles",            "CAT-00009"),

    # Accessories sub-categories
    ("CAT-00056", "Bags & Luggage",     "CAT-00010"),
    ("CAT-00057", "Watches",            "CAT-00010"),
    ("CAT-00058", "Jewellery",          "CAT-00010"),
    ("CAT-00059", "Sunglasses",         "CAT-00010"),
    ("CAT-00060", "Wallets & Belts",    "CAT-00010"),
]

# ------------------------------------------------------------
# Brands per sub-category
# ------------------------------------------------------------
BRANDS = {
    "Smartphones":          ["Apple", "Samsung", "Xiaomi", "Vivo", "Nokia", "Oppo"],
    "Laptops":              ["Apple", "HP", "Asus", "Lenovo", "Dell"],
    "Tablets":              ["Apple", "Samsung", "Xiaomi", "Honor", "OnePlus"],
    "Cameras":              ["Panasonic", "Kodak", "Nikon", "Sony", "Canon"],
    "Televisions":          ["Apple", "Samsung", "Xiaomi", "Sony", "LG"],
    "Men's Clothing":       ["Zara", "Gucci", "Louis Vuitton", "Balenciaga", "Hermès", "Dollar"],
    "Women's Clothing":     ["Zara", "Gucci", "Louis Vuitton", "Balenciaga", "Hermès", "Dollar"],
    "Kid's Clothing":       ["Zara", "Gucci", "Louis Vuitton", "Balenciaga", "Hermès", "Dollar"],
    "Ethnic Wear":          ["Zara", "Gucci", "Louis Vuitton", "Balenciaga", "Hermès", "Dollar"],
    "Activewear":           ["Zara", "Gucci", "Louis Vuitton", "Balenciaga", "Hermès", "Dollar"],
    "Men's Footwear":       ["Nike", "Adidas", "Puma", "Bata", "Woodland"],
    "Women's Footwear":     ["Nike", "Adidas", "Puma", "Bata", "Woodland"],
    "Kid's Footwear":       ["Nike", "Adidas", "Puma", "Bata", "Woodland"],
    "Sports Shoes":         ["Nike", "Adidas", "Puma", "Bata", "Woodland"],
    "Sandals & Slippers":   ["Nike", "Adidas", "Puma", "Bata", "Woodland"],
    "Cookware":             ["IKEA", "Prestige", "Milton", "Cello", "Borosil"],
    "Storage & Organisation": ["IKEA", "Prestige", "Milton", "Cello", "Borosil"],
    "Bedding":              ["IKEA", "Prestige", "Milton", "Cello", "Borosil"],
    "Bathroom":             ["IKEA", "Prestige", "Milton", "Cello", "Borosil"],
    "Dining":               ["IKEA", "Prestige", "Milton", "Cello", "Borosil"],
    "Washing Machines":     ["LG", "Whirlpool", "Samsung", "Bosch", "Philips"],
    "Refrigerators":        ["LG", "Whirlpool", "Samsung", "Bosch", "Philips"],
    "Air Conditioners":     ["LG", "Whirlpool", "Samsung", "Bosch", "Philips"],
    "Microwaves":           ["LG", "Whirlpool", "Samsung", "Bosch", "Philips"],
    "Vacuum Cleaners":      ["LG", "Whirlpool", "Samsung", "Bosch", "Philips"],
    "Skincare":             ["Lakme", "L'Oréal", "Nivea", "Dove", "The Body Shop"],
    "Haircare":             ["Lakme", "L'Oréal", "Nivea", "Dove", "The Body Shop"],
    "Fragrances":           ["Lakme", "L'Oréal", "Nivea", "Dove", "The Body Shop"],
    "Men's Grooming":       ["Lakme", "L'Oréal", "Nivea", "Dove", "The Body Shop"],
    "Makeup":               ["Lakme", "L'Oréal", "Nivea", "Dove", "The Body Shop"],
    "Exercise Equipment":   ["Decathlon", "Cosco", "Nivia", "Boldfit", "Strauss"],
    "Outdoor Sports":       ["Decathlon", "Cosco", "Nivia", "Boldfit", "Strauss"],
    "Yoga & Meditation":    ["Decathlon", "Cosco", "Nivia", "Boldfit", "Strauss"],
    "Cycling":              ["Decathlon", "Cosco", "Nivia", "Boldfit", "Strauss"],
    "Swimming":             ["Decathlon", "Cosco", "Nivia", "Boldfit", "Strauss"],
    "Fiction":              ["Penguin", "Harper Collins", "Classmate", "Camlin", "Natraj"],
    "Non-Fiction":          ["Penguin", "Harper Collins", "Classmate", "Camlin", "Natraj"],
    "Academic":             ["Penguin", "Harper Collins", "Classmate", "Camlin", "Natraj"],
    "Office Supplies":      ["Penguin", "Harper Collins", "Classmate", "Camlin", "Natraj"],
    "Art Supplies":         ["Penguin", "Harper Collins", "Classmate", "Camlin", "Natraj"],
    "Board Games":          ["Lego", "Mattel", "Funskool", "Fisher-Price", "Hasbro"],
    "Action Figures":       ["Lego", "Mattel", "Funskool", "Fisher-Price", "Hasbro"],
    "Educational Toys":     ["Lego", "Mattel", "Funskool", "Fisher-Price", "Hasbro"],
    "Outdoor Toys":         ["Lego", "Mattel", "Funskool", "Fisher-Price", "Hasbro"],
    "Puzzles":              ["Lego", "Mattel", "Funskool", "Fisher-Price", "Hasbro"],
    "Bags & Luggage":       ["Fossil", "Titan", "Fastrack", "Wildcraft", "Baggit"],
    "Watches":              ["Fossil", "Titan", "Fastrack", "Wildcraft", "Baggit"],
    "Jewellery":            ["Fossil", "Titan", "Fastrack", "Wildcraft", "Baggit"],
    "Sunglasses":           ["Fossil", "Titan", "Fastrack", "Wildcraft", "Baggit"],
    "Wallets & Belts":      ["Fossil", "Titan", "Fastrack", "Wildcraft", "Baggit"],
}

# ------------------------------------------------------------
# Price ranges per sub-category (INR)
# ------------------------------------------------------------
PRICE_RANGES = {
    "Smartphones":          (8_000,   150_000),
    "Laptops":              (30_000,  200_000),
    "Tablets":              (10_000,  80_000),
    "Cameras":              (15_000,  200_000),
    "Televisions":          (10_000,  300_000),
    "Men's Clothing":       (300,     15_000),
    "Women's Clothing":     (300,     15_000),
    "Kid's Clothing":       (200,     5_000),
    "Ethnic Wear":          (500,     50_000),
    "Activewear":           (500,     10_000),
    "Men's Footwear":       (500,     20_000),
    "Women's Footwear":     (500,     20_000),
    "Kid's Footwear":       (300,     5_000),
    "Sports Shoes":         (1_000,   15_000),
    "Sandals & Slippers":   (200,     3_000),
    "Cookware":             (300,     10_000),
    "Storage & Organisation": (200,   5_000),
    "Bedding":              (500,     10_000),
    "Bathroom":             (200,     5_000),
    "Dining":               (300,     8_000),
    "Washing Machines":     (15_000,  80_000),
    "Refrigerators":        (15_000,  100_000),
    "Air Conditioners":     (25_000,  150_000),
    "Microwaves":           (5_000,   30_000),
    "Vacuum Cleaners":      (3_000,   30_000),
    "Skincare":             (200,     5_000),
    "Haircare":             (150,     3_000),
    "Fragrances":           (500,     20_000),
    "Men's Grooming":       (200,     3_000),
    "Makeup":               (300,     8_000),
    "Exercise Equipment":   (500,     50_000),
    "Outdoor Sports":       (500,     20_000),
    "Yoga & Meditation":    (300,     5_000),
    "Cycling":              (3_000,   50_000),
    "Swimming":             (300,     5_000),
    "Fiction":              (100,     1_000),
    "Non-Fiction":          (100,     1_500),
    "Academic":             (200,     2_000),
    "Office Supplies":      (50,      2_000),
    "Art Supplies":         (100,     3_000),
    "Board Games":          (300,     5_000),
    "Action Figures":       (200,     3_000),
    "Educational Toys":     (300,     5_000),
    "Outdoor Toys":         (200,     3_000),
    "Puzzles":              (150,     2_000),
    "Bags & Luggage":       (500,     50_000),
    "Watches":              (500,     500_000),
    "Jewellery":            (500,     200_000),
    "Sunglasses":           (300,     20_000),
    "Wallets & Belts":      (200,     5_000),
}

# ------------------------------------------------------------
# Colors (generic across all products)
# ------------------------------------------------------------
COLORS = [
    "Black", "White", "Red", "Blue", "Green",
    "Yellow", "Grey", "Brown", "Pink", "Purple",
    "Orange", "Beige", "Navy", "Maroon", "Gold"
]

# ------------------------------------------------------------
# Sizes per sub-category
# None means the product has no size attribute
# ------------------------------------------------------------
SIZES = {
    "Smartphones":          None,
    "Laptops":              ['13"', '14"', '15"', '16"', '17"'],
    "Tablets":              ['8"', '10"', '11"', '12"'],
    "Cameras":              None,
    "Televisions":          ['32"', '43"', '50"', '55"', '65"', '75"'],
    "Men's Clothing":       ["XS", "S", "M", "L", "XL", "XXL"],
    "Women's Clothing":     ["XS", "S", "M", "L", "XL", "XXL"],
    "Kid's Clothing":       ["2-3Y", "3-4Y", "4-5Y", "5-6Y", "6-7Y", "7-8Y"],
    "Ethnic Wear":          ["XS", "S", "M", "L", "XL", "XXL"],
    "Activewear":           ["XS", "S", "M", "L", "XL", "XXL"],
    "Men's Footwear":       ["6", "7", "8", "9", "10", "11", "12"],
    "Women's Footwear":     ["4", "5", "6", "7", "8", "9"],
    "Kid's Footwear":       ["1C", "2C", "3C", "4C", "5C", "6C"],
    "Sports Shoes":         ["6", "7", "8", "9", "10", "11"],
    "Sandals & Slippers":   ["6", "7", "8", "9", "10"],
    "Cookware":             None,
    "Storage & Organisation": None,
    "Bedding":              ["Single", "Double", "Queen", "King"],
    "Bathroom":             None,
    "Dining":               None,
    "Washing Machines":     ["6kg", "7kg", "8kg", "9kg", "10kg"],
    "Refrigerators":        ["200L", "300L", "400L", "500L", "600L"],
    "Air Conditioners":     ["1 Ton", "1.5 Ton", "2 Ton"],
    "Microwaves":           ["17L", "20L", "25L", "30L", "32L"],
    "Vacuum Cleaners":      None,
    "Skincare":             None,
    "Haircare":             None,
    "Fragrances":           ["30ml", "50ml", "75ml", "100ml"],
    "Men's Grooming":       None,
    "Makeup":               None,
    "Exercise Equipment":   None,
    "Outdoor Sports":       None,
    "Yoga & Meditation":    None,
    "Cycling":              None,
    "Swimming":             None,
    "Fiction":              None,
    "Non-Fiction":          None,
    "Academic":             None,
    "Office Supplies":      None,
    "Art Supplies":         None,
    "Board Games":          None,
    "Action Figures":       None,
    "Educational Toys":     None,
    "Outdoor Toys":         None,
    "Puzzles":              None,
    "Bags & Luggage":       None,
    "Watches":              None,
    "Jewellery":            None,
    "Sunglasses":           None,
    "Wallets & Belts":      None,
}

# ------------------------------------------------------------
# Country → currency mapping
# ------------------------------------------------------------
COUNTRY_CURRENCY = {
    "India":          "INR",
    "United States":  "USD",
    "United Kingdom": "GBP",
    "Germany":        "EUR",
    "France":         "EUR",
    "UAE":            "AED",
    "Saudi Arabia":   "SAR",
    "Japan":          "JPY",
    "China":          "CNY",
    "Singapore":      "SGD",
    "Australia":      "AUD",
    "Canada":         "CAD",
    "Brazil":         "BRL",
    "Mexico":         "MXN",
    "Hong Kong":      "HKD",
    "South Korea":    "KRW",
    "Switzerland":    "CHF",
    "Sweden":         "SEK",
}

# Country → Faker locale mapping
COUNTRY_LOCALE = {
    "India":          "en_IN",
    "United States":  "en_US",
    "United Kingdom": "en_GB",
    "Germany":        "de_DE",
    "France":         "fr_FR",
    "UAE":            "en_US",
    "Saudi Arabia":   "en_US",
    "Japan":          "ja_JP",
    "China":          "zh_CN",
    "Singapore":      "en_US",
    "Australia":      "en_AU",
    "Canada":         "en_CA",
    "Brazil":         "pt_BR",
    "Mexico":         "es_MX",
    "Hong Kong":      "en_US",
    "South Korea":    "ko_KR",
    "Switzerland":    "de_CH",
    "Sweden":         "sv_SE",
}

# Countries weighted towards India (largest customer base)
COUNTRY_WEIGHTS = {
    "India":          0.40,
    "United States":  0.15,
    "United Kingdom": 0.07,
    "Germany":        0.05,
    "France":         0.04,
    "UAE":            0.05,
    "Saudi Arabia":   0.03,
    "Japan":          0.03,
    "China":          0.03,
    "Singapore":      0.02,
    "Australia":      0.02,
    "Canada":         0.02,
    "Brazil":         0.02,
    "Mexico":         0.02,
    "Hong Kong":      0.01,
    "South Korea":    0.01,
    "Switzerland":    0.01,
    "Sweden":         0.01,
}

COUNTRY_MOBILE_CODE = {
    "India":          "+91",
    "United States":  "+1",
    "United Kingdom": "+44",
    "Germany":        "+49",
    "France":         "+33",
    "UAE":            "+971",
    "Saudi Arabia":   "+966",
    "Japan":          "+81",
    "China":          "+86",
    "Singapore":      "+65",
    "Australia":      "+61",
    "Canada":         "+1",
    "Brazil":         "+55",
    "Mexico":         "+52",
    "Hong Kong":      "+852",
    "South Korea":    "+82",
    "Switzerland":    "+41",
    "Sweden":         "+46",
}

MAIL_DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "zohomail.com"]

# ------------------------------------------------------------
# Payment configuration
# ------------------------------------------------------------
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "cod", "wallet"]

PAYMENT_PROVIDERS = {
    "credit_card":  ["Visa", "Mastercard", "Amex", "RuPay"],
    "debit_card":   ["Visa", "Mastercard", "RuPay", "Maestro"],
    "upi":          ["GPay", "PhonePe", "Paytm", "BHIM", "Amazon Pay"],
    "net_banking":  ["HDFC", "SBI", "ICICI", "Axis", "Kotak"],
    "cod":          [None],
    "wallet":       ["Paytm", "Amazon Pay", "Mobikwik", "Freecharge"],
}

GATEWAY_CODES = {
    "success":   ["00", "000", "APPROVED"],
    "failed":    ["51", "54", "05", "14", "NSF", "DECLINED"],
    "pending":   [None],
    "cancelled": ["USER_CANCELLED", "TIMEOUT", "SESSION_EXPIRED"],
}

CARRIERS = [
    "FedEx", "DHL", "UPS", "BlueDart", "Delhivery",
    "Ecom Express", "XpressBees", "Shadowfax", "DTDC", "India Post"
]

REFUND_REASONS = [
    "Damaged product",
    "Wrong product delivered",
    "Product quality not as described",
    "Changed my mind",
    "Better price available elsewhere",
    "Item arrived too late",
    "Duplicate order",
]
