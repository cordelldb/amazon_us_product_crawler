import mysql.connector
from itemadapter.adapter import ItemAdapter
from scrapy.exceptions import DropItem

class DatabasePipeline(object):
    
    def __init__(self):
        self.create_connection()
        
    def create_connection(self):
        self.connection = mysql.connector.connect(
            host = '74.208.230.203',
            user = 'cbreaux',
            password = 'Full$tack(74)',
            database = 'moloko_vellocet'
        )
        self.curr = self.connection.cursor()
        
    def process_item(self, item, spider):
        self.store_db(item)

        return item


    def store_db(self, item):

        keys = [1, "product_title", "platform_product_id", "manufacturer", "model_number", "part_number", None,
        None, "manufacturer", "buy_box_price", "image_url", "listing_url"]

        values = []
        for key in keys:
            if key is not None and key != "" and key != 1:
                if key == "buy_box_price":
                    try:
                        values.append(float(item.get(key, None)))
                    except:
                        values.append(float(0))
                else:
                    values.append(item.get(key, None))
            else:
                values.append(key)

        sql_query = """
            INSERT INTO products (client_id, product_title, ASIN, manufacturer, model, 
            partnumber, item_number, upc, publisher, price, image_url, product_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.curr.execute("SELECT ASIN FROM products WHERE ASIN = %s", (values[2],))
        existing_asin = self.curr.fetchone()

        if existing_asin:
            print(f"ASIN {values[2]} is already in the database. Skipping insertion.")
        else:
            self.curr.execute(sql_query, values)
            self.connection.commit()

            product_id = self.curr.lastrowid

            sql_query_2 = """
                INSERT INTO product_to_ids (product_id, market_id, platform_id)
                VALUES (%s, %s, %s)
            """
            values_2 = [product_id, 1, 1]

            self.curr.execute(sql_query_2, values_2)
            self.connection.commit()

class DuplicatesPipeline:

    def __init__(self):
        self.products_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter['platform_product_id'] in self.products_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.products_seen.add(adapter['platform_product_id'])
            return item

