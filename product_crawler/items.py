import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, Join, MapCompose, Compose
import re

class AmazonProduct(scrapy.Item):
    platform_product_id = scrapy.Field()
    product_title = scrapy.Field()
    buy_box_price = scrapy.Field()
    currency = scrapy.Field()
    model_number = scrapy.Field()
    listing_url = scrapy.Field()
    manufacturer_name = scrapy.Field()
    brand = scrapy.Field()
    part_number = scrapy.Field()
    model_name = scrapy.Field()
    product_dimensions = scrapy.Field()
    ratings_count = scrapy.Field()
    average_rating = scrapy.Field()
    image_url = scrapy.Field()
    product_weight = scrapy.Field()
    platform_marketplace_id = scrapy.Field()
    seller_url = scrapy.Field()
    buy_box_seller = scrapy.Field()
    platform = scrapy.Field()
    upc = scrapy.Field()



def handle_data(value):
    if value is None:
        return ""
    value = value.strip().replace('\u200e', '')
    return value

def handle_none(value):
    if value is None:
        return ""
    return value
       
def remove_ratings(value):
    if value is None:
        return ""
    cleaned_text = re.sub(r'\brating\b|\bratings\b', '', value, flags=re.IGNORECASE)
    return ' '.join(cleaned_text.split())

def clean_url(url):
    ref_index = url.find("ref=")
    if ref_index != -1:
        return url[:ref_index]
    else:
        return url
    
def clean_img(url):
    cleaned_url = re.sub(r'(\.[^\.]+)\.jpg$', r'.jpg', url)
    return cleaned_url

def remove_commas(text):
    return text.replace(",", "")

def remove_rating_text(input_string):
    cleaned_string = input_string.replace("out of 5 stars", "").strip()
    return cleaned_string



class AmazonProductLoader(ItemLoader):
    
    default_output_processor = TakeFirst()


    def add_css_with_default(self, field_name, css, default_value=None):
        self.add_css(field_name, css)
        if not self.get_output_value(field_name):
            self.add_value(field_name, default_value)
    
    product_title_in = MapCompose(str.strip)
    buy_box_price_in = MapCompose(lambda x: x.split("$")[-1])
    listing_url_in = MapCompose(clean_url)
    manufacturer_name_in = MapCompose(handle_data)
    brand_in = MapCompose(handle_data)
    part_number_in = MapCompose(handle_data)
    upc_in = MapCompose(handle_data)
    model_number_in = MapCompose(handle_data)
    # model_name_in = MapCompose(handle_data, remove_commas)
    ratings_count_in = MapCompose(remove_ratings)
    average_rating_in = MapCompose(handle_none, remove_rating_text)
    image_url_in = MapCompose(clean_img)
    seller_url_in = MapCompose(lambda x: x if x.startswith('https://www.amazon.com') else 'https://www.amazon.com' + x)
    

