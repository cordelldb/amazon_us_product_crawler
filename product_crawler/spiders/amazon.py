import scrapy
from product_crawler.items import AmazonProduct, AmazonProductLoader

class AmazonSpider(scrapy.Spider):
    name = "amazon"

    def start_requests(self):
        keyword_list = [
            'targus',  
        ]
        for keyword in keyword_list:
            amazon_search_url = f'https://www.amazon.com/s?k={keyword}&page=1'
            yield scrapy.Request(url=amazon_search_url, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': 1})
        
 
    def discover_product_urls(self, response):
        page = response.meta['page']
        keyword = response.meta['keyword'] 
        
        product_urls = response.css("h2 a::attr(href)")
        yield from response.follow_all(product_urls, callback=self.parse_product, meta={'keyword': keyword, 'page': page})
         
        if page == 1: 
            # available_pages = response.xpath(
            #     '//*[contains(@class, "s-pagination-item")][not(has-class("s-pagination-separator"))]/text()'
            # ).getall()
            # last_page = available_pages[-1]
            max_pages = 10
            for page_num in range(2, max_pages):
                amazon_search_url = f'https://www.amazon.com/s?k={keyword}&page={page_num}'
                yield scrapy.Request(url=amazon_search_url, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': page_num})
                
        

    def parse_product(self, response):
        products = response.css("#a-page")

        for product in products:
            loader = AmazonProductLoader(item=AmazonProduct(), selector=product)

            loader.add_css('platform_product_id', '#ASIN::attr(value)')
            loader.add_value('marketplace_id', 'AMAZON-US')
            loader.add_css('product_title', '#productTitle::text')
            loader.add_css('buy_box_price', '.a-offscreen::text')
            loader.add_css('currency', '#currencyOfPreference::attr(value)')
            loader.add_value('listing_url', response.url)
            loader.add_css('manufacturer_name', 'th:contains("Manufacturer") + td::text')
            loader.add_css('brand', 'th:contains("Brand") + td::text')
            loader.add_css('model_number', 'th:contains("Item model number") + td::text')
            loader.add_css('part_number', 'th:contains("Part Number") + td::text')
            loader.add_css('model_name', 'th:contains("Model Name") + td::text')
            # loader.add_css('product_dimensions', 'th:contains("Dimensions") + td::text')
            loader.add_css('ratings_count', '#acrCustomerReviewText::text')
            loader.add_css('average_rating', '#acrPopover::attr(title)')
            loader.add_css('image_url', '#imgTagWrapperId img::attr(src)')
            # loader.add_css('product_weight', 'th:contains("Weight") + td::text')
            loader.add_css('buy_box_seller', '#sellerProfileTriggerId::text')
            loader.add_css('seller_url', '#sellerProfileTriggerId::attr(href)')
            yield loader.load_item()
