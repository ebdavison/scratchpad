import scrapy
import json
import pyodbc
import datetime
from torrequest import TorRequest
from stem import Signal
from stem.control import Controller
import requests
import dateparser
import re
import hashlib

Asins = {}
KeyCache = {}
HashCache = {}
Stats = {}
Stats['captcha'] = 0
Stats['cache_hit'] = 0
Stats['cache_miss'] = 0
Stats['hash_hit'] = 0
Stats['hash_miss'] = 0
Stats['review_count'] = 0
AsinList = {}

class AsinReviewsSpider(scrapy.Spider):
    name = "asinreviews"
    download_delay = 2

    # read state file json file
    with open("scraper_state_ua.json", "r") as f:
        uastate=(json.load(f))
    uastate['ip_address_changed'] = 1
    wjson = json.dumps(uastate)
    with open("scraper_state_ua.json", "w") as f:
        f.write(wjson)

    # get TOR session
    def get_tor_session():
        session = requests.session()
        # Tor uses the 9050 port as the default socks port
        session.proxies = {'http':  'socks5://127.0.0.1:9050',
                           'https': 'socks5://127.0.0.1:9050'}
        return session

    # signal TOR for a new connection
    def renew_connection():
        with Controller.from_port(port = 9051) as controller:
            controller.authenticate(password="password")
            controller.signal(Signal.NEWNYM)


    def start_requests(self):
        # log start of scraper run
        self.write_log('N', 'Review Scraper Started')

        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        SQLCommand = ("SELECT ItemAsin FROM AmzAsin where IsEnabled = 1 and getdate() > dateadd(hh, 24, ReviewCollectedTime) order by newid()")
        cursor.execute(SQLCommand)
        asincount = 0
        for row in cursor.fetchall():
            AsinList[row[0]] = 1
            asincount += 1
        cursor.close()
        connection.close()

        msg = 'Review Scraper to process %s ASINs' % (asincount)
        self.write_log('N', msg)

        urls = []
        for asin in AsinList.keys():
            urls.append('https://www.amazon.com/product-reviews/'+asin+'/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=1')

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # extract ASIN from URL
        asin = response.url.split("/")[4]
        if (asin == "product-reviews"):
            asin = response.url.split("/")[5]
        if (len(asin) > 10):
            asin = asin.split("?")[0]
        if (len(asin) > 10):
            print("  ** Invalid ASIN: %s" % (asin))
            print("  ** URL: %s" % (response.url))
            return

        # extract page number from URL
        pageno_re = re.compile("www.amazon.com/.*pageNumber=(\d+).*")
        pageno = pageno_re.search(response.url).group(1)
        # sanity check page number
        if (pageno is None):
            pageno = '0'

        if (pageno == '1'):
            XPATH_TOTAL_REVIEWS = '//span[contains(@class,"totalReviewCount")]//text()'
            raw_review_total_count = response.xpath(XPATH_TOTAL_REVIEWS)
            print("    *** Raw Review Total Count: %s" % (raw_review_total_count))
            try:
                review_total_count = raw_review_total_count.extract()[0]
            except IndexError as error:
                review_total_count = 0
                msg = "Review: Index Error on total count extraction [%s]" % (error)
                self.write_log('E', msg)
                print("  *E* %s" % (msg))
            msg = "Review: ASIN %s, total reviews %s" % (asin, review_total_count)
            self.write_log('N', msg)
            self.update_total_reviews(asin, review_total_count)
            self.update_collected_reviews(asin, 0)
            Stats[asin] = 0

        # check if caught by captcha
        check_captcha_re = re.compile(".*captchacharacters.*")
        if check_captcha_re.search(response.body.decode('utf-8')):
            print("    ** CAPTCHA detected, resubmitting page")
            msg = "Review: CAPTCHA detected for ASIN %s, resubmitting page" % (asin)
            self.write_log('N', msg)

            Stats['captcha'] += 1
            with open("scraper_captcha.json", "r") as f:
                captcha=(json.load(f))
            if asin:
                if pageno:
                    captcha_key = ''.join([asin,str(pageno),'url'])
                else:
                    captcha_key = ''.join([asin,'0url'])
            else:
                captcha_key = 'AAAAAAAAAA0url'
            captcha[captcha_key] = response.url
            wjson = json.dumps(captcha)
            with open("scraper_captcha.json", "w") as f:
                f.write(wjson)
            yield scrapy.Request(url=response.url, callback=self.parse)
            return
        
        # write HTML to file
        filename = '/opt/amzwiz/cache/html/%s-reviews-%s.html' % (asin, pageno)
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

        # query for existing reviews and cache the keys
        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        SQLCommand = ("SELECT r.ReviewID, r.review_hash FROM AmzReview r")
        cursor.execute(SQLCommand)
        for row in cursor.fetchall():
            KeyCache[str(row[0])] = 1
            HashCache[str(row[0])] = row[1]
        cursor.close()
        connection.close()

        # xpath for reviews
        XPATH_AGGREGATE = '//span[@id="cm_cr-review-list"]'
        XPATH_REVIEW_SECTION_1 = '//div[contains(@data-hook,"review")]'
        XPATH_AGGREGATE_RATING = '//table[@id="histogramTable"]//tr'
        XPATH_PRODUCT_NAME = '//h1//span[@id="productTitle"]//text()'
        XPATH_PRODUCT_PRICE = '//span[@id="priceblock_ourprice"]/text()'

        # extract review info
        raw_product_price = response.xpath(XPATH_PRODUCT_PRICE).getall()
        raw_product_name = response.xpath(XPATH_PRODUCT_NAME).getall()
        total_ratings = response.xpath(XPATH_AGGREGATE_RATING)
        reviews = response.xpath(XPATH_REVIEW_SECTION_1)

        product_price = ''.join(raw_product_price).replace(',', '')
        product_name = ''.join(raw_product_name).strip()

        ratings_dict = {}
        reviews_list = []

        # Grabing the rating  section in product page
        for ratings in total_ratings:
            extracted_rating = ratings.xpath('./td//a//text()').getall()
            if extracted_rating:
                rating_key = extracted_rating[0] 
                raw_raing_value = extracted_rating[1]
                rating_value = raw_raing_value
                if rating_key:
                    ratings_dict.update({rating_key: rating_value})

        # Parsing individual reviews
        for review in reviews:
            Stats[asin] += 1
            XPATH_RATING  = './/i[@data-hook="review-star-rating"]//text()'
            XPATH_REVIEW_ID = './/[@id]//text()'
            XPATH_REVIEW_HEADER = './/a[@data-hook="review-title"]//text()'
            XPATH_REVIEW_HEADER_URL = './/a[@data-hook="review-title"]'
            XPATH_REVIEW_POSTED_DATE = './/span[@data-hook="review-date"]//text()'
            XPATH_REVIEW_TEXT_1 = './/span[@data-hook="review-body"]//text()'
            XPATH_REVIEW_COMMENTS = './/div[contains(@class,"review-comment")]//text()'
            XPATH_REVIEW_COMMENT_COUNT = './/span[contains(@class,"review-comment-total")]//text()'
            XPATH_AUTHOR = './/span[contains(@class,"profile-name")]//text()'
            XPATH_REVIEW_BADGE = './/span[@data-hook="avp-badge"]//text()'
            XPATH_PROFILE_URL = './/div[contains(@data-hook,"genome-widget")]//a'
            XPATH_FORMAT = './/a[contains(@data-hook,"format-strip")]//text()'
            
            raw_review_author = review.xpath(XPATH_AUTHOR).getall()
            raw_review_rating = review.xpath(XPATH_RATING).getall()
            raw_review_header = review.xpath(XPATH_REVIEW_HEADER).getall()
            raw_review_url = review.xpath(XPATH_REVIEW_HEADER_URL)
            raw_review_posted_date = review.xpath(XPATH_REVIEW_POSTED_DATE).getall()
            raw_review_text1 = review.xpath(XPATH_REVIEW_TEXT_1).getall()
            raw_review_text2 = ""
            raw_review_text3 = ""
            raw_review_badge = review.xpath(XPATH_REVIEW_BADGE).getall()
            if 'href' not in review.xpath(XPATH_PROFILE_URL).attrib.keys():
                print ("  ** ASIN %s, page %s" % (asin, pageno))
                print ("  ** href not found in %s" % (review.xpath(XPATH_PROFILE_URL).attrib))
                print ("  ** PROFILE URL: %s" % (review.xpath(XPATH_PROFILE_URL)))
                raw_profile_url = ""
            else:
                raw_profile_url = review.xpath(XPATH_PROFILE_URL).attrib['href']
            raw_format = review.xpath(XPATH_FORMAT).getall()

            # Cleaning data
            author = ' '.join(' '.join(raw_review_author).split())
            review_rating = ''.join(raw_review_rating).replace('out of 5 stars', '')
            review_header = ' '.join(' '.join(raw_review_header).split())
            try:
                review_id = review.xpath('.//div[contains(@id,"review-card")]/@id').extract()[0]
            except IndexError as error:
                review_id = 'abcdefghijklm'
                msg = "Review: Index Error on review ID extraction [%s]" % (error)
                self.write_log('E', msg)
                print("  *E* %s" % (msg))
            review_id = review_id.replace("-review-card","")
            review_badge = ' '.join(' '.join(raw_review_badge).split())
            profile_url = response.urljoin(raw_profile_url)
            try:
                review_url = response.urljoin(raw_review_url.xpath('@href').extract()[0])
            except IndexError as error:
                review_url = ""
                msg = "Review: Index Error on review URL extraction [%s]" % (error)
                self.write_log('E', msg)
                print("  *E* %s" % (msg))
            review_format = ' '.join(' '.join(raw_format).split())

            try:
                review_posted_date = dateparser.parse(''.join(raw_review_posted_date)).strftime('%d %b %Y')
            except:
                review_posted_date = None
            review_text = ' '.join(' '.join(raw_review_text1).split())

            # Grabbing hidden comments if present
            if raw_review_text2:
                json_loaded_review_data = loads(raw_review_text2[0])
                json_loaded_review_data_text = json_loaded_review_data['rest']
                cleaned_json_loaded_review_data_text = re.sub('<.*?>', '', json_loaded_review_data_text)
                full_review_text = review_text+cleaned_json_loaded_review_data_text
            else:
                full_review_text = review_text
            if not raw_review_text1:
                full_review_text = ' '.join(' '.join(raw_review_text3).split())

            raw_review_comment_count = review.xpath(XPATH_REVIEW_COMMENT_COUNT)
            try:
                review_comment_count = raw_review_comment_count.extract()[0]
            except IndexError as error:
                review_comment_count = 0
                msg = "Review: Index Error on review comment count extraction [%s]" % (error)
                self.write_log('E', msg)
                print("  *E* %s" % (msg))

            # calc hash of objects for future comparision for changes
            hash_str = ''.join(map(str, [review_posted_date, review_header, review_rating, author, review_text, review_comment_count]))
            hash_object = hashlib.sha256(hash_str.encode('utf-8'))
            hex_dig = hash_object.hexdigest()
            review_dict = {
                                'review_comment_count': review_comment_count,
                                'review_text': full_review_text,
                                'review_posted_date': review_posted_date,
                                'review_header': review_header,
                                'review_rating': review_rating,
                                'review_author': author,
                                'review_id': review_id,
                                'review_badge': review_badge,
                                'profile_url': profile_url,
                                'review_url': review_url,
                                'review_format': review_format,
                            }
            hash_str_all = ''.join(review_dict)
            hash_obj_all = hashlib.sha256(hash_str_all.encode('utf-8'))
            hex_dig_all = hash_obj_all.hexdigest()
            print("    ** HD : %s" % (hex_dig))
            print("    ** HDA: %s" % (hex_dig_all))
            review_dict['review_hash'] = hex_dig
            reviews_list.append(review_dict)

            if (AsinList.get(asin)):
                print("  * Sending %s (review ID %s) results to DB" % (asin, review_dict['review_id'])) 
                self.update_collected_reviews(asin, Stats[asin])
                self.insert_review_mapping(asin, review_dict)
                k = KeyCache.get(review_dict['review_id'])
                h = HashCache.get(review_dict['review_id'])
                if k:
                    print("    * Already in cache; skipping insert")
                    Stats['cache_hit'] += 1
                    Stats['review_count'] += 1
                    if (h == hex_dig):
                        print("    * Hash matches")
                        Stats['hash_hit'] += 1
                    else:
                        print("    ** Hashes do not match")
                        Stats['hash_miss'] += 1
                        hmkey = ''.join(["hash_miss_",review_dict['review_id']])
                        Stats[hmkey] = 1
                        self.insert_review_hash_miss(asin, review_dict['review_id'])
                        self.update_review(asin, review_dict)
                else:
                    print("    * NOT in cache; inserting")
                    Stats['cache_miss'] += 1
                    Stats['review_count'] += 1
                    self.insert_review_mapping(asin, review_dict)
                    self.insert_review(asin, review_dict)

        data = {
                    'ratings': ratings_dict,
                    'reviews': reviews_list,
                    'url': response.url,
                    'name': product_name,
                    'price': product_price.replace("$", "")
                }


        #filename = 'asin-%s.json' % asin
        filename = '/opt/amzwiz/cache/json/%s-reviews.json' % asin
        if (pageno == '1'):
            f=open(filename, 'w')
        else:
            f=open(filename, 'a')
        json.dump(data,f,indent=4)

        # clear the key cache for next round
        KeyCache.clear()

        # update last updated date
        self.update_last_collected_dt(asin)

        # stash stats
        print(Stats)
        wjson = json.dumps(Stats)
        with open("scraper_stats.json", "a") as f:
            f.write(wjson)

        # find next page and "push" a URL to the spider
        next_page = response.css('li.a-last > a::attr(href)').extract_first()
        if next_page:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(url=next_page, callback=self.parse)


    def insert_review(self, asin, data):
        sql_ins_item = """
            insert into AmzReview 
            (ReviewID, author, header, rating, posted_date, review, comment_count, url_profile, badge, format, url_review, review_hash)
            values
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	    """

        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        cursor.execute(sql_ins_item, data['review_id'], 
            data['review_author'], data['review_header'],
            data['review_rating'], data['review_posted_date'], 
            data['review_text'], data['review_comment_count'],
            data['profile_url'], data['review_badge'], data['review_format'],
            data['review_url'], data['review_hash'])
        connection.commit()
        cursor.close()

        KeyCache[data['review_id']] = 1
        HashCache[data['review_id']] = data['review_hash']

    def insert_review_mapping(self, asin, data):
        print("    *** in insert_review_mapping, Sending %s (%s) to DB"%(asin, data['review_id']))
        sql_ins_mapping = """
            if not exists (select ItemAsin, ReviewId 
                           from AmzReviewAsin
                           where ItemAsin = ? and ReviewID = ?)
            insert into AmzReviewAsin
                (ItemAsin, ReviewID)
                values
                (?, ?)
        """

        if data['review_id'] != 'abcdefghijlkm':
            connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
            cursor = connection.cursor()
            cursor.execute(sql_ins_mapping, asin, data['review_id'], asin, data['review_id']) 
            connection.commit()
            cursor.close()

    def update_last_collected_dt(self, asin):
        sqldict = { 'strasin': asin }
        sql_update_last_collected = """
	    update AmzAsin
	    set ReviewCollectedDate = (select(max(AmzReview.dt_collected))
	    from AmzReview
	    where AmzReview.ItemAsin = AmzAsin.ItemAsin)
	    where AmzAsin.ItemAsin = '{strasin}'
	    """.format(**sqldict)
        sql_update_last_collected = """
	    update AmzAsin
	    set ReviewCollectedTime = getdate()
	    where ItemAsin = '{strasin}'
	    """.format(**sqldict)
        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        cursor.execute(sql_update_last_collected)
        connection.commit()
        cursor.close()

    def update_total_reviews(self, asin, total):
        sql_update_total_reviews = """
	    update AmzAsin
	    set TotalReviews = ?
        where ItemAsin = ?
	    """
        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        cursor.execute(sql_update_total_reviews, total, asin)
        connection.commit()
        cursor.close()

    def update_collected_reviews(self, asin, total):
        sql_update_collected_reviews = """
	    update AmzAsin
	    set CollectedReviews = ?
        where ItemAsin = ?
	    """
        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        cursor.execute(sql_update_collected_reviews, total, asin)
        connection.commit()
        cursor.close()

    def insert_review_hash_miss(self, asin, review_id):
        sql_ins_item = """
            insert into scraper_review_hash_miss
            (asin, review_id)
            values
            (?, ?)
	    """

        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        cursor.execute(sql_ins_item, asin, review_id)
        connection.commit()
        cursor.close()

    def update_review(self, asin, data):
        sql_get_review = """
            select r.ReviewID, r.author, r.header, r.rating, r.posted_date, r.review, r.comment_count, r.review_hash 
            from AmzReview r, AmzReviewAsin m
            where m.ItemAsin = ? 
            and r.ReviewID = ?
            and r.ReviewID = m.ReviewID
	    """
        sql_get_sql_date = """
            select dateadd(hh, 0, ?)
	    """
        sql_ins_review_audit = """
            insert into AmzReviewAudit 
            (asin, ReviewId, audit_field, audit_data_before, audit_data_after)
            values
            (?, ?, ?, ?, ?)
	    """
        sql_update_collection_dt = """
            update AmzReview
            set dt_collected = getdate()
            where ReviewID = ?
        """

        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        get_cursor = connection.cursor()
        dt_cursor = connection.cursor()
        cursor = connection.cursor()

        update_hash_flag = 0
        print("Checking ASIN: %s, Review ID: %s" % (asin, data['review_id']))
        get_cursor.execute(sql_get_review, asin, data['review_id'])
        for row in get_cursor.fetchall():
            print ("    * Checking author [%s] : [%s]" % (row[1], data['review_author']))
            if data['review_id'] == 'abcdefghijklm':
                continue
            if (row[1] != data['review_author']):
                update_hash_flag = 1
                updated_hash = row[7]
                cursor.execute(sql_ins_review_audit, asin, data['review_id'], 
                    'review_author', row[1], data['review_author'])
                connection.commit()
                sql_update_review = """
                    update AmzReview
                    set %s = '%s'
                    where ReviewID = '%s'
                """ % ('author', data['review_author'], data['review_id'])
                print("    * SQL %s" % (sql_update_review))
                cursor.execute(sql_update_review)
                connection.commit()
                cursor.execute(sql_update_collection_dt, data['review_id']) 
                connection.commit()

            print ("    * Checking header [%s] : [%s]" % (row[2], data['review_header']))
            if (row[2] != data['review_header']):
                update_hash_flag = 1
                updated_hash = row[7]
                cursor.execute(sql_ins_review_audit, asin, data['review_id'], 
                    'review_header', row[2], data['review_header'])
                connection.commit()
                sql_update_review = """
                    update AmzReview
                    set %s = '%s'
                    where ReviewID = '%s'
                """ % ('header', data['review_header'], data['review_id'])
                print("    * SQL %s" % (sql_update_review))
                cursor.execute(sql_update_review)
                connection.commit()
                cursor.execute(sql_update_collection_dt, data['review_id']) 
                connection.commit()

            print ("    * Checking rating [%s] : [%s]" % (row[3], data['review_rating']))
            if (str(row[3]).strip() != str(data['review_rating']).strip()):
                update_hash_flag = 1
                updated_hash = row[7]
                cursor.execute(sql_ins_review_audit, asin, data['review_id'], 
                    'review_rating', str(row[3]).strip(), str(data['review_rating']).strip())
                connection.commit()
                sql_update_review = """
                    update AmzReview
                    set %s = %s
                    where ReviewID = '%s'
                """ % ('rating', data['review_rating'], data['review_id'])
                print("    * SQL %s" % (sql_update_review))
                if (data['review_id'] != 'abcdefghijklm'):
                    cursor.execute(sql_update_review)
                    connection.commit()
                cursor.execute(sql_update_collection_dt, data['review_id']) 
                connection.commit()

            dt_cursor.execute(sql_get_sql_date, data['review_posted_date'])
            check_date = ''
            for dtrow in dt_cursor.fetchall():
                check_date = dtrow[0]
            print ("    * Checking posted date [%s] : [%s]" % (row[4], check_date))
            if (row[4] != check_date):
                update_hash_flag = 1
                updated_hash = row[7]
                cursor.execute(sql_ins_review_audit, asin, data['review_id'], 
                    'review_posted_date', str(row[4]), str(check_date))
                connection.commit()
                sql_update_review = """
                    update AmzReview
                    set %s = '%s'
                    where ReviewID = '%s'
                """ % ('posted_date', check_date, data['review_id'])
                print("    * SQL %s" % (sql_update_review))
                cursor.execute(sql_update_review)
                connection.commit()
                cursor.execute(sql_update_collection_dt, data['review_id']) 
                connection.commit()

            rev_text = data['review_text']
            rev_text.replace("'", "\\'")
            print ("    * Checking text [%s] : [%s]" % (row[5], rev_text))
            if (row[5] != data['review_text']):
                update_hash_flag = 1
                updated_hash = row[7]
                cursor.execute(sql_ins_review_audit, asin, data['review_id'], 
                    'review_text', row[5], data['review_text'])
                connection.commit()
                sql_update_review = """
                    update AmzReview
                    set %s = ?
                    where ReviewID = '%s'
                """ % ('review', data['review_id'])
                print("    * SQL %s" % (sql_update_review))
                cursor.execute(sql_update_review, rev_text)
                connection.commit()
                cursor.execute(sql_update_collection_dt, data['review_id']) 
                connection.commit()

            print ("    * Checking comment count [%s] : [%s]" % (row[6], data['review_comment_count']))
            if (int(row[6]) != int(data['review_comment_count'])):
                update_hash_flag = 1
                updated_hash = row[7]
                cursor.execute(sql_ins_review_audit, asin, data['review_id'], 
                    'review_comment_count', str(row[6]), str(data['review_comment_count']))
                connection.commit()
                sql_update_review = """
                    update AmzReview
                    set %s = %s
                    where ReviewID = '%s'
                """ % ('comment_count', data['review_comment_count'], data['review_id'])
                print("    * SQL %s" % (sql_update_review))
                cursor.execute(sql_update_review)
                connection.commit()
                cursor.execute(sql_update_collection_dt, data['review_id']) 
                connection.commit()
            print("    * ready to fetch next row")

        if update_hash_flag == 1:
            cursor.execute(sql_ins_review_audit, asin, data['review_id'], 
                'review_hash', updated_hash, data['review_hash'])
            connection.commit()
            sql_update_review = """
                update AmzReview
                set %s = '%s'
                where ReviewID = '%s'
            """ % ('review_hash', data['review_hash'], data['review_id'])
            print("    * SQL %s" % (sql_update_review))
            cursor.execute(sql_update_review)
            connection.commit()

        get_cursor.close()
        cursor.close()

    def write_log(self, level, msg):
        sql_insert_log = """
	    insert into AppLog
            (Level, Message, CreatedBy)
            values
	    (?, ?, 6)
	    """
        connection = pyodbc.connect('DSN=VMC60ACDD;database=database;uid=username;pwd=password')
        cursor = connection.cursor()
        cursor.execute(sql_insert_log, level, msg)
        connection.commit()
        cursor.close()

