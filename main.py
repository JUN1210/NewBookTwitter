# -*- coding: utf-8 -*-
"""
!pip install bottlenose
!pip install retry
!pip install twitter
!pip install beautifulsoup4
!pip install requests
!pip3 install lxml
"""
import os
import urllib
import re
import pandas as pd
from bottlenose import Amazon
from bs4 import BeautifulSoup
from retry import retry
from twitter import *
from requests_oauthlib import OAuth1Session
import json
import random
import datetime
from time import sleep
import requests

# 1時間に3タイトルのみ投稿するため、ページをランダムにして、そのなかからさらにランダムで3タイトル取得することにする
page = random.randint(1,5)

# url = "https://www.amazon.co.jp/gp/new-releases/books" の情報を取得する
url = "https://www.amazon.co.jp/gp/new-releases/books" + '?pg=' + str(page) 

#urlsリストのページ情報を取得 リトライを各URLごとに効かせる
@retry(urllib.error.HTTPError, tries=10, delay=2, backoff=2)
def soup_single_url(url):
    htmltext = requests.get(url).text
    soup = BeautifulSoup(htmltext, "lxml")
    return soup

#取得したページの情報から、必要なデータを抜き出す
@retry(urllib.error.HTTPError, tries=7, delay=1)
def get_Title_list(soup):
    df = pd.DataFrame(index=[],columns=["NewReleaseRanking", "title", "author", "asin", "price", "releaseDate"])
    for el in soup.find_all("div", class_="zg_itemRow"):
        rank  = el.find("span", class_="zg_rankNumber").string.strip()
        rank = rank.replace(".","")
        
        title  = el.find_all("div", class_="p13n-sc-truncate")[0].string.strip()

        author = el.find("a", class_="a-size-small")
        if author:
            author = author.string.strip()
            author = author.replace(" ", "")
        else:
            author = el.find("span", class_="a-size-small").string.strip()
   
        if author.isdigit(): #文字列かどうか判定
            author = el.find("span", class_="a-size-small").string.strip()
                
        price = el.find("span", class_="p13n-sc-price")
        if price:
            price = price.string.strip()
        else:
            price = "未定"

        asin_tag = el.find("a", class_="a-link-normal").get("href")
        asin_list =re.findall('[0-9]{9}.' , str(asin_tag))
        asin = ",".join(asin_list)
            
        re_date = el.find("div", class_="zg_releaseDate").string.strip()
        re_date = re_date.replace("発売日: ", "")
        re_date = re_date.replace("出版日: ", "")
            
        print("{} {} {} {} {} {}".format(rank, price, title, author, asin, re_date))

        series = pd.Series([rank, title, author, str(asin), price, re_date], index = df.columns)
        df = df.append(series, ignore_index = True)

    return df


#各タイトルのASINをアマゾンで検索、書籍のみ > tweet_dfにtitle/image/url/rank/releasedateを格納する

#amazonのアクセスキー
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_ASSOCIATE_TAG = os.environ["AWS_ASSOCIATE_TAG"]

# エラーの場合、1秒待機しリトライ（最大7回）
# ResponseGroupについては、https://images-na.ssl-images-amazon.com/images/G/09/associates/paapi/dg/index.html　参照

@retry(urllib.error.HTTPError, tries=7, delay=1)
def search(amazon, k):
    print('get products...')
    return amazon.ItemLookup(ItemId=k, ResponseGroup="Medium")


def get_amazon(title_df):
    amazon = Amazon(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ASSOCIATE_TAG, Region='JP',
                    Parser=lambda text: BeautifulSoup(text, 'xml')
                    )
    amazon_df = pd.DataFrame(index=[], columns=['asin','image', 'url'])
    # ASINでアマゾン検索かける。 
    for i,v in title_df.iterrows():
    
        response = search(amazon, v["asin"]) 
#        print(response)  #特にprintする必要はない

        # 検索によって返ってきた情報をデータフレームに格納していく
        for item in response.find_all('Item'):
            print(item.Title.string, item.LargeImage, item.DetailPageURL.string, item) # ここも特にprintする必要はない
            li = item.LargeImage
            URL = item.DetailPageURL
            
            series = pd.Series([v["asin"], li.URL.string, URL.string], index=amazon_df.columns)
            amazon_df = amazon_df.append(series, ignore_index = True)

    #amazon_df と title_dfを結合する
    tweet_df = pd.merge(title_df, amazon_df, on="asin")
    
    return tweet_df


# twitterのアクセストークン
CONSUMER_KEY        = os.environ["CONSUMER_KEY"]
CONSUMER_SECRET_KEY = os.environ["CONSUMER_SECRET_KEY"]
ACCESS_TOKEN        = os.environ["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = os.environ["ACCESS_TOKEN_SECRET"]

# twitterの各トークン
def tweet(tweet_df):
    twitter = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    url_media = "https://upload.twitter.com/1.1/media/upload.json"
    url_text = "https://api.twitter.com/1.1/statuses/update.json"

    tweets = []   #リストにツイートする内容を入れる

    # データフレームの１行ごとにツイートを作成していく。sleepでツイート毎の投稿間隔を調整。

    for i,v in tweet_df.iterrows():
        print(i,v["title"],v["author"],v["url"])
        print("i: " + str(i) + " v: " + str(v))
        tweet =v["title"] +"\n"+ v["author"] +" "+ "v[price]" + "\n" + "発売日:" + v["releaseDate"] + "\n" + "#"+ v["author"]+" #NewRelease" + "\n" + v["url"] # tweetの文面部
        media_name = v["image"]
        tweets.append(tweet)

        # 画像の投稿は下記の処理が必要。アップロードしてメディアIDを取得する必要あり。

        files = {"media" : urllib.request.urlopen(media_name).read()}
        req_media = twitter.post(url_media, files = files)

        media_id = json.loads(req_media.text)['media_id']
        print("MEDIA ID: %d" % media_id)

        params = {"status" : tweet, "media_ids" : [media_id]}
        req = twitter.post("https://api.twitter.com/1.1/statuses/update.json", params = params)    

        sleep(120) #2分待つ
    
# サムネイルが自動反映されない　→画像の添付で対応した

# 全体の実行処理
def main():
    page = random.randint(1,5)
    url = "https://www.amazon.co.jp/gp/new-releases/books" + '?pg=' + str(page) 
    soup = soup_single_url(url)
    title_df = get_Title_list(soup)
    title_df = title_df.sample(n=3)
    tweet_df = get_amazon(title_df)
    tweet(tweet_df)
    
main()



