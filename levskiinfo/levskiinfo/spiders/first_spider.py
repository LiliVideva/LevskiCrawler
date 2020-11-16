import csv
import json
import re
import pandas as pd
import dateparser
import scrapy


class QuotesSpider(scrapy.Spider):
    name = "levski_info"
    start_urls = [
        'https://levskisofia.info/season-1921-1922/',
        'https://levskisofia.info/season-1922-1923/',
        'https://levskisofia.info/season-1923-1924/',
        'https://levskisofia.info/season-1924-1925/',
        'https://levskisofia.info/season-1925-1926/',
        'https://levskisofia.info/season-1926-1927/',
        'https://levskisofia.info/season-1927-1928/',
        'https://levskisofia.info/season-1928-1929/',
        'https://levskisofia.info/season-1929-1930/',
        'https://levskisofia.info/season-1930-1931/',
        'https://levskisofia.info/season-1931-1932/',
        'https://levskisofia.info/season-1932-1933/',
        'https://levskisofia.info/season-1933-1934/',
        'https://levskisofia.info/season-1934-1935/',
        'https://levskisofia.info/season-1935-1936/',
        'https://levskisofia.info/season-1936-1937/',
        'https://levskisofia.info/season-1937-1938/',
        'https://levskisofia.info/season-1938-1939/',
        'https://levskisofia.info/season-1939-1940/',
        'https://levskisofia.info/season-1940-1941/',
        'https://levskisofia.info/season-1941-1942/',
        'https://levskisofia.info/season-1942-1943/',
        'https://levskisofia.info/season-1943-1944/',
        'https://levskisofia.info/season-1944-1945/',
        'https://levskisofia.info/season-1945-1946/',
        'https://levskisofia.info/season-1946-1947/',
        'https://levskisofia.info/season-1947-1948/',
        'https://levskisofia.info/season-1948-1949/',
        'https://levskisofia.info/season-1949-1950/',
        'https://levskisofia.info/season-1950/',
        'https://levskisofia.info/season-1951/',
        'https://levskisofia.info/season-1952/',
        'https://levskisofia.info/season-1953/',
        'https://levskisofia.info/season-1954/',
        'https://levskisofia.info/season-1955/',
        'https://levskisofia.info/season-1956/',
        'https://levskisofia.info/season-1957/',
        'https://levskisofia.info/season-1958/',
        'https://levskisofia.info/season-1958-1959/',
        'https://levskisofia.info/season-1959-1960/',
        'https://levskisofia.info/season-1960-1961/',
        'https://levskisofia.info/season-1961-1962/',
        'https://levskisofia.info/season-1962-1963/',
        'https://levskisofia.info/season-1963-1964/',
        'https://levskisofia.info/season-1964-1965/',
        'https://levskisofia.info/season-1965-1966/',
        'https://levskisofia.info/season-1966-1967/',
        'https://levskisofia.info/season-1967-1968/',
        'https://levskisofia.info/season-1968-1969/',
        'https://levskisofia.info/season-1969-1970/',
        'https://levskisofia.info/season-1970-1971/',
        'https://levskisofia.info/season-1971-1972/',
        'https://levskisofia.info/season-1972-1973/',
        'https://levskisofia.info/season-1973-1974/',
        'https://levskisofia.info/season-1974-1975/',
        'https://levskisofia.info/season-1975-1976/',
        'https://levskisofia.info/season-1976-1977/',
        'https://levskisofia.info/season-1977-1978/',
        'https://levskisofia.info/season-1978-1979/',
        'https://levskisofia.info/season-1979-1980/',
        'https://levskisofia.info/season-1980-1981/',
        'https://levskisofia.info/season-1981-1982/',
        'https://levskisofia.info/season-1982-1983/',
        'https://levskisofia.info/season-1983-1984/',
        'https://levskisofia.info/season-1984-1985/',
        'https://levskisofia.info/season-1985-1986/',
        'https://levskisofia.info/season-1986-1987/',
        'https://levskisofia.info/season-1987-1988/',
        'https://levskisofia.info/season-1988-1989/',
        'https://levskisofia.info/season-1989-1990/',
        'https://levskisofia.info/season-1990-1991/',
        'https://levskisofia.info/season-1991-1992/',
        'https://levskisofia.info/season-1992-1993/',
        'https://levskisofia.info/season-1993-1994/',
        'https://levskisofia.info/season-1994-1995/',
        'https://levskisofia.info/season-1995-1996/',
        'https://levskisofia.info/season-1996-1997/',
        'https://levskisofia.info/season-1997-1998/',
        'https://levskisofia.info/season-1998-1999/',
        'https://levskisofia.info/season-1999-2000/',
        'https://levskisofia.info/season-2000-2001/',
        'https://levskisofia.info/season-2001-2002/',
        'https://levskisofia.info/season-2002-2003/',
        'https://levskisofia.info/season-2003-2004/',
        'https://levskisofia.info/season-2004-2005/',
        'https://levskisofia.info/season-2005-2006/',
        'https://levskisofia.info/season-2006-2007/',
        'https://levskisofia.info/season-2007-2008/',
        'https://levskisofia.info/season-2008-2009/',
        'https://levskisofia.info/season-2009-2010/',
        'https://levskisofia.info/season-2010-2011/',
        'https://levskisofia.info/season-2011-2012/',
        'https://levskisofia.info/season-2012-2013/',
        'https://levskisofia.info/season-2013-2014/',
        'https://levskisofia.info/season-2014-2015/',
        'https://levskisofia.info/season-2015-2016/',
        'https://levskisofia.info/season-2016-2017/',
        'https://levskisofia.info/season-2017-2018/',
        'https://levskisofia.info/season-2018-2019/',
        'https://levskisofia.info/season-2019-2020/'
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.players = {}

    def __del__(self):
        with open("players.json", "w", encoding="utf-8") as out_file:
            json.dump(self.players, out_file, indent=4, ensure_ascii=False)

    def parse(self, response):
        player_links = response.xpath('//*[@id="playerTable"]//tbody//*[@class="plt"]//a/@href').getall()

        for link in player_links:
            yield response.follow(link, callback=self.parse_links)

    def parse_links(self, response):
        player_data = {}

        player_info = response.xpath('//*[@class="playerstat"]')
        player_first_name = player_info.xpath('//*[@itemprop="givenName"]//text()').get()
        player_last_name = player_info.xpath('//*[@itemprop="familyName"]//text()').get()
        player_full_name = player_first_name + " " + player_last_name
        player_data["label"] = player_full_name
        player_data["playerID"] = response.url.split("player/")[1].split("/")[0]

        player_en_names = player_data.get("playerID").split("-")
        player_en_full_name = ""
        for name in player_en_names:
            player_en_full_name = player_en_full_name + " " + name.capitalize()

        player_en_full_name = player_en_full_name.strip()
        player_data["en_label"] = player_en_full_name

        player_additional_name = player_info.xpath('//*[@itemprop="additionalName"]//text()').get()
        if player_additional_name:
            player_data["Also known as"] = player_additional_name

        player_data["instance of"] = "human"
        player_data["sex or gender"] = "male"
        player_data["sport"] = "association football"
        player_data["occupation"] = "association football player"

        player_nationality = player_info.xpath('//*[@itemprop="nationality"]//text()').get()
        player_data["place of birth"] = player_nationality

        player_birth_date = player_info.xpath('//*[@class="plt"]//*[@itemprop="birthDate"]//text()').get()
        try:
            player_data["date of birth"] = dateparser.parse(player_birth_date).strftime("%Y-%m-%d")
        except TypeError:
            player_data["date of birth"] = ""

        player_carrier = player_info.xpath('//*[@class="stat"]//tr[last()]//td//text()')
        player_seasons = player_info.xpath(
            '(//*[@class="stat"]//*[@class="hr"])//a[contains(text(), "/")]//text()').getall()
        player_data["seasons"] = player_seasons
        player_matches = player_carrier[1].extract()
        player_data["number of matches played/races/starts"] = player_matches
        player_goals = player_carrier[2].extract()
        player_data["number of points/goals/set scored"] = player_goals

        self.players[player_en_full_name] = player_data
