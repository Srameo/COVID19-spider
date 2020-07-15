# beautifulsoup
from bs4 import BeautifulSoup
# requests
import requests
# 链接 mongodb
import pymongo
# 正则表达式
import re

class ConnectException(Exception):
    """
    连接到网站时的异常
    """
    def __init__(self, msg="连接异常！"):
        super(ConnectException, self).__init__(msg)

class NoDataException(Exception):
    """
    获取数据失败的异常
    """
    def __init__(self, msg="当日无数据！"):
        super(NoDataException, self).__init__(msg)

class NoSuchProvinceException(Exception):
    """
    没有当前省的异常
    """
    def __init__(self, msg="没有这个省！"):
        super(NoSuchProvinceException, self).__init__(msg)

class Spider:

    BASE_URL = "http://m.sinovision.net/newpneumonia.php"
    client = pymongo.MongoClient('mongodb://spider:spider007@101.200.189.12:27017/')
    db = client['COVID19Spider']
    CHINA_TABLE = "CHINA"
    CHINA_DETAIL_TABLE = "CHINA_DETAIL"
    OTHER_TABLE = "OTHERS"
    soup = None

    def __init__(self):
        pass

    def makeSoup(self):
        """
        链接url，做一锅汤
        :return: BeautifulSoup
        """
        req = requests.get(self.BASE_URL, headers={"User-Agent": "Mozilla5.0"})
        if req.status_code != 200:
            # 请求失败时报错
            raise ConnectException
        self.soup = BeautifulSoup(req.text, 'lxml')
        return self.soup

    def getUpdateTime(self, title):
        # 确定数据更新的日期
        date_element = title.find_next_sibling('span', attrs={"class": "today-time"}) \
            .find_next_sibling('span', attrs={"class": "today-time"})
        regex = re.compile(r'\d{4}-\d{2}-\d{2}')
        date = str(regex.search(date_element.text).group())
        regex = re.compile(r'\d{2}:\d{2}:\d{2}')
        time = str(regex.search(date_element.text).group())
        return [date, time]

    def setOldLatestNotLatest(self, db, fltr=None):
        """
        设置久的最新数据的latest字段为0（即不是最新）
        :return: None
        """
        print("try to update latest data not latest!")
        if fltr is None:
            db.update_many({"latest": 1}, {"$set": {"latest": 0}})
        else:
            db.update_many(fltr, {"$set": {"latest": 0}})

    def getDataList(self, hosts, date, time):
        """
        将所有其他国家的书存入数据库
        :param time: 更新的时间
        :param date: 更新的日期
        :param hosts: 所有其他国家的Tag类型list
        :return:存有所有数据的dir list
        """
        return [{
            "name": host.contents[1].text,
            "confirm": int(host.contents[3].text),
            "dead": int(host.contents[5].text),
            "cure": int(host.contents[7].text),
            "date": date,
            "time": time,
            "latest": 1
        } for host in hosts]

    def getOtherCountries(self):
        """
        封装的获取其他数据
        :return: 成功与否
        """
        # 获取当前的网页
        print("尝试爬取 %s 数据！" % self.OTHER_TABLE)
        self.makeSoup()

        # 首先验证有没有今天的数据
        title = self.soup.find('span', class_="today-title", text="全球疫情")
        if title is None:
            raise NoDataException
        print("获取到链接，且今天有数据！")

        # 确定数据更新的日期
        [date, time] = self.getUpdateTime(title)
        print("网页数据最后更新时间%s %s" % (date, time))

        # 判断数据库中是否有当日数据
        database = self.db[self.OTHER_TABLE]
        exists = database.find_one({"date": date})
        if exists is None:
            # 当日数据不存在, 则需要插入
            # 获取所有其他国家数据的Tag list
            print("尝试插入数据")
            countries = title.find_all_next('div', attrs={"class": "prod"})
            data = self.getDataList(countries, date, time)
            print("insert", data)
            try:
                self.setOldLatestNotLatest(database)
                database.insert_many(data)
            except Exception as e:
                print("Something Wrong in insert!", e)
                return False
            print("插入成功！")
            return True
        else:
            # 当日数据存在, 则需要具体判断
            exists = database.find_one({"time": time})
            if exists is None:
                # 当日数据存在, 但更新时间不同，则需要更新
                print("尝试更新数据")
                countries = title.find_all_next('div', attrs={"class": "prod"})
                data = self.getDataList(countries, date, time)
                print("update", data)
                try:
                    self.setOldLatestNotLatest(database)
                    database.delete_many({"date": date})
                    database.insert_many(data)
                except Exception as e:
                    print("Something Wrong in delete and insert!", e)
                    return False
                print("更新成功！")
                return True
            else:
                # 当日数据存在且数据更新时间相同
                print("网页更新时间与当前相同...\n什么都不需要做")
                return True

    def getProvinceChild(self, province):
        """
        输入省名获取child的Tag对象
        :param province: 省名
        :return: child的Tag list对象
        """
        temp = self.soup.find('span', attrs={"class": "area"}, text=province)
        if temp is None:
            raise NoSuchProvinceException
        try:
            parent = temp.parent.parent
            return parent.find_all('div')[2:]
        except Exception:
            raise NoDataException

    def getProvinceDataList(self, cities, parent, date, time):
        """
        返回所有cities的数据list
        :param parent: 城市所属的省市
        :param time: 数据刷新的时间
        :param date: 数据刷新的日期
        :param cities: 所有城市的Tag list
        :return: list
        """
        return [{
            "parent": parent,
            "name": city.contents[1].text,
            "confirm": int(city.contents[3].text),
            "dead": int(city.contents[5].text),
            "cure": int(city.contents[7].text),
            "date": date,
            "time": time,
            "latest": 1
        } for city in cities]

    def getProvinceData(self, province):
        """
        获取一个省的所有子数据
        :param province: 省名
        :return: 是否成功
        """
        print("尝试爬取 %s 数据！省份：%s" % (self.CHINA_DETAIL_TABLE, province))
        # 获取当前的网页
        self.makeSoup()

        # 首先验证有没有今天的数据
        title = self.soup.find('span', class_="today-title", text="中国疫情")
        if title is None:
            raise NoDataException
        print("获取到链接，且今天有数据！")

        # 确定数据更新的日期
        [date, time] = self.getUpdateTime(title)
        print("网页数据最后更新时间\n%s %s" % (date, time))

        # 获取所有child
        try:
            children = self.getProvinceChild(province)
        except NoSuchProvinceException as e:
            print("没有想要查询的省份", e)
            return False
        except NoDataException as e:
            print("div父子关系有误！", e)
            return False

        # 判断数据库中是否有当日数据
        database = self.db[self.CHINA_DETAIL_TABLE]
        exists = database.find_one({"parent": province, "date": date})
        if exists is None:
            # 当日数据不存在, 则需要插入
            print("尝试插入数据")
            # 获取所有子数据数据的Tag list
            data = self.getProvinceDataList(children, province, date, time)
            print("insert", data)
            try:
                self.setOldLatestNotLatest(database, fltr={"parent": province, "latest": 1})
                database.insert_many(data)
            except Exception as e:
                print("Something Wrong in insert!", e)
                return False
            print("插入成功！")
            return True
        else:
            # 当日数据存在, 则需要具体判断
            exists = database.find_one({"parent": province, "date": date, "time": time})
            if exists is None:
                # 当日数据存在, 但更新时间不同，则需要更新
                print("尝试更新数据")
                data = self.getProvinceDataList(children, province, date, time)
                print("update", data)
                try:
                    database.delete_many({"parent": province, "date": date})
                    self.setOldLatestNotLatest(database, fltr={"parent": province, "latest": 1})
                    database.insert_many(data)
                except Exception as e:
                    print("Something Wrong in delete and insert!", e)
                    return False
                print("更新成功！")
                return True
            else:
                # 当日数据存在且数据更新时间相同
                print("网页更新时间与当前相同...\n什么都不需要做")
                return True

    def getChinaData(self):
        """
        获取一个中国所有省的子数据
        :return: 是否成功
        """
        print("尝试爬取 %s 数据！" % self.CHINA_TABLE)
        # 获取当前的网页
        self.makeSoup()

        # 首先验证有没有今天的数据
        title = self.soup.find('span', class_="today-title", text="中国疫情")
        if title is None:
            raise NoDataException
        print("获取到链接，且今天有数据！")

        # 确定数据更新的日期
        [date, time] = self.getUpdateTime(title)
        print("网页数据最后更新时间\n%s %s" % (date, time))

        # 获取所有数据
        proviences = title.find_all_next('div', attrs={'class': 'prod'}, limit=34)

        # 判断数据库中是否有当日数据
        database = self.db[self.CHINA_TABLE]
        exists = database.find_one({"date": date})
        if exists is None:
            # 当日数据不存在, 则需要插入
            print("尝试插入数据")
            # 获取所有子数据数据的Tag list
            data = self.getDataList(proviences, date, time)
            print("insert", data)
            try:
                self.setOldLatestNotLatest(database)
                database.insert_many(data)
            except Exception as e:
                print("Something Wrong in insert!", e)
                return False
            print("插入成功！")
            return True
        else:
            # 当日数据存在, 则需要具体判断
            exists = database.find_one({"date": date, "time": time})
            if exists is None:
                # 当日数据存在, 但更新时间不同，则需要更新
                print("尝试更新数据")
                data = self.getDataList(proviences, date, time)
                print("update", data)
                try:
                    database.delete_many({"date": date})
                    self.setOldLatestNotLatest(database)
                    database.insert_many(data)
                except Exception as e:
                    print("Something Wrong in delete and insert!", e)
                    return False
                print("更新成功！")
                return True
            else:
                # 当日数据存在且数据更新时间相同
                print("网页更新时间与当前相同...\n什么都不需要做")
                return True