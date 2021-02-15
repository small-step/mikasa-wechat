import json
import requests
from lxml import etree


def num2unit(num):
    def strofsize(num, level):
        if level >= 2:
            return num, level
        elif num >= 10000:
            num /= 10000
            level += 1
            return strofsize(num, level)
        else:
            return num, level
    units = ['', '万', '亿']
    num, level = strofsize(num, 0)
    if level > len(units):
        level -= 1
    return '{}{}'.format(round(num, 3), units[level])


def get_weather(url):
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.10240"
    with requests.request('GET', url, headers={'User-agent': ua}) as res:
        content = res.text
        html = etree.HTML(content)
        # 当前天气状况
        title = html.xpath('//*[@id="WxuCurrentConditions-main-b3094163-ef75-4558-8d9a-e35e6b9b1034"]/div/section/div/div[1]/h1/text()')
        update_time = html.xpath('//*[@id="WxuCurrentConditions-main-b3094163-ef75-4558-8d9a-e35e6b9b1034"]/div/section/div/div[1]/div/text()')
        cur_temp = html.xpath('//*[@id="WxuCurrentConditions-main-b3094163-ef75-4558-8d9a-e35e6b9b1034"]/div/section/div/div[2]/div[1]/span/text()')
        cur_cond = html.xpath('//*[@id="WxuCurrentConditions-main-b3094163-ef75-4558-8d9a-e35e6b9b1034"]/div/section/div/div[2]/div[1]/div/text()')
        cur_rain = html.xpath('//*[@id="WxuCurrentConditions-main-b3094163-ef75-4558-8d9a-e35e6b9b1034"]/div/section/div/div[3]/span/text()')
        body_temp = html.xpath('//*[@id="WxuTodayDetails-main-fd88de85-7aa1-455f-832a-eacb037c140a"]/section/div[1]/div[1]/span[1]/text()')
        # 今日预报
        morning_temp = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[1]/a/div[1]/span/text()')
        morning_rain = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[1]/a/div[3]/span/text()')
        afternoon_temp = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[2]/a/div[1]/span/text()')
        afternoon_rain = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[2]/a/div[3]/span/text()')
        night_temp = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[3]/a/div[1]/span/text()')
        night_rain = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[3]/a/div[3]/span/text()')
        early_morning_temp = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[4]/a/div[1]/span/text()')
        early_morning_rain = html.xpath('//*[@id="WxuTodayWeatherCard-main-486ce56c-74e0-4152-bd76-7aea8e98520a"]/section/div/ul/li[4]/a/div[3]/span/text()')
        # 日出/日落时间
        sunrise = html.xpath('//*[@id="SunriseSunsetContainer-fd88de85-7aa1-455f-832a-eacb037c140a"]/div/div/div/div[1]/p/text()')
        sunset = html.xpath('//*[@id="SunriseSunsetContainer-fd88de85-7aa1-455f-832a-eacb037c140a"]/div/div/div/div[2]/p/text()')
        # 空气质量
        air_quality = html.xpath('//*[@id="WxuAirQuality-sidebar-aa4a4fb6-4a9b-43be-9004-b14790f57d73"]/section/div/div[1]/svg/text/text()')
        air_quality_des1 = html.xpath('//*[@id="WxuAirQuality-sidebar-aa4a4fb6-4a9b-43be-9004-b14790f57d73"]/section/div/div[2]/div/div/span/text()')
        air_quality_des2 = html.xpath('//*[@id="WxuAirQuality-sidebar-aa4a4fb6-4a9b-43be-9004-b14790f57d73"]/section/div/div[2]/div/div/p/text()')
        # 风速
        wind = html.xpath('//*[@id="WxuTodayDetails-main-fd88de85-7aa1-455f-832a-eacb037c140a"]/section/div[2]/div[2]/div[1]/text()')
        wind_speed = html.xpath('//*[@id="WxuTodayDetails-main-fd88de85-7aa1-455f-832a-eacb037c140a"]/section/div[2]/div[2]/div[2]/span/text()')
        # 湿度
        humidity = html.xpath('//*[@id="WxuTodayDetails-main-fd88de85-7aa1-455f-832a-eacb037c140a"]/section/div[2]/div[3]/div[2]/span/text()')
        # 紫外线指数
        ultraviolet_ray = html.xpath('//*[@id="WxuTodayDetails-main-fd88de85-7aa1-455f-832a-eacb037c140a"]/section/div[2]/div[6]/div[2]/span/text()')

        lines = ['{}({})'.format(title[0], update_time[0])]
        if len(cur_rain) == 0:
            lines.append('当前天气：{}，体感温度：{}，{}'.format(cur_temp[0], body_temp[0], cur_cond[0]))
        else:
            lines.append('当前天气：{}，体感温度：{}，{}，{}'.format(cur_temp[0], body_temp[0], cur_cond[0], cur_rain[0]))
        if len(air_quality) > 0 and len(air_quality_des1) > 0 and len(air_quality_des2) > 0:
            lines.append('空气质量指数：{}，{}，{}'.format(air_quality[0], air_quality_des1[0], air_quality_des2[0]))
        lines.append('紫外线指数：{}'.format(ultraviolet_ray[0]))
        lines.append('风力：{}，{}'.format(wind[0], wind_speed[0]))
        lines.append('湿度：{}'.format(humidity[0]))
        lines.append('日出时间：{}，日落时间：{}'.format(sunrise[0], sunset[0]))
        lines.append('夜间温度：{}，降雨概率：{}'.format(early_morning_temp[0], early_morning_rain[0]))
        lines.append('上午温度：{}，降雨概率：{}'.format(morning_temp[0], morning_rain[0]))
        lines.append('下午温度：{}，降雨概率：{}'.format(afternoon_temp[0], afternoon_rain[0]))
        lines.append('晚上温度：{}，降雨概率：{}'.format(night_temp[0], night_rain[0]))
        return '\n'.join(lines)


def get_chat_content(question):
    appid = '770e1376073466aef4e532a7af0850a2'
    url = 'https://api.ownthink.com/bot?appid={}&userid=alinshans&spoken={}'.format(appid, question)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'api.ownthink.com',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36",
    }
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.10240"
    with requests.request('GET', url, headers=headers) as res:
        answer = res.text
        answer = json.loads(answer)
        return answer
