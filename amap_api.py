# 高德地图开放平台
import json
import requests


def utf8_encode(msg):
    byte = msg.encode(encoding='utf-8')
    res = str(byte)
    res = res[2:-1]
    res = res.replace('\\x','%')
    return res


def get_weather(city, type='base', day=1):
    city = utf8_encode(city)
    url = 'http://restapi.amap.com/v3/weather/weatherInfo?key=64b47acf2e22a8b69df377e0aaf9f7aa&city={}&extensions={}'.format(city, type)
    with requests.request('GET', url) as result:
        data = json.loads(result.content)
        print(data)
        if data['status'] == '1':
            if type == 'base':
                if len(data['lives']) > 0:
                    wea = data['lives'][0]
                    res = '[{} {} 实时天气]\n更新时间：{}\n\n天气：{}\n温度：{}°\n风向：{}\n风力：{}级\n湿度：{}%\n'.format(
                        wea['province'], wea['city'], wea['reporttime'], wea['weather'], wea['temperature'], wea['winddirection'], wea['windpower'], wea['humidity'])
                    print(res)
                    return res
            elif type == 'all':
                if len(data['forecasts']) > 0:
                    wea = data['forecasts'][0]
                    res = ['[{} {} 天气预报]'.format(wea['province'], wea['city'])]
                    res.append('更新时间：{}'.format(wea['reporttime']))
                    num = 0
                    for cast in wea['casts']:
                        res.append('\n日期：{}'.format(cast['date']))
                        res.append('白天：{} {}℃'.format(cast['dayweather'], cast['daytemp']))
                        res.append('晚上：{} {}℃'.format(cast['nightweather'], cast['nighttemp']))
                        num += 1
                        if num >= day:
                            break
                    return '\n'.join(res)
        return None
