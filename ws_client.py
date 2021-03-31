import shutil
import time
import threading
import datetime
import json
import json5
import codecs
import random
import websocket
import tushare
from apscheduler.schedulers.background import BackgroundScheduler
import dataframe_image as dfi
import pandas

import thirdparty.datasourcing.stock_components as stock
import thirdparty.datasourcing.update as update

from constants import *
import utils
import amap_api


pandas.set_option('display.unicode.ambiguous_as_wide', True)
pandas.set_option('display.unicode.east_asian_width', True)


class WeChatBot(object):
    def __init__(self):
        self.ws = websocket.WebSocketApp(SERVER_ADDR,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ready = threading.Event()
        self.req_cnt = 0

        self.config = {}
        self.bot_info = {}
        self.data = {}
        self.chatroom = {}
        self.wxid2name = {}
        self.name2wxid = {}
        self.room_log_file = {}
        self.is_self = False
        self.need_update = False
        self.key_word = []
        self.quotation_path = ''
        self.quotation_name = ''
        self.state = NORMAL
        self.scheduler = BackgroundScheduler()
        self.ts = tushare.pro_api(TUSHARE_TOKEN)

    def run(self, enable_trace=False):
        self.scheduler.add_job(self.autosave_data, 'interval', seconds=10)
        self.scheduler.add_job(self.reset_data, 'cron', hour=5)
        self.scheduler.add_job(self.push_weather, 'cron', hour=7, minute=00)
        self.scheduler.add_job(self.dragon_king_job, 'cron', hour=9, minute=00)
        self.scheduler.add_job(self.push_strategy, 'cron', hour=9, minute=00)
        self.scheduler.add_job(self.sleep_helper, 'cron', hour=0, minute=00)
        self.scheduler.start()
        websocket.enableTrace(enable_trace)
        self.ws.run_forever()

    def on_open(self):
        print('[wxbot] 正在验证机器人身份...')
        self.handle_self_info({})

    def on_message(self, message):
        ctx = json.loads(message)
        resp_type = ctx['type']
        action = {
            HEART_BEAT: self.do_nothing,
            DEBUG_SWITCH: self.do_nothing,
            PERSONAL_INFO: self.handle_self_info,
            PERSONAL_DETAIL: self.handle_self_info,
            AT_MSG: self.do_nothing,
            TXT_MSG: self.do_nothing,
            PIC_MSG: self.do_nothing,
            USER_LIST: self.handle_user_list,
            GET_USER_LIST_SUCCSESS: self.handle_user_list,
            GET_USER_LIST_FAIL: self.handle_user_list,
            CHATROOM_INFO: self.handle_chatroom_info,
            CHATROOM_NICK_INFO: self.handle_chatroom_nick,
            RECV_PIC_MSG: self.handle_recv_msg,
            RECV_TXT_MSG: self.handle_recv_msg,
        }
        action.get(resp_type)(ctx)

    def on_error(self, error):
        print(error)

    def on_close(self):
        self.autosave_data()

    def init_config(self, wxid: str):
        print('[wxbot] 正在给机器人灌输记忆...')
        config_file = os.path.join(CONFIG_PATH, 'config.'+wxid+'.json')
        if not os.path.exists(config_file) and not os.path.exists('config.template.json'):
            print('[wxbot] 记忆文件缺失，机器人无法启动！')
        if not os.path.exists(config_file):
            shutil.copyfile('config.template.json', config_file)

        with codecs.open(filename=config_file, mode='r', encoding='utf-8') as f:
            self.config = json5.load(f)
        for room in self.config['enable_room']:
            self.room_log_file[room] = codecs.open(filename='log/' + room + '.log', mode='a+', encoding='utf-8')
        with codecs.open(filename='data/keyword.txt', mode='r', encoding='utf-8') as f:
            for line in f.readlines():
                self.key_word.append(line.rstrip())
        with codecs.open(filename='tmp/data.json', mode='r', encoding='utf-8') as f:
            self.data = json5.load(f)

        print('[wxbot] 正在给机器人寻找好友...')
        self.get_user_list()
        self.get_chatroom_info()

    def autosave_data(self):
        if not self.need_update: return
        with codecs.open(filename='tmp/data.json', mode='w', encoding='utf-8') as f:
            json.dump(self.data, fp=f, indent=4)
        self.need_update = False

    def reset_data(self):
        print('[INFO] reset data')
        self.data['dragon'].clear()
        for roomid in self.data['record'].keys():
            if roomid not in self.config['push']['dragon']:
                continue
            max_len, dragon = 0, ''
            for wxid in self.data['record'][roomid].keys():
                if len(self.data['record'][roomid][wxid]) > max_len:
                    max_len, dragon = len(self.data['record'][roomid][wxid]), wxid
            self.data['dragon'].append({'roomid': roomid, 'dragon': dragon})
        self.data['record'].clear()
        self.data['key_word_count'].clear()
        self.need_update = True
        self.autosave_data()

    def push_weather(self):
        for roomid in self.config['push']['weather'].keys():
            room = self.config['push']['weather'][roomid]
            if room['enable']:
                for city in room['city']:
                    res = amap_api.get_weather(city, 'all')
                    if res:
                        self.send_txt_msg(roomid, res)
                    time.sleep(3)

    def push_strategy(self):
        today = datetime.datetime.now().date()
        if not stock.is_trade_date(today):
            return
        update.update()
        res = stock.get_rec_stock_list(None, 5, 1)
        date = datetime.datetime.now().date() - datetime.timedelta(days=1)
        lines = []
        for item in res:
            name = item['name']
            if len(item['name']) < 4:
                name = item['name'] + '    ' + ('    ' * (4 - len(item['name'])))
            lines.append('{:<10} {:<8} {:.1f}'.format(item['code'], name, item['rec']))
        if len(lines) > 0:
            lines.insert(0, '{:<8} {:<8} {}'.format('股票代码', '股票名称', '评分'))
            lines.insert(0, '[{}][北向策略]：'.format(date.strftime('%Y-%m-%d')))
        else:
            lines.append('{}的数据还没出来哦~'.format(date.strftime('%Y-%m-%d')))
        for room in self.config['push']['stock_strategy']:
            self.send_txt_msg(room, '\n'.join(lines))

    def sleep_helper(self):
        file_path = os.path.join(ASSERTS_PATH, 'sleep_helper.jpg')
        for room in self.config['push']['sleep']:
            self.send_img_msg(room, file_path)
            time.sleep(1)

    def dragon_king_job(self):
        for item in self.data['dragon']:
            self.send_txt_msg(item['roomid'],
                              '昨天「{}」发言最多，获得龙王🐉称号'.format(self.get_name(item['roomid'], item['dragon'])))
            time.sleep(1)

    def get_id(self):
        return time.strftime("%Y%m%d%H%M%S", time.localtime())

    def get_name(self, roomid, wxid):
        if not roomid.endswith('@chatroom'):
            if not self.wxid2name.__contains__(wxid):
                self.get_user_list()
            return self.wxid2name[wxid]['name']
        if not self.wxid2name.__contains__(wxid) or not self.wxid2name[wxid].__contains__(roomid):
            self.get_chatroom_info()
        return self.wxid2name[wxid][roomid]

    # 这两个接口暂时还不支持，不能获取个人信息，只能先自己写死
    # def get_self_info(self):
    #     req = {
    #         'id': self.get_id(),
    #         'type': PERSONAL_INFO,
    #         'content': 'op:personal info',
    #         'wxid': 'ROOT',
    #     }
    #     self.ws.send(json.dumps(req))
    #
    # def get_self_detail_info(self):
    #     req = {
    #         'id': self.get_id(),
    #         'type': PERSONAL_DETAIL,
    #         'content': 'op:personal detail',
    #         'wxid': 'ROOT',
    #     }
    #     self.ws.send(json.dumps(req))

    def get_user_list(self):
        req = {
            'id': self.get_id(),
            'type': USER_LIST,
            'roomid': 'null',
            'wxid': 'null',
            'content': 'null',
            'nickname': 'null',
            'ext': 'null'
        }
        self.ws.send(json.dumps(req))

    def get_chatroom_info(self):
        req = {
            'id': self.get_id(),
            'type': CHATROOM_INFO,
            'roomid': 'null',
            'wxid': 'null',
            'content': 'null',
            'nickname': 'null',
            'ext': 'null'
        }
        self.ws.send(json.dumps(req))

    def get_chatroom_nick_info(self, roomid, wxid):
        req = {
            'id': self.get_id(),
            'type': CHATROOM_NICK_INFO,
            'roomid': roomid,
            'wxid': wxid,
            'content': 'null',
            'nickname': 'null',
            'ext': 'null'
        }
        self.ws.send(json.dumps(req))

    def send_txt_msg(self, to, content):
        print('LOG(send_txt_req):', to, content)
        msg = {
            'id': self.get_id(),
            'type': TXT_MSG,
            'roomid': 'null',
            'wxid': to,
            'content': content,
            'nickname': 'null',
            'ext': 'null'
        }
        self.ws.send(json.dumps(msg))
        time.sleep(1)

    def send_img_msg(self, to, file):
        msg = {
            'id': self.get_id(),
            'type': PIC_MSG,
            'roomid': 'null',
            'wxid': to,
            'content': file,
            'nickname': 'null',
            'ext': 'null'
        }
        self.ws.send(json.dumps(msg))

    def save_config(self):
        config_file = os.path.join(CONFIG_PATH, 'config.' + self.bot_info['id'] + '.json')
        with codecs.open(filename=config_file, mode='w', encoding='utf-8') as f:
            json.dump(self.config, fp=f, indent=4)
            f.flush()

    def do_nothing(self, ctx):
        pass

    def handle_self_info(self, msg):
        # content = json.loads(msg['content'])
        # 因为获取个人信息的接口现在暂时没法用了
        content = {
            'wx_id': 'wxid_1qbiovhcy6vm12',
            'wx_name': 'Mikasa'
        }
        self.bot_info = {
            'id': content['wx_id'],
            'name': content['wx_name']
        }
        self.wxid2name[content['wx_id']] = {'name': content['wx_name']}
        self.name2wxid[content['wx_name']] = content['wx_id']
        t = threading.Thread(target=self.init_config, args=(content['wx_id'],), daemon=True)
        t.start()

    def handle_user_list(self, msg):
        print('[message] 找到好友 {}'.format(msg))
        content = msg['content']
        for item in content:
            id = str(item['wxid'])
            if not id.endswith('@chatroom'):
                if not self.wxid2name.__contains__(id):
                    self.wxid2name[id] = {}
                self.wxid2name[id]['name'] = item['name']
                self.name2wxid[item['name']] = id

    def handle_chatroom_info(self, msg):
        self.ready.clear()
        print('[message] 找到一群好友 {}'.format(msg))
        content = msg['content']
        for chatroom in content:
            room_id = chatroom['room_id']
            if room_id not in self.config['enable_room']:
                continue
            # 已经被移出了群聊
            if self.config['self']['id'] not in chatroom['member']:
                continue
            self.req_cnt += 1
            self.get_chatroom_nick_info(room_id, room_id)
            members = chatroom['member']
            for member in members:
                self.req_cnt += 1
                self.get_chatroom_nick_info(room_id, member)
        self.ready.set()
        print('[wxbot] 准备就绪，机器人开始整活啦~！')

    def handle_chatroom_nick(self, msg):
        #print(msg)
        content = json5.loads(msg['content'])
        roomid = str(content['roomid'])
        wxid = str(content['wxid'])
        nick = str(content['nick'])
        if roomid == wxid:
            self.chatroom[roomid] = {'name': nick, 'member': []}
        else:
            self.chatroom[roomid]['member'].append(wxid)
            if not self.wxid2name.__contains__(wxid):
                self.wxid2name[wxid] = {}
            self.wxid2name[wxid][roomid] = nick
            if not self.name2wxid.__contains__(roomid):
                self.name2wxid[roomid] = {}
            self.name2wxid[roomid][nick] = wxid
        self.req_cnt -= 1
        if self.req_cnt == 0:
            self.ready.set()

    def handle_recv_msg(self, msg):
        if not self.ready.is_set():
            print(1)
            return
        print('[message] {}'.format(msg))
        is_txt = msg['type'] == RECV_TXT_MSG
        receiver = str(msg['wxid'])
        if len(msg['id1']) > 0:
            sender = str(msg['id1'])
        else:
            sender = str(msg['id2'])
        content = msg['content']

        self.is_self = False
        if sender == self.config['self']['id']:
            self.is_self = True
            words = str(content).split(' ')
            words = list(filter(None, words))
            if self.parse_self_command(receiver, words):
                return

        if receiver.endswith('@chatroom') and receiver not in self.config['enable_room']:
            return

        if receiver and sender:
            if is_txt:
                output = '{} {}:\n{}'.format(msg['time'], self.get_name(receiver, sender), str(content))
            else:
                output = '{} {}:\n[pic]{}'.format(msg['time'], self.get_name(receiver, sender), content['file'])
            print(output)

        if not is_txt: return
        if not receiver.endswith('@chatroom'):
            self.handle_priv_chat(receiver, sender, content)
        else:
            if receiver not in self.config['enable_room']: return
            if output:
                self.room_log_file[receiver].writelines(output)
                self.room_log_file[receiver].writelines('\n')
                self.room_log_file[receiver].flush()
            self.need_update = True
            self.handle_room_chat(receiver, sender, content)

    def handle_priv_chat(self, receiver, sender, content):
        pass

    def handle_room_chat(self, roomid, sender, content):
        if sender in self.config['super_admin']:
            authority = SUPER_ADMIN
        elif sender in self.config['admin']:
            authority = ADMIN
        else:
            authority = NORMAL
        if sender in self.config['pm']:
            authority |= PM
        if sender in self.config['qa']:
            authority |= QA
        words = str(content).split(' ')
        words = list(filter(None, words))

        if self.is_self:
            if self.parser_command(roomid, sender, words, SUPER_ADMIN):
                return
            if words[0].lower() == 'ping':
                self.send_txt_msg(roomid, 'pong!')
        elif roomid.__eq__('25137162819@chatroom'):
            if self.parser_command(roomid, sender, words, authority):
                return
        elif words[0].lower() == 'mikasa' and len(words) > 1:
            if self.parser_command(roomid, sender, words[1:], authority):
                return

        if not self.data['record'].__contains__(roomid):
            self.data['record'][roomid] = {sender: [content]}
        elif not self.data['record'][roomid].__contains__(sender):
            self.data['record'][roomid][sender] = [content]
        else:
            self.data['record'][roomid][sender].append(content)

        if not self.data['key_word_count'].__contains__(sender):
            self.data['key_word_count'][sender] = 0
        for word in self.key_word:
            if word in content:
                self.data['key_word_count'][sender] = self.data['key_word_count'][sender] + 1
                break

    def parse_self_command(self, receiver, words):
        cmd = str(words[0])
        dispatcher = {
            'enable': self.handle_enable_cmd,
            'disable': self.handle_disable_cmd,
            'update': self.handle_update_cmd,
        }
        if cmd in dispatcher.keys():
            print('parse self command')
            dispatcher.get(cmd)(receiver, words)
            return True
        return False

    def handle_enable_cmd(self, receiver, words):
        if len(words) < 2:
            return
        ctx = words[1]
        if ctx == 'room':
            if not receiver.endswith('@chatroom'):
                return
            if receiver not in self.config['enable_room']:
                self.config['enable_room'].append(receiver)
                self.get_chatroom_info()
                self.save_config()
                self.send_txt_msg(receiver, 'done')
        elif ctx == 'sleep':
            if not receiver.endswith('@chatroom'):
                return
            if not self.config['push'].__contains__('sleep'):
                self.config['push']['sleep'] = []
            if receiver not in self.config['push']['sleep']:
                self.config['push']['sleep'].append(receiver)
                self.save_config()
                self.send_txt_msg(receiver, 'done')

    def handle_disable_cmd(self, receiver, words):
        if len(words) < 2:
            return
        ctx = words[1]
        if ctx == 'room':
            if not receiver.endswith('@chatroom'):
                return
            if receiver in self.config['enable_room']:
                self.config['enable_room'].remove(receiver)
                self.get_chatroom_info()
                self.send_txt_msg(receiver, 'done')
                self.save_config()

    def handle_update_cmd(self, receiver, words):
        if len(words) < 2:
            return
        ctx = words[1]
        if ctx == 'user':
            self.get_user_list()
            self.send_txt_msg(receiver, 'update finished.')
        elif ctx == 'room':
            self.get_chatroom_info()
            self.send_txt_msg(receiver, 'update finished.')
        elif ctx == 'stock':
            day = 7
            self.send_txt_msg(receiver, '开始更新{}天内的数据...'.format(day))
            update.update(day)
            self.send_txt_msg(receiver, '更新完成！')

    def parser_command(self, roomid, sender, words, authority):
        print('LOG([parser_command):', roomid, sender, words, authority)
        is_command = False
        cmd = words[0]
        if cmd.__eq__('需求') and ((authority & SUPER_ADMIN) != 0 or (authority & PM) != 0):
            self.handle_cmd_requirement(roomid, sender, words)
            return True
        if cmd.__eq__('bug') and ((authority & SUPER_ADMIN) != 0 or (authority & PM) != 0):
            self.handle_cmd_bug(roomid, sender, words)
            return True
        # 普通权限
        if cmd in ['help', '帮助', '/help', '/h', '/?']:
            self.handle_cmd_help(roomid, words, authority)
            is_command = True
        elif cmd.__eq__('统计'):
            self.handle_cmd_statistics(roomid, words)
            is_command = True
        elif cmd.__eq__('语录'):
            self.handle_cmd_quotations(roomid, words, authority)
            is_command = True
        elif cmd.__eq__('股票'):
            self.handle_cmd_stock(roomid, words, authority)
            is_command = True
        elif cmd.__eq__('天气') or cmd == '天气预报':
            self.handle_cmd_weather(roomid, words)
            is_command = True
        elif cmd.__eq__('策略'):
            self.handle_cmd_strategy(roomid, words)
            is_command = True
        if authority < ADMIN: return is_command

        # 管理员权限
        if cmd.__eq__('复读'):
            self.handle_cmd_repeat(roomid, words, authority)
            is_command = True
        if authority < SUPER_ADMIN: return is_command

        # 超级管理员权限
        if cmd.__contains__('管理员'):
            is_command = self.handle_cmd_admin(roomid, words)
        elif cmd.__contains__('pm'):
            is_command = self.handle_cmd_pm(roomid, words)
        elif cmd.__contains__('qa'):
            is_command = self.handle_cmd_qa(roomid, words)
        elif cmd.__eq__('更新联系人'):
            self.get_user_list()
            is_command = True
        elif cmd.__eq__('更新群信息'):
            self.get_chatroom_nick_info(roomid)
            is_command = True
        elif cmd.__eq__('添加关键词'):
            if len(words) > 1:
                with codecs.open(filename='keyword.txt', mode='a+', encoding='utf-8') as f:
                    for i in range(1, len(words)):
                        self.key_word.append(words[i])
                        f.writelines(words[i])
                        f.writelines('\n')
            self.send_txt_msg(roomid, '添加成功~')
            is_command = True
        elif cmd.__eq__('更新'):
            self.handle_cmd_update(roomid, words)
            is_command = True
        return is_command

    def handle_cmd_requirement(self, roomid, sender, words):
        if len(words) < 2: return
        num = self.config['require_num']
        self.config['require_num'] = num + 1
        self.send_txt_msg(roomid, '需求 #{} 已创建，待评审~'.format(num))
        with codecs.open(filename='data/requirement_list.txt', mode='a+', encoding='utf-8') as f:
            f.writelines('需求 #{} {} 创建人：{}\n'.format(num, words[1], self.get_name(roomid, sender)))
            if len(words) > 2:
                for i in range(2, len(words)):
                    f.writelines(' ' + words[i])
                f.writelines('\n\n')
        self.save_config()

    def handle_cmd_bug(self, roomid, sender, words):
        if len(words) < 2: return
        num = self.config['bug_num']
        self.config['bug_num'] = num + 1
        self.send_txt_msg(roomid, 'BUG #{} 已创建，待修复~'.format(num))
        with codecs.open(filename='data/bug_list.txt', mode='a+', encoding='utf-8') as f:
            f.writelines('BUG #{} {} 创建人：{}\n'.format(num, words[1], self.get_name(roomid, sender)))
            if len(words) > 2:
                for i in range(2, len(words)):
                    f.writelines(' ' + words[i])
                f.writelines('\n\n')
        self.save_config()

    def handle_cmd_help(self, roomid, words, authority):
        file_path = os.path.join(ASSERTS_PATH, 'help.png')
        if len(words) > 1:
            if (authority & SUPER_ADMIN) != 0 and words[1].__eq__('all'):
                self.send_img_msg(roomid, file_path)
            else:
                self.send_txt_msg(roomid, '未知可选项，请检查输入')
        else:
            self.send_img_msg(roomid, file_path)

    def handle_cmd_quotations(self, roomid, words, authority):
        name = words[1]
        if (authority & SUPER_ADMIN) != 0:
            #  显示语录数量
            if name.__eq__('list'):
                # 显示某个人的语录
                if len(words) > 2:
                    lines = ['']
                    for quotation in self.config['quotations']:
                        if words[2] in quotation['alias']:
                            dir_path = os.path.join(os.getcwd(), 'data\\quotations\\' + quotation['path'])
                            files = os.listdir(dir_path)
                            for file_name in files:
                                fn, _ = file_name.split('.')
                                lines.append(fn)
                            lines[0] = '「{}」共收录{}条语录：'.format(words[2], len(files))
                            # print(lines)
                            self.send_txt_msg(roomid, '\n'.join(lines))
                            return True
                    self.send_txt_msg('nobody nobody call 「{}」~~'.format(words[2]))
                lines = ['']
                total = 0
                for quotation in self.config['quotations']:
                    dir_path = os.path.join(os.getcwd(), 'data\\quotations\\' + quotation['path'])
                    files = os.listdir(dir_path)
                    if self.wxid2name.__contains__(quotation['wxid']) and self.wxid2name[
                        quotation['wxid']].__contains__(roomid):
                        lines.append('「{}」{}条'.format(self.get_name(roomid, quotation['wxid']), len(files)))
                        total += len(files)
                if total == 0:
                    lines[0] = '目前该群聊没有收集到语录呢~'
                else:
                    lines[0] = '共收录语录{}条，其中'.format(total)
                print(lines)
                self.send_txt_msg(roomid, '\n'.join(lines))
                return
            # 添加语录
            elif name.__eq__('add'):
                if len(words) < 3: return
                user = words[2]
                for quotation in self.config['quotations']:
                    if user in quotation['alias']:
                        self.quotation_path = quotation['path']
                        self.state = QUOTATION_RECV1
                        if len(words) >= 4:
                            self.quotation_name = words[3]
                        return
                self.send_txt_msg(roomid, '布吉岛你说的「{}」是谁呢~'.format(user))
                return
            elif name.__eq__('alias'):
                if len(words) != 4: return
                user = words[2]
                for quotation in self.config['quotations']:
                    if user in quotation['alias']:
                        quotation['alias'].append(words[3])
                        self.send_txt_msg(roomid, '添加别名成功~')
                        self.save_config()
                        return
                self.send_txt_msg(roomid, '布吉岛你说的「{}」是谁呢~'.format(user))
                return
        for quotation in self.config['quotations']:
            if name in quotation['alias']:
                dir_path = os.path.join(os.getcwd(), 'data\\quotations\\' + quotation['path'])
                print(dir_path)
                files = os.listdir(dir_path)
                if len(words) > 2:
                    keyword = words[2:]
                    for file in files:
                        for word in keyword:
                            if word in file:
                                file_path = os.path.join(dir_path, file)
                                self.send_img_msg(roomid, file_path)
                                return
                file_path = os.path.join(dir_path, random.choice(files))
                self.send_img_msg(roomid, file_path)
                return
        self.send_txt_msg(roomid, '目前没有「{}」的语录呢~'.format(name))

    def handle_cmd_stock(self, roomid, words, authority):
        if len(words) == 1:
            self.send_txt_msg(roomid, '正在同步自选股票实时数据...')
            df = tushare.get_realtime_quotes(self.config['stock'])
            df['change'] = df.apply(
                lambda x: '{:.2%}'.format((float(x['price']) - float(x['pre_close'])) / float(x['pre_close'])), axis=1)
            df['change_price'] = df.apply(lambda x: '{:g}'.format(float(x['price']) - float(x['pre_close'])), axis=1)
            df['volume'] = df.apply(lambda x: utils.num2unit(int(float(x['volume']))), axis=1)
            df['amount'] = df.apply(lambda x: utils.num2unit(int(float(x['amount']))), axis=1)

            stock_data = df[
                ['code', 'name', 'price', 'change', 'change_price', 'pre_close', 'open', 'high', 'low', 'volume', 'amount',
                 'date', 'time']]
            stock_data = stock_data.rename(
                columns={'code': '股票代码', 'name': '股票名称', 'price': '最新价', 'change': '涨跌幅', 'change_price': '涨跌额', 'open': '开盘价',
                         'pre_close': '昨收', 'high': '当日最高', 'low': '当日最低', 'volume': '成交量', 'amount': '成交额',
                         'date': '日期', 'time': '时间'})
            file_path = os.path.join(TMP_PATH, 'stock.png')
            stock_data.dfi.export(file_path)
            self.send_img_msg(roomid, file_path)
        elif authority == SUPER_ADMIN:
            if words[1].__eq__('添加自选'):
                for i in range(2, len(words)):
                    if words[i] not in self.config['stock']:
                        self.config['stock'].append(words[i])
                self.save_config()
                self.send_txt_msg(roomid, '添加完成！')
            elif words[1].__eq__('删除自选'):
                for i in range(2, len(words)):
                    if words[i] in self.config['stock']:
                        self.config['stock'].remove(words[i])
                self.save_config()
                self.send_txt_msg(roomid, '删除完成！')

    def handle_cmd_strategy(self, roomid, words):
        topk = 5
        daydelta = 1
        if len(words) > 1:
            topk = int(words[1])
        if len(words) > 2:
            daydelta = int(words[2])
        res = stock.get_rec_stock_list(None, topk, daydelta)
        date = datetime.datetime.now().date() - datetime.timedelta(days=daydelta)
        if len(res) == 0:
            self.send_txt_msg(roomid, '{}的数据还没出来哦，自动查找更早之前的策略'.format(date.strftime('%Y-%m-%d')))
            while len(res) == 0:
                print('查找{}的数据'.format(date.strftime('%Y-%m-%d')))
                daydelta += 1
                res = stock.get_rec_stock_list(None, topk, daydelta)
                date = datetime.datetime.now().date() - datetime.timedelta(days=daydelta)
        lines = []
        for item in res:
            name = item['name']
            if len(item['name']) < 4:
                name = item['name'] + '    ' + ('    ' * (4 - len(item['name'])))
            lines.append('{:<10} {:<8} {:.1f}'.format(item['code'], name, item['rec']))
        if len(lines) > 0:
            lines.insert(0, '{:<8} {:<8} {}'.format('股票代码', '股票名称', '评分'))
            lines.insert(0, '[{}][北向策略]：'.format(date.strftime('%Y-%m-%d')))
        else:
            lines.append('{}的数据还没出来哦~'.format(date.strftime('%Y-%m-%d')))
        self.send_txt_msg(roomid, '\n'.join(lines))

    def handle_cmd_weather(self, roomid, words):
        if len(words) < 2:
            return
        city = words[1]
        ext = 'base'
        day = 3
        if words[0] == '天气预报':
            ext = 'all'
        if len(words) > 2 and words[2].isnumeric():
            day = int(words[2])
        res = amap_api.get_weather(city, ext, day)
        if res:
            self.send_txt_msg(roomid, res)
        else:
            self.send_txt_msg(roomid, '没有该城市的数据啦！')

    def handle_cmd_statistics(self, roomid, words):
        details = False
        reverse = True
        if len(words) > 1:
            for i in range(1, len(words)):
                if words[i].__eq__('详情'):
                    details = True
                elif words[i].__eq__('升序'):
                    reverse = False
                else:
                    self.send_txt_msg(roomid, '未知可选项，请检查输入')
                    return
        if not self.data['record'].__contains__(roomid):
            self.send_txt_msg(roomid, '本群暂无统计记录！')
            return
        lines = ['统计发言信息：', '']
        total = 0
        infos = []
        for user in self.data['record'][roomid].keys():
            name = self.get_name(roomid, user)
            speak = len(self.data['record'][roomid][user])
            count = self.data['key_word_count'][user]
            total += speak
            infos.append((name, speak, count))
        infos.sort(key=lambda info: info[1], reverse=reverse)
        for info in infos:
            if details:
                lines.append('「{}」共发言{}次（触发关键词{}次）'.format(info[0], info[1], info[2]))
            else:
                lines.append('「{}」共发言{}次'.format(info[0], info[1]))
        lines[1] = '本群共发言{}次，其中'.format(total)
        self.send_txt_msg(roomid, '\n'.join(lines))

    def handle_cmd_repeat(self, roomid, words, authority):
        if len(words) == 3:
            name = words[1].lstrip('@').rstrip(' ')
            if not self.name2wxid.__contains__(name):
                self.send_txt_msg(roomid, '没有找到「{}」这个人了捏'.format(name))
                return
            id = self.name2wxid[name]
            if not self.data['record'].__contains__(roomid) or not self.data['record'][roomid].__contains__(id):
                self.send_txt_msg(roomid, '没有记录了啦')
                return
            try:
                num = int(words[2])
            except:
                self.send_txt_msg(roomid, '你输入的数字有误，请检查输入')
                return
            if num <= 0:
                self.send_txt_msg(roomid, '输入的n必须为正整数，请检查输入')
                return
            if not self.data['record'].__contains__(roomid) or not self.data['record'][roomid].__contains__(id):
                self.send_txt_msg(roomid, '「{}」没有说过话了啦'.format(name))
                return
            elif num > len(self.data['record'][roomid][id]):
                print(num, len(self.data['record'][roomid][id]))
                self.send_txt_msg(roomid, '「{}」只说过{}句话了啦'.format(name, len(self.data['record'][roomid][id])))
                time.sleep(1)
                num = len(self.data['record'][roomid][id])
            if num > 20 and authority != SUPER_ADMIN:
                self.send_txt_msg(roomid, '你想知道那么多句干什么了啦！问过我的敲级管理员了吗！顶多告诉你20句了啦')
                time.sleep(1)
                num = 20
            lines = ['「{}」最后说过的{}句话：'.format(name, num)]
            for say in self.data['record'][roomid][id][len(self.data['record'][roomid][id]) - num:]:
                lines.append(say)
            self.send_txt_msg(roomid, '\n'.join(lines))
        else:
            self.send_txt_msg(roomid, '指令有误，请检查输入')

    def handle_cmd_admin(self, roomid, words):
        cmd = words[0]
        if cmd.__eq__('添加管理员'):
            if len(words) > 1:
                for i in range(1, len(words)):
                    if not self.name2wxid.__contains__(words[i]):
                        self.send_txt_msg(roomid, '没有找到「{}」这个人！'.format(words[i]))
                        continue
                    if self.name2wxid[words[i]] not in self.config['admin']:
                        self.config['admin'].append(self.name2wxid[words[i]])
                        self.send_txt_msg(roomid, '成功添加「{}」为管理员~'.format(words[i]))
                        self.save_config()
                    else:
                        self.send_txt_msg(roomid, '「{}」已经是管理员啦！'.format(words[i]))
            return True
        elif cmd.__eq__('删除管理员'):
            if len(words) > 1:
                for i in range(1, len(words)):
                    if not self.name2wxid.__contains__(words[i]):
                        self.send_txt_msg(roomid, '没有找到「{}」这个人！'.format(words[i]))
                        continue
                    if self.name2wxid[words[i]] in self.config['admin']:
                        self.config['admin'].remove(self.name2wxid[words[i]])
                        self.send_txt_msg(roomid, '已撤销「{}」的管理员身份'.format(words[i]))
                        self.save_config()
                    else:
                        self.send_txt_msg(roomid, '「{}」不是管理员！'.format(words[i]))
            return True
        elif cmd.__eq__('查看管理员'):
            lines = ['本群的超级管理员：']
            nobody = True
            for wxid in self.config['super_admin']:
                if not self.wxid2name[wxid].__contains__(roomid):
                    continue
                lines.append(self.get_name(roomid, wxid))
                nobody = False
            if nobody:
                lines.append('无')
            nobody = True
            lines.append('本群的管理员：')
            for wxid in self.config['admin']:
                if not self.wxid2name[wxid].__contains__(roomid):
                    continue
                lines.append(self.get_name(roomid, wxid))
                nobody = False
            if nobody:
                lines.append('无')
            self.send_txt_msg(roomid, '\n'.join(lines))
            return True
        return False

    def handle_cmd_pm(self, roomid, words):
        cmd = words[0]
        if cmd.__eq__('添加pm'):
            if len(words) > 1:
                for i in range(1, len(words)):
                    if not self.name2wxid.__contains__(words[i]):
                        self.send_txt_msg(roomid, '没有找到「{}」这个人！'.format(words[i]))
                        continue
                    if self.name2wxid[words[i]] not in self.config['pm']:
                        self.config['pm'].append(self.name2wxid[words[i]])
                        self.send_txt_msg(roomid, '成功添加「{}」为PM大大~'.format(words[i]))
                        self.save_config()
                    else:
                        self.send_txt_msg(roomid, '「{}」已经是PM大大啦！~')
            return True
        elif cmd.__eq__('删除pm'):
            if len(words) > 1:
                for i in range(1, len(words)):
                    if not self.name2wxid.__contains__(words[i]):
                        self.send_txt_msg(roomid, '没有找到「{}」这个人！'.format(words[i]))
                        continue
                    if self.name2wxid[words[i]] in self.config['pm']:
                        self.config['pm'].remove(self.name2wxid[words[i]])
                        self.send_txt_msg(roomid, 'PM大大「{}」已被开除！'.format(words[i]))
                        self.save_config()
                    else:
                        self.send_txt_msg(roomid, '「{}」不是PM大大！')
            return True
        elif cmd.__eq__('查看pm'):
            lines = ['本群的PM大大们：']
            for wxid in self.config['pm']:
                if not self.wxid2name[wxid].__contains__(roomid):
                    continue
                lines.append(self.get_name(roomid, wxid))
            if len(lines) == 1:
                lines.append('无')
            self.send_txt_msg(roomid, '\n'.join(lines))
            return True
        return False

    def handle_cmd_qa(self, roomid, words):
        cmd = words[0]
        if cmd.__eq__('添加qa'):
            if len(words) > 1:
                for i in range(1, len(words)):
                    if not self.name2wxid.__contains__(words[i]):
                        self.send_txt_msg(roomid, '没有找到「{}」这个人！'.format(words[i]))
                        continue
                    if self.name2wxid[words[i]] not in self.config['qa']:
                        self.config['qa'].append(self.name2wxid[words[i]])
                        self.send_txt_msg(roomid, '成功添加「{}」为QA大大~'.format(words[i]))
                        self.save_config()
                    else:
                        self.send_txt_msg(roomid, '「{}」已经是QA大大啦！~')
            return True
        elif cmd.__eq__('删除qa'):
            if len(words) > 1:
                for i in range(1, len(words)):
                    if not self.name2wxid.__contains__(words[i]):
                        self.send_txt_msg(roomid, '没有找到「{}」这个人！'.format(words[i]))
                        continue
                    if self.name2wxid[words[i]] in self.config['qa']:
                        self.config['qa'].remove(self.name2wxid[words[i]])
                        self.send_txt_msg(roomid, 'QA大大「{}」已被开除！'.format(words[i]))
                        self.save_config()
                    else:
                        self.send_txt_msg(roomid, '「{}」不是QA大大！')
            return True
        elif cmd.__eq__('查看qa'):
            lines = ['本群的QA大大们：']
            for wxid in self.config['qa']:
                if not self.wxid2name[wxid].__contains__(roomid):
                    continue
                lines.append(self.get_name(roomid, wxid))
            if len(lines) == 1:
                lines.append('无')
            self.send_txt_msg(roomid, '\n'.join(lines))
            return True
        return False

    def handle_cmd_update(self, roomid, words):
        day = 7
        if len(words) > 1:
            day = int(words[1])
        self.send_txt_msg(roomid, '开始更新{}天内的数据...'.format(day))
        update.update(day)
        self.send_txt_msg(roomid, '更新完成！')


def main():
    print('[wxbot] 正在注入DLL...')
    injector_path = os.path.join(INJECTOR_PATH, 'injector.exe')
    dll_path = os.path.join(INJECTOR_PATH, 'version3.2.1.121.dll')
    cmd = '{} {} {}'.format(injector_path, 'WeChat.exe', dll_path)
    print('[CMD] {}'.format(cmd))
    r = os.system(cmd)
    if r != 0:
        print('[wxbot] 注入失败，进程结束！')
        return
    print('[wxbot] 注入成功！正在启动微信机器人...')
    time.sleep(1)
    bot = WeChatBot()
    bot.run(enable_trace=False)


if __name__ == '__main__':
    main()
