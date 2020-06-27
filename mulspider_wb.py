# coding=utf-8
import mysql
from selenium import webdriver
import time
import re
from multiprocessing import Pool
from bs4 import BeautifulSoup
import error_email
import os
import traceback


db = mysql.Database()
base_url = 'https://weibo.cn'
ERROR_INFO = ''

# 获取用户名，关注数等信息
def get_user_info(bs, userid, type):
    bs.get('https://weibo.cn/' + userid)
    bs.implicitly_wait(2)
    pagesource = bs.page_source.encode('utf-8').decode('utf-8')
    if '您当前访问的用户状态异常' in pagesource:
        sql = "insert into users(user_id,username,weibo_num,follows_num,fans_num,note) select '{}','{}',{},{},{},'{}' from dual where not exists (select user_id from users where user_id='{}')".format(
            userid, 'banner_acc', 0, 0, 0, type, userid);
        db.insert(sql)
    else:
        name = bs.find_element_by_class_name('ctt').text
        name = name.split()[0]
        raw_info = bs.find_element_by_class_name('tip2').text
        info = re.findall(r'\[(\d+)\]', raw_info)
        sql = "insert into users(user_id,username,weibo_num,follows_num,fans_num,note) select '{}','{}',{},{},{},'{}' from dual where not exists (select user_id from users where user_id='{}')".format(
            userid, name, info[0], info[1], info[2], type, userid);
        db.insert(sql)
    time.sleep(2)


# 获得微博内容，转发数，时间等
def get_weibo_info(bs, source, userid):
    global ERROR_INFO
    for i in range(1, len(source)-2):
        # bug检测
        if len(source[i].select(".ct")) < 1:
            ERROR_INFO += source[i].text
            break
        posttime = source[i].select(".ct")[0].text
        text = source[i].text
        like, repost, comment = re.findall(r'赞\[(\d+)\]\s转发\[(\d+)\]\s评论\[(\d+)\]', text)[0]
        wb_content = source[i].select('div')[0].text
        if '转发了' in wb_content and len(source[i].select('div')) > 1:
            wb_content += source[i].select('div')[-1].text
        if '...全文' in wb_content:
            fulltext_link = source[i].select('div')[0].select('a')
            for item in fulltext_link:
                if '全文' in item.text:
                    new_url = base_url + item['href']
                    flag = 3  # 防止页面打开失败, 并设置重试次数
                    while flag > 0:
                        bs.get(new_url)
                        time.sleep(2)
                        tmp_pagesource = bs.page_source.encode('utf-8').decode('utf-8')
                        if '她还没发过微博.' in tmp_pagesource:
                            flag -= 1
                            continue
                        elif '如果没有自动跳转,请' in tmp_pagesource:
                            flag -= 1
                            continue
                        elif '<title>我的首页</title>' in tmp_pagesource:
                            flag -= 1
                            continue
                        else:
                            soup = BeautifulSoup(tmp_pagesource, 'html.parser')
                            tmp_select = soup.select('.c')
                            if len(tmp_select) == 3:
                                flag -= 1
                                continue
                            else:
                                flag = -1

                    with open('test.txt', 'w', encoding='utf-8') as file:
                        file.write(tmp_pagesource)

                    if flag == 0:
                        error_email.send_email('{}用户全文显示时出现异常，{} Source code:{}\n'.format(userid, new_url, tmp_pagesource))

                        ERROR_INFO += '{}用户全文显示时出现异常， Source code:{}\n'.format(userid, tmp_pagesource)
                    else:
                        # 检查是否数组越界
                        if len(tmp_select) < 2:
                            ERROR_INFO += tmp_pagesource + '\n'
                            error_email.send_email(userid + '数组越界: ' + tmp_pagesource)
                        else:
                            wb_content = tmp_select[1].select('div')[0].text if len(tmp_select[1].select('div')) > 0 else \
                            tmp_select[2].select('div')[0].text
                            lt = [userid, wb_content, posttime, repost, comment, like]
                            lt = [mysql.pymysql.escape_string(i) for i in lt]  # 转义特殊sql字符
                            sql = "insert into wb(user_id, content, post_time, repost_num, comment_num, like_num) " \
                                  "values('{}','{}','{}',{},{},{});".format(*lt)
                            db.insert(sql)
                        # wb_content = soup.select('.c')[1].select('div')[0].text
        else:
            lt = [userid, wb_content, posttime, repost, comment, like]
            lt = [mysql.pymysql.escape_string(i) for i in lt]    # 转义特殊sql字符
            sql = "insert into wb(user_id, content, post_time, repost_num, comment_num, like_num) values('{}','{}'," \
                  "'{}',{},{},{});".format(*lt)
            db.insert(sql)


def get_wb_content(bs, userid, type):
    global ERROR_INFO
    get_user_info(bs, userid, type)
    wb_num = db.query("select weibo_num from users where user_id={};".format(userid))[0][0]
    if wb_num == 0:
        print("weibo_num: 0.")
    elif wb_num < 11:
        bs.get('https://weibo.cn/' + userid)
        time.sleep(2)
        pagesource = bs.page_source.encode('utf-8').decode('utf-8')
        if '还没发过微博.' in pagesource:
            ERROR_INFO += '{} 首页显示未发过微博，需要核实\n'.format(userid)
            error_email.send_email('{} 首页显示未发过微博，需要核实\n'.format(userid))
            return
        else:
            soup = BeautifulSoup(pagesource, 'html.parser')
            class_c = soup.select(".c")
            get_weibo_info(bs, class_c, userid)
    else:
        flag = 3  # 防止页面打开失败,并限制重试次数
        while flag > 0:
            bs.get('https://weibo.cn/'+userid)
            time.sleep(2)
            pagesource = bs.page_source.encode('utf-8').decode('utf-8')
            if '她还没发过微博.' in pagesource:
                flag -= 1
                continue
            elif '如果没有自动跳转,请' in pagesource:
                flag -= 1
                continue
            elif not re.search(r'1/\d+页</div>', pagesource): # 防止首页没有页码数
                ERROR_INFO += '{}首页无法显示用户页码\n'.format(userid)
                flag -= 1
                print(ERROR_INFO)
                continue
            elif '<title>我的首页</title>' in pagesource:
                flag -= 1
                continue
            else:
                soup = BeautifulSoup(pagesource, 'html.parser')
                class_c = soup.select('.c')
                if len(class_c) == 3:
                    flag -= 1
                    continue
                else:
                    flag = -1
        with open('test.txt', 'w', encoding='utf-8') as file:
            file.write(pagesource)
        if flag == 0:
            ERROR_INFO += '{}首页重试次数超限\n'.format(userid)
            error_email.send_email('{}首页重试次数超限\n'.format(userid))
        else:
            soup = BeautifulSoup(pagesource, 'html.parser')
            page_num = int(re.findall(r'1/(\d+)页', soup.select(".pa")[0].text)[0])
            page_num = page_num if page_num < 8 else 8
            class_c = soup.select(".c")
            get_weibo_info(bs, class_c, userid)  # 获取第一页微博内容
            for num in range(2, page_num + 1):
                flag = 3  # 防止页面打开失败
                while flag > 0:
                    bs.get('https://weibo.cn/' + userid + '?page=' + str(num))
                    time.sleep(2)
                    pagesource = bs.page_source.encode('utf-8').decode('utf-8')
                    if '还没发过微博.' in pagesource:
                        flag -= 1
                        continue
                    elif '如果没有自动跳转,请' in pagesource:
                        flag -= 1
                        continue
                    elif '<title>我的首页</title>' in pagesource:
                        flag -= 1
                        continue
                    else:
                        soup = BeautifulSoup(pagesource, 'html.parser')
                        class_c = soup.select('.c')
                        if len(class_c) == 3:
                            flag -= 1
                            continue
                        else:
                            flag = -1
                print('current page: {}/{}'.format(num, page_num))
                # 测试代码
                with open('test.txt', 'w', encoding='utf-8') as file:
                    file.write(pagesource)
                if flag == 0:
                    ERROR_INFO += '{}用户重试次数超限，当前错误页面{}/{}\n'.format(userid, num, page_num)
                    error_email.send_email('{}用户重试次数超限，当前错误页面{}/{}\n'.format(userid, num, page_num))
                else:
                    get_weibo_info(bs, class_c, userid)


def func(arr, slp, type):
    time.sleep(slp)
    bs = webdriver.Chrome()
    bs.get(
        'https://passport.weibo.cn/signin/login?entry=mweibo&r=https%3A%2F%2Fweibo.cn%2F&backTitle=%CE%A2%B2%A9&vt=')
    bs.implicitly_wait(10)
    bs.find_element_by_id('loginName').send_keys('15835693634')
    bs.find_element_by_id('loginPassword').send_keys('zc123456')
    bs.find_element_by_id('loginAction').click()
    time.sleep(2)
    count = 1
    for userid in arr:
        print('Current userid: ', userid, ', Progress: {}/{}'.format(count, len(arr)))
        # get_user_info(bs, userid)
        get_wb_content(bs, userid, type)
        count += 1


if __name__ == '__main__':
    # 384450进度
    # --*--当前进度 1300个用户
    # 4127个
    # user_list = os.listdir('./../dataset/fake_account/fake_account_raw')
    # user_list = [i[:-5] for i in user_list]
    # user_list = user_list[389:400]
    # print(user_list)
    # user_list = ['3564403743', '5930539557', '5941484734', '6022577916', '6004001244', '5936389679', '6014891160', '6004140831', '5861679059', '6137754512', '5942043354', '5951318788', '3187790394', '5898074374', '6034157712', '5936600773', '6015344662', '5951693244', '6055897272']
    # for i in user_list:
    #     print(db.query("select * from users where user_id='{}';".format(i)))
    # exit()
    # func(user_list, 0, 'yellow')


    try:
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        u1 = ['5727802278', '3985073867', '5377598285', '5091084851', '5301236724', '3165820465', '5505851216', '3822712451', '2635172317', '5607072131']
        u2 = ['5224178651', '5292093513', '5543871990', '3514150117', '5573225406', '2122149263', '5608624080', '1973593033', '2978842514']
        pool = Pool(2)
        arr = [u1, u2]
        sep_time = 0 # 设置登录间隔，防止登录卡死
        res = list()
        for item in arr:
            res.append(pool.apply_async(func, args=(item, sep_time, 'normal')))
            sep_time = 4
        pool.close()
        pool.join()
        for item in res:
            item.get(11)
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        # 保存错误日志
        with open('error.log', 'w+', encoding='utf-8') as f:
            f.writelines(ERROR_INFO)
            print(ERROR_INFO)
            error_email.send_email('本次任务完成。\n' + ERROR_INFO)
    except Exception as e:
        error_email.send_email(traceback.format_exc() + '\n' + '本次任务信息：\n' + ERROR_INFO)
        print(ERROR_INFO)
        with open('error.log', 'w+', encoding='utf-8') as f:
            f.writelines(ERROR_INFO)
