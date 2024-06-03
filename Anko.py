# -*- coding: utf-8 -*-
# Original author: https://github.com/ChangingSelf/anko-thread-weekly
# Edited by cloud_rider@NGA
# 导入
import pandas as pd
from datetime import datetime
import re

from openpyxl.utils import get_column_letter
from pandas import ExcelWriter
import numpy as np
import pyperclip

import os
# Functions

# 数据读取与预处理
def load_data(filepath):
    """
    读取xlsx并预处理数据
    """

    def convert_uid(x: str):
        return x.strip("用户ID ")

    def convert_date(x: str):
        return datetime.strptime(x, "%y-%m-%d %H:%M")

    def get_tid(x: str):
        m = re.search(r'tid=(\d+)', x)
        if m :
            return m.group(1)
        else:
            return "-1"

    data = pd.read_excel(io=filepath, usecols='C:K',
                         converters={'uid': convert_uid, 'publish_time': convert_date, 'reply_time': convert_date})

    # 补全用户名
    for index, row in data.iterrows():
        if not pd.isnull(row['publisher_fullname']):
            row['publisher'] = row['publisher_fullname']
        data.iloc[index] = row
    data.drop('publisher_fullname', axis=1, inplace=True)

    # 提取tid
    data['tid'] = data['link'].map(get_tid)

    # 删除link列（因为可以通过tid得到，就不需要这种冗余数据了）
    data.drop('link', axis=1, inplace=True)

    # 将楼层为空的行填充
    # 将空分区填充为空字符串
    data.fillna({'level': 0, 'area': ''}, inplace=True)

    # 将楼层列转化为整数
    data['level'] = data['level'].astype('int64')

    # 匿名用户的uid改为-1
    data.loc[data['uid'].str.startswith("#anony"), 'uid'] = -1

    # 调整列顺序
    data = data[['tid', 'level', 'title', 'uid', 'publisher', 'publish_time', 'reply_time', 'area']]

    # 将tid设置为索引
    data.set_index('tid', inplace=True, drop=False)
    return data

# 保存预处理数据
def to_excel_auto_column_weight(df: pd.DataFrame, writer: ExcelWriter, sheet_name):
    """
    DataFrame保存为excel并自动设置列宽
    代码来源：https://laowangblog.com/pandas-openpyxl-excel-column-dimensions.html
    """
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    #  计算表头的字符宽度
    column_widths = (
        df.columns.to_series().apply(lambda x: len(x)).values
    )
    #  计算每列的最大字符宽度
    max_widths = (
        df.astype(str).applymap(lambda x: len(x)).agg(max).values
    )
    # 计算整体最大宽度
    widths = np.max([column_widths, max_widths], axis=0)
    # 设置列宽
    worksheet = writer.sheets[sheet_name]
    for i, width in enumerate(widths, 1):
        # openpyxl引擎设置字符宽度时会缩水0.5左右个字符，所以干脆+2使左右都空出一个字宽。
        worksheet.column_dimensions[get_column_letter(i)].width = width + 2

# 过滤非单人安科帖
def filter_anko_threads(data:pd.DataFrame):
    # 去除地下城分区的帖子
    data = data[data['area'] != "[地下城(多人安科/TRPG区)]"]
    # 去除图书馆分区的帖子
    data = data[data['area'] != "[图书馆(资料交流区)]"]
    # 筛选含有安科标签的帖子
    data = data[data['title'].str.contains('\[安科\]') | data['title'].str.contains('\[安价\]') | data['title'].str.contains('\[安科/安价\]') | data['title'].str.contains('\[安价/安科\]')]
    return data

def diff(df1: pd.DataFrame, df2: pd.DataFrame):
    '''
    求差集，返回 df1 - df2
    '''
    return pd.concat([df1, df2, df2]).drop_duplicates(subset=['tid'], keep=False)

# 筛选出本周达到x楼的安科的数据
def filter_arrive_x_level(last_source: pd.DataFrame, this_source: pd.DataFrame, x: int):
    """
    筛选出本周达到x楼的安科的数据
    """
    # 已经达到x楼
    this_dist = this_source[(this_source['level'] >= x)]
    last_dist = last_source[(last_source['level'] >= x)]

    # 去除上次已经达到的，即两者差集，this_dist - last_dist
    # result = pd.concat([this_dist,last_dist,last_dist]).drop_duplicates(subset=['tid'],keep=False)
    result = diff(this_dist, last_dist)

    # 如果得到的tid不存在于上次的表中(可能是新发布或是上次采集数据时进入审核，后者需要去掉)，则去除
    # 即，筛选result中tid存在于上次的表中的行，或者发布时间在两次的表的收集时间之间的行
    result = result[(result['tid'].isin(last_source['tid'])) | (
                (datetime_last < result['publish_time']) & (result['publish_time'] < datetime_this))]

    # 按照楼层升序
    result = result.sort_values(by='level', ascending=True)

    return result

# 数据统计
# 小于x层的安科数
def num_lower_than_x(x: int):
    return len(data_this[data_this['level'] < x])

# 本周完结的安科
def filter_finished_threads(last_source: pd.DataFrame, this_source: pd.DataFrame):
    last_dist = last_source[last_source['area'] == "[记忆回廊(完结区)]"]
    this_dist = this_source[this_source['area'] == "[记忆回廊(完结区)]"]

    # 去除上次已经达到的，即两者差集，this_dist - last_dist
    result = diff(this_dist, last_dist)

    # 如果得到的tid不存在于上次的表中，则去除。原因是：这次与上次的差集除了是这段时间内完结的帖子之外，还可能是上次收集时被NGA吞楼的以往已经完结的帖子
    # 即，筛选result中tid存在于上次的表中的行
    # result = result[result['tid'].isin(last_source['tid'])]

    # 年度报告临时筛选
    # result = result[result['publish_time'].dt.year == 2023]

    return result

# 获取复更安科
def get_revive_threads(data:pd.DataFrame):
    black_list = ['35282960','31463087','26768686','27077547','36914755','36124954','35662098']
    return data[~data['tid'].isin(black_list) & data['title'].str.contains('\[恢复更新\]') & ((datetime_this- data['reply_time']) <= pd.Timedelta(days=30))].sort_values(by='level',ascending=True)

# 获取日更安科
def get_daily_update_threads(data:pd.DataFrame):
    black_list = ['25896944','38714676','39077655','39288834','36790101','37931225','39748475','39747436']
    return data[~data['tid'].isin(black_list) & data['title'].str.contains('\[日更\]') & ((datetime_this- data['reply_time']) <= pd.Timedelta(days=7))].sort_values(by='level',ascending=True)

# 获取活动安科
def get_activity_threads(data:pd.DataFrame):
    black_list = []
    return data[~data['tid'].isin(black_list) & data['title'].str.contains('\[更新挑战\]') & ((datetime_this- data['reply_time']) <= pd.Timedelta(days=7))].sort_values(by='level',ascending=True)

'''
def get_brief(tid: str, size: int):
    response = requests.get(f"https://ngabbs.com/read.php?tid={tid}&__output=11", headers=headers)
    print(response.status_code)  # 获取响应状态码
    print(response.headers)  # 获取响应头
    print(response.content)  # 获取响应内容

    json_data = response.json()

    content = json_data['data']['__R'][0]['content']
    content = re.sub(r"(\[.*?\])", "", content)  # 删除所有中括号代码
    content = re.sub(r"<br/>", "", content)  # 删除所有中括号代码

    return content[0:size]
'''
# 输出排版文本
def data_to_bbcode(data: pd.DataFrame, col_name='level', show_reply_time=False, is_fold=True, is_order_list=False):
    if len(data) == 0: return "无"

    output = ""
    FOLD_MAX = 5  # 超过多少个安科进行折叠
    BRIEF_NUM = 20  # 简介字数

    if is_fold and len(data) > FOLD_MAX: output += f"[collapse={len(data)}个安科]"

    output += f"[list{'=1' if is_order_list else ''}]\n"
    for index, row in data.iterrows():

        title = str(row['title'])

        # 是否为本周新发布安科
        recommend_tag = ""
        if (datetime_last <= row['publish_time']) and (row['publish_time'] <= datetime_this):
            recommend_tag += "[color=green][b][新][/b][/color]"

        if re.search("\[创作活动[-—]超能都市\]", title):
            recommend_tag += "[color=orangered][b][活动][/b][/color]"

        if re.search("\[2024新年活动\]", title):
            recommend_tag += "[color=orangered][b][活动][/b][/color]"

        if re.search("\[更新挑战\]", title):
            recommend_tag += "[color=purple][b][活动][/b][/color]"

        if re.search("\[恢复更新\]", title):
            recommend_tag += "[color=blue][b][复更][/b][/color]"

        if re.search("\[日更\]", title):
            recommend_tag += "[color=tomato][b][日更][/b][/color]"

        # 标题处理
        title = title.replace("[安科/安价]", "")
        title = title.replace("[安价/安科]", "")
        title = title.replace("[安科]", "")
        title = title.replace("[安价]", "")

        # title = re.sub(r"(\[.*?\])","[color=silver]\g<1>[/color]",title)
        # title = re.sub(r"(\(.*?\)|（.*?）)","[color=silver]\g<1>[/color]",title)
        title = re.sub(r"(\(.*?\)|（.*?）)", "", title)  # 删除所有括号
        # title = re.sub(r"(\[创作活动[-—]超能都市\])","[b]\g<1>[/b]",title) # 将活动标签标粗
        title = re.sub(r"(\[.*?\])", "", title)  # 删除所有标签

        title = title.strip()

        if title == '': title = '[del]（此安科因标题全部由标签或者括号文本构成而被处理掉了）[/del]'

        # 读取数据
        # brief = get_brief(row['tid'],20)

        reply_time = f"({(datetime_this - row['reply_time']).days}天前更新)" if show_reply_time else ""

        output += f"[*][{row[col_name]}]{recommend_tag}[url=https://ngabbs.com/read.php?tid={row['tid']}]{title}[/url]{reply_time}\n"
    output += "[/list]"

    if is_fold and len(data) > FOLD_MAX: output += "[/collapse]"

    return output

def get_exceed_prompt(x: int):
    return f"[color=silver]已超过{num_lower_than_x(x)}/{thread_sum}=[b]{round(num_lower_than_x(x) / thread_sum * 100, 2)}%[/b]的安科[/color]"

def set_milestone(x: int, next_x: int):
    this_milestones = filter_arrive_x_level(data_last, data_this, x)
    next_milestones = filter_arrive_x_level(data_last, data_this, next_x)
    this_milestones = diff(this_milestones, next_milestones)  # 去除下一个里程碑里含有的帖子

    label = f"{x}层"
    desc = ""  # 等级描述

    if x == 50:
        label = "[color=green][b]50层(入门级)[/b][/color]"
        desc = ""
    if x == 500:
        label = "[color=blue][b]500层(殿堂级)[/b][/color]"
        desc = ""
    if x == 5000:
        label = "[color=purple][b]5000层(传说级)[/b][/color]"
        desc = ""
    if x == 50000:
        label = "[color=red][b]50000层(神话级)[/b][/color]"
        desc = ""

    return f"""[align=center][size=150%]{label}[/size]
{get_exceed_prompt(x)}
{desc}[/align]
[quote]{data_to_bbcode(this_milestones)}[/quote]
"""

def set_milestones(milestone_list: list):
    output = ""
    for i in range(len(milestone_list)):
        x = milestone_list[i]
        next_x = milestone_list[i + 1] if i + 1 < len(milestone_list) else milestone_list[i]
        output += set_milestone(x, next_x)
    return output

# Script
os.makedirs("raw_data", exist_ok=True)
os.makedirs("data", exist_ok=True)
# 读取rawdata文件夹下的文档，选出其中最近和次近的两个作为this和last
file_time_format = "%Y-%m-%d-%H%M"  # 本地文件名的时间格式，目前是nga-thread-2023-02-05-1208.xlsx这种格式

rawdata_filenames = os.listdir("raw_data")
rawdata_filenames = [re.sub("[^0-9\-]", "",x) for x in rawdata_filenames]
rawdata_filenames = [x.split("-")[2:] for x in rawdata_filenames]

if len(rawdata_filenames) < 2:
    print("采集数据文件数量不足，请检查raw_data文件夹下是否有至少两个采集数据文件")

rawdata_filetime = []
for x in rawdata_filenames:
    rawdata_filetime.append(datetime(int(x[0]), int(x[1]), int(x[2]),int(x[3][0:2]), int(x[3][2:4])))

rawdata_filetime.sort(reverse = True)

try:
    datetime_last = rawdata_filetime[1]  # 上周采集数据的时间
    datetime_this = rawdata_filetime[0] # 本周采集数据的时间
    print("经过程序检测，raw_data文件夹下最后的两个数据收集时间分别为 %s 和 %s, 将分别其作为上周和本周的数据采集时间。如果有误，请重新检查raw_data文件夹中的文件名"
          % (datetime_last.strftime(file_time_format), datetime_this.strftime(file_time_format)))
except:
    print("程序无法自动检测上周和本周的数据采集时间。请重新检查raw_data文件夹中的文件名，确保其为nga-thread-YYYY-mm-dd-HHMM.xlsx的格式")
    exit()

print("步骤一：读入数据文件（可能耗时较长，请耐心等待）")
data_last = load_data(f'raw_data/nga-thread-{datetime_last.strftime(file_time_format)}.xlsx')
data_this = load_data(f'raw_data/nga-thread-{datetime_this.strftime(file_time_format)}.xlsx')

print("步骤二：数据文件预处理")
with pd.ExcelWriter(f'data/data-{datetime_last.strftime(file_time_format)}.xlsx', engine='openpyxl') as writer:
    to_excel_auto_column_weight(data_last, writer, f'data')

with pd.ExcelWriter(f'data/data-{datetime_this.strftime(file_time_format)}.xlsx', engine='openpyxl') as writer:
    to_excel_auto_column_weight(data_this, writer, f'data')

data_last = filter_anko_threads(data_last)
data_this = filter_anko_threads(data_this)

print("步骤三：计算数据 - 新增安科")
# 本周新增安科
new_threads = data_this[(datetime_last <= data_this['publish_time']) & (data_this['publish_time'] <= datetime_this)]
new_threads.fillna(0)

# 总数
thread_sum = len(data_this)

print("步骤四：计算数据 - 完结安科")
finished_threads = filter_finished_threads(data_last, data_this).sort_values(by='level', ascending=True)

print("步骤五：计算数据 - 活跃安科")
active_data = data_this['level'] - data_last['level']

active_data = active_data[active_data.notna() & active_data != 0.0]

print("步骤六：输出结果")
output = f"""[align=center][size=200%][b]周报基础内容[/b][/size][/align]
[quote][collapse=相关说明][list]
[*]下文的“本周”所指代的时间段为：{datetime_last} ~ {datetime_this}
[*]没有[安价/安科]、[安科/安价]、[安科]、[安价]等tag的帖子，会筛掉，可以在周报楼后面自行补充（然后自行改好tag不然下次还是一样）。
[*]在数据采集时间点附近进入审核的帖子，有可能被遗漏，可以在周报楼后面自行补充。
[*]方括号内的数字代表采集数据时帖子的楼层数。
[*]带有[color=green][b][新][/b][/color]标签的安科是在本周新发布的安科。
[*]带有[color=orangered][b][活动][/b][/color]标签的安科是参与活动的安科。可以是自行举办的活动，跟我说就可以加上活动标签。
[*]带有[color=blue][b][复更][/b][/color]标签的安科是曾经断更但现在恢复更新的安科。
[/list]

“本周完结”的判断标准：上周未在完结区，而本周在完结区内。

筛选“本周达到里程碑的安科”使用的算法概述：
1. 去除“地下城”和“图书馆”分区的帖子，去除没有带安科相关标签的帖子
2. 列表A为本周的数据中大于等于x层的帖子，列表B为上周的数据中大于等于x层的帖子
3. 去除在本周之前已经达到x层的帖子，也就是获取二者差集，即 C = A - B。
4. 如果某个帖子出现在C中，但是不存在于B中，说明这个帖子在收集上周数据时进入了审核或者是这周新发布，如果是前者，则从结果中去除，避免出现“在本周之前达成x层的帖子进入本周达成列表”的情况 
5. 按照楼层升序排列
[/collapse][/quote]
"""
# 本周活跃数据
output += f"""[align=center][size=150%][b]本周版面活跃数据[/b][/size][/align]
[quote]本周有[b]{len(active_data)}[/b]篇旧安科处于活跃状态，在这周总共增长了{int(active_data.sum())}层，平均每一篇增长{round(active_data.mean(), 2)}层。其中楼层增长最多的安科增长了{int(active_data.max())}层，楼层增长最少的安科增长了{int(active_data.min())}层。

本周新增{len(new_threads)}篇安科，在这周总共增长了{int(new_threads['level'].sum())}层，平均每一篇增长{round(new_threads['level'].mean(), 2)}层。其中楼层增长最多的安科增长了{int(new_threads['level'].max())}层，楼层增长最少的安科增长了{int(new_threads['level'].min())}层。[/quote]
"""

# 日更安科
output += f"""[align=center][size=150%][b]日更宣传栏[/b][/size][/align]
[collapse=说明][quote]本栏目用于鼓励大家多多更新，规则如下，因为是试运行，有可能会改动规则：：
1. 本栏目列出所有在标题标记了[b][color=tomato][日更][/color][/b]的tag的安科。如不再日更，请暂时撤下该tag；如打算开始日更，可以挂上该tag以在本栏目宣传。
2. 打上该tag需要满足以下条件才会被展示在这里：本周该贴每天发布的内容需要是请假声明或者正文更新，请假声明一周内不能超过两天，每日更新量不限
3. 精力有限，一般不会主动检查，但请不要滥用此tag。[color=red]如果不符合要求且一周不主动撤下tag，被周报读者指出后，会手动从本栏目展示区撤下[/color][/quote][/collapse]
[quote]{data_to_bbcode(get_daily_update_threads(data_this), show_reply_time=True, is_fold=False)}[/quote]
"""

# 复更安科
output += f"""[align=center][size=150%][b]复更宣传栏[/b][/size][/align]
[collapse=说明][quote]众所周知，写安科断更是非常常见的事情，甚至你只要达到50层还没有断更，就足以超过一半的安科作者。但也有一些安科作者在打败了现实恶魔之后，回来恢复更新，却因为断更太久而无人问津。

本栏目就是为了帮助这些恢复更新的安科作者找回以前的读者，进行一定程度的宣传。规则如下，因为是试运行，有可能会改动规则：

1.将会列出所有在标题标记了[b][color=blue][恢复更新][/color][/b]的tag[color=red]（注意不是[b][复更][/b]，下面标注的[b][复更][/b]是为了节约篇幅以更多展示你标题的）[/color]，且最后回复时间在30天以内的安科
2.精力有限，一般不会主动检查，但请不要滥用此tag。一般来说，挂了一个月已恢复稳定更新后，或者再次断更等情况，请自觉撤下该tag
[color=red][b]3.如果超过一个月太多不主动撤下，被周报读者指出后，会手动撤下（且将帖子加入本栏目黑名单，需要主动申请移出）。[/b][/color][/quote][/collapse]
[quote]{data_to_bbcode(get_revive_threads(data_this), show_reply_time=True, is_fold=False)}[/quote]
"""

# 活动安科
output += f"""[align=center][size=150%][b][color=orangered]活动宣传栏[/color][/b][/size][color=red][b][i]New！[/i][/b][/color]
详情见：[url=https://ngabbs.com/read.php?tid=40222462][活动专贴][第一期]向导游们发起安科更新挑战的邀请！！！[/url][/align]
[quote]{data_to_bbcode(get_activity_threads(data_this), show_reply_time=True, is_fold=False)}[/quote]
"""

# 本周完结的内容
output += f"""[align=center][size=150%][b]本周完结的安科[/b][/size][/align]
[quote]{data_to_bbcode(finished_threads, is_fold=False)}[/quote]
"""

# 本周达到里程碑的安科
output += f"""[align=center][size=150%][b]本周达到里程碑的安科[/b][/size]
[/align]
{set_milestones([25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000])}
"""

# 复制结果
# output = data_to_bbcode(get_activity_threads(data_this),show_reply_time=True,is_fold=False)
pyperclip.copy(output)
# print(output)

paper_file = open(f'paper_weekly-{datetime_last.strftime(file_time_format)}.txt', "w", encoding="utf-8")
paper_file.write(output)
paper_file.close()

active_data = active_data.sort_values(ascending=False)

# active_data += new_threads['level']

data_this['incLevel'] = active_data.astype(int)

data_this = data_this.fillna(0)

data_this = data_this.sort_values(by='incLevel',ascending=False)

hot_thread = data_this.head(10)

#print(data_to_bbcode(hot_thread,'incLevel'))


#  & ((datetime_this- data_this['reply_time']) <= pd.Timedelta(days=30)) # 最后回复时间小于等于30天

gt500 = data_this[(data_this['level'] >= 500)]
gt1000 = data_this[(data_this['level'] >= 1000)]
gt2500 = data_this[(data_this['level'] >= 2500)]
gt5000 = data_this[(data_this['level'] >= 5000)]
gt10000 = data_this[(data_this['level'] >= 10000)]
gt25000 = data_this[(data_this['level'] >= 25000)]
gt50000 = data_this[(data_this['level'] >= 50000)]

t500 = diff(gt500,gt1000).sort_values(by='level',ascending=True)
t1000 = diff(gt1000,gt2500).sort_values(by='level',ascending=True)
t2500 = diff(gt2500,gt5000).sort_values(by='level',ascending=True)
t5000 = diff(gt5000,gt10000).sort_values(by='level',ascending=True)
t10000 = diff(gt10000,gt25000).sort_values(by='level',ascending=True)
t25000 = diff(gt25000,gt50000).sort_values(by='level',ascending=True)
t50000 = gt50000

output = f"""[align=center][size=200%][b]高楼层里程碑[/b][/size][/align]"""

output += f"""
[align=center][size=150%]500层[/size]
[color=silver]11760/13002=[b]90.45%[/b][/color]
[/align]
[quote]{data_to_bbcode(t500,is_fold=True,is_order_list=True)}[/quote]
[align=center][size=150%]1000层[/size]
[color=silver]12338/13002=[b]94.89%[/b][/color]
[/align]
[quote]{data_to_bbcode(t1000,is_fold=True,is_order_list=True)}[/quote]
[align=center][size=150%]2500层[/size]
[color=silver]已超过12786/13002=[b]98.34%[/b]的安科[/color]
[/align]
[quote]{data_to_bbcode(t2500,is_fold=True,is_order_list=True)}[/quote]
[align=center][size=150%][color=purple][b]5000层(传说级)[/b][/color][/size]
[color=silver]已超过12905/13002=[b]99.25%[/b]的安科[/color]
[/align]
[quote]{data_to_bbcode(t5000,is_fold=True,is_order_list=True)}[/quote]
[align=center][size=150%]10000层[/size]
[color=silver]已超过12975/13002=[b]99.79%[/b]的安科[/color]
[/align]
[quote]{data_to_bbcode(t10000,is_fold=False,is_order_list=True)}[/quote]
[align=center][size=150%]25000层[/size]
[color=silver]已超过12996/13002=[b]99.95%[/b]的安科[/color]
[/align]
[quote]{data_to_bbcode(t25000,is_fold=False,is_order_list=True)}[/quote]
[align=center][size=150%][color=red][b]50000层(神话级)[/b][/color][/size]
[color=silver]已超过13001/13002=[b]99.99%[/b]的安科[/color]
[/align]
[quote]{data_to_bbcode(t50000,is_fold=False,is_order_list=True)}[/quote]
"""

# print(output)
# pyperclip.copy(output)
paper_file = open(f'paper_milestone-{datetime_last.strftime(file_time_format)}.txt', "w", encoding="utf-8")
paper_file.write(output)
paper_file.close()

past5000 = data_last[data_last['level'] >= 5000]
now5000 = data_this[data_this['level'] >= 5000]

new5000 = diff(now5000,past5000)
with pd.ExcelWriter(f'now5000.xlsx', engine='openpyxl') as writer:
    to_excel_auto_column_weight(now5000, writer, f'data')

print("本期周报已经处理完毕。周报内容保存于paper_weekly文件中，里程碑内容保存于paper_milestone文件中。")
input("请按任意键结束程序。")