安科周报编写工具
原版 by ChangingSelf 憧憬少：https://github.com/ChangingSelf/anko-thread-weekly
在原版基础上进行了便利化修改：
- 自动检查并生成所需要的文件夹。
- 自动检测本周和上周的数据文件。
- 运行时提示当前进度。
- 周报内容保存文本版本。

同时将运行环境打包为exe文件，方便对Python不熟悉的朋友下载使用。

使用方法（EXE版）：
- 下载EXE版到本地并解压。
- 在解压后的raw_data文件夹中，已经保留了两个精简版文件供测试用。可以进一步获取新的数据文件，并以同样的格式命名后加入raw_data文件夹中。
- 程序会从raw_data的文件夹中根据文件名获得时间最近的两个文件作为“本周”和“上周”的数据文件读取并处理。
- 处理完成后的周报内容和里程碑内容将保存在以paper_weekly和paper_milestone开头，包含“本周”时间的文本文件中，其中的代码可以直接在论坛发布。

另外在Optional_dataset文件夹中提供了一个完整版的数据文件供参考（程序运行时仍然只会从raw_data文件夹中读取文件）。

2024/6/15 更新
- 现在程序可以同时使用原始数据和处理后的数据作为数据输入了。
- 增加了数据检查环节，避免因为两周间采样范围不同或者部分帖子被锁隐导致周报出错问题。
- 修改了活跃安科的处理部分，使用两周数据以tid配对的方式进一步避免因为两周间帖子排序不一致导致的周报出错问题。
- 现在程序将会把计算活跃安科和新增安科的中间结果输出到temp文件夹内供使用者参考（如果没有建立temp文件夹，程序会自动建立）。