# Week 2 Homework
## 作业简介
编写MapReduce完成流量数据计算

## 实验思路
1. 实现一个Writable的类，用于把每行数据作为一个Bean，在map阶段后落盘并最终输出。   
2. 实现一个Mapper，在map函数中对每行数据处理并以key-value形式落盘
3. 实现一个Reducer，在reduce函数中对每个key的一系列value处理并最终输出
Writable类在src/main/java/Flow.java中展示
Mapper和Reducer在src/main/java/FlowCount.java中展示


## 实验结果
[实验结果](!Result.png)

## 遇到的问题和经验
随后补充