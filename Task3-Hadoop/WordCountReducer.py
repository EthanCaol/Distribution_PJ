#!/usr/bin/env python3

import sys

def main():
    current_word = None
    current_count = 0
    word = None
    
    # 从标准输入读取mapper的输出
    for line in sys.stdin:
        # 去除首尾空格
        line = line.strip()
        
        # 解析mapper的输出：单词\t数量
        parts = line.split('\t')
        if len(parts) != 2:
            continue  # 跳过格式错误的行
            
        word, count = parts[0], parts[1]
        
        try:
            count = int(count)
        except ValueError:
            continue  # 跳过计数不是数字的行
        
        # Hadoop Streaming保证相同单词的数据是连续的
        if current_word == word:
            current_count += count
        else:
            # 遇到新单词，输出上一个单词的统计结果
            if current_word:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count
    
    # 输出最后一个单词的统计结果
    if current_word:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    main()