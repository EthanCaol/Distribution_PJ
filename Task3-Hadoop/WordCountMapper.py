#!/usr/bin/env python3

import sys
import re

def read_input(file):
    for line in file:
        # 分割每行成单词
        yield line.split()

def main():
    # 从标准输入读取数据
    data = read_input(sys.stdin)
    
    for words in data:
        # 处理每个单词
        for word in words:
            # 清洗单词：转小写，去除标点
            clean_word = re.sub(r'[^a-zA-Z0-9]', '', word.lower())
            if clean_word:  # 只处理非空单词
                # 输出: 单词\t1
                print(f"{clean_word}\t1")

if __name__ == "__main__":
    main()