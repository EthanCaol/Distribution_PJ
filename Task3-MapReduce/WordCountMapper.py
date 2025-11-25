#!/home/hadoop/miniconda3/bin/python3
import sys
import re


def main():
    # 读取标准输入
    for line in sys.stdin:
        # 移除首尾空白字符
        line = line.strip()
        # 使用正则表达式分割单词（处理标点符号）
        words = re.findall(r"\b\w+\b", line.lower())

        # 输出每个单词和计数1
        for word in words:
            if word:  # 确保不是空字符串
                print(f"{word}\t1")


if __name__ == "__main__":
    main()
