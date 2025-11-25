#!/home/hadoop/miniconda3/bin/python3
import sys


def main():
    current_word = None
    current_count = 0

    for line in sys.stdin:
        # 移除首尾空白字符并分割
        line = line.strip()
        if not line:
            continue

        # 分割单词和计数
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        word, count = parts[0], parts[1]

        try:
            count = int(count)
        except ValueError:
            continue

        # 如果遇到新单词，输出上一个单词的统计结果
        if current_word == word:
            current_count += count
        else:
            if current_word:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    # 输出最后一个单词的统计结果
    if current_word:
        print(f"{current_word}\t{current_count}")


if __name__ == "__main__":
    main()
