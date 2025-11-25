#!/home/hadoop/miniconda3/bin/python3
import random
import os


def generate_word():
    """生成随机单词"""
    length = random.randint(3, 10)
    word = "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(length))
    return word


def generate_test_file(filename, size_mb):
    """生成指定大小的测试文件"""
    target_size = size_mb * 1024 * 1024  # 转换为字节
    words_per_line = random.randint(5, 15)

    with open(filename, "w") as f:
        current_size = 0
        while current_size < target_size:
            line_words = [generate_word() for _ in range(words_per_line)]
            line = " ".join(line_words) + "\n"
            f.write(line)
            current_size += len(line.encode("utf-8"))

    actual_size = float(os.path.getsize(filename))
    print(f"Generated {filename}: {actual_size / (1024 * 1024):.3f} MB")


if __name__ == "__main__":
    # 生成不同大小的测试文件
    sizes = [0.001, 1, 10, 100]  # 1KB, 1MB, 10MB, 100MB
    for size in sizes:
        if size == 0.001:  # 1KB
            filename = "test_1k.txt"
            generate_test_file(filename, 0.001)
        elif size == 1:
            filename = "test_1m.txt"
            generate_test_file(filename, 1)
        elif size == 10:
            filename = "test_10m.txt"
            generate_test_file(filename, 10)
        elif size == 100:
            filename = "test_100m.txt"
            generate_test_file(filename, 100)
