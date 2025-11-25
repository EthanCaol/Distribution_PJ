#!/home/hadoop/miniconda3/bin/python3
import os
import time
import subprocess
import matplotlib.pyplot as plt

# 切换到脚本所在目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def run_hadoop_wordcount(input_path, output_path):
    """运行Hadoop WordCount任务"""
    # 删除已存在的输出目录
    subprocess.run(["hdfs", "dfs", "-rm", "-r", output_path], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    # 运行Hadoop Streaming任务
    start_time = time.time()

    cmd = [
        "hadoop",
        "jar",
        "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar",
        "-files",
        "WordCountMapper.py,WordCountReducer.py",
        "-mapper",
        "python3 WordCountMapper.py",
        "-reducer",
        "python3 WordCountReducer.py",
        "-input",
        input_path,
        "-output",
        output_path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    end_time = time.time()

    if result.returncode != 0:
        print(f"Error running Hadoop job: {result.stderr}")
        return None

    return end_time - start_time


def run_local_wordcount(input_file):
    """在本地运行WordCount（单机版）用于对比"""
    start_time = time.time()

    # 使用系统的sort和uniq命令模拟MapReduce
    cmd1 = f"python3 WordCountMapper.py < {input_file}"
    cmd2 = "sort"
    cmd3 = "python3 WordCountReducer.py"

    full_cmd = f"{cmd1} | {cmd2} | {cmd3} > /tmp/local_result.txt"
    result = subprocess.run(full_cmd, shell=True, capture_output=True)

    end_time = time.time()

    if result.returncode != 0:
        print(f"Error running local WordCount: {result.stderr}")
        return None

    return end_time - start_time


def main():
    # 测试文件列表
    test_files = [("test_1k.txt", "1KB"), ("test_1m.txt", "1MB"), ("test_10m.txt", "10MB"), ("test_100m.txt", "100MB")]

    # 生成测试数据
    print("Generating test data...")
    subprocess.run(["python3", "data.py"])

    # 上传测试文件到HDFS
    print("Uploading test files to HDFS...")
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/user/hadoop/input"])

    for filename, _ in test_files:
        if os.path.exists(filename):
            subprocess.run(["hdfs", "dfs", "-put", "-f", filename, f"/user/hadoop/input/{filename}"])
            print(f"Uploaded {filename} to HDFS")

    # 运行测试
    results = []

    for filename, size_label in test_files:
        print(f"\nTesting with {size_label} data...")

        # Hadoop运行时间
        input_path = f"/user/hadoop/input/{filename}"
        output_path = f"/user/hadoop/output_{filename.replace('.txt', '')}"

        hadoop_time = run_hadoop_wordcount(input_path, output_path)

        # 本地运行时间
        local_time = None
        if filename in [file[0] for file in test_files]:
            local_time = run_local_wordcount(filename)

        if hadoop_time:
            results.append({"size": size_label, "hadoop_time": hadoop_time, "local_time": local_time, "speedup": local_time / hadoop_time if local_time else None})

            print(f"Hadoop time: {hadoop_time:.2f} seconds")
            if local_time:
                print(f"Local time: {local_time:.2f} seconds")
                print(f"Speedup: {local_time/hadoop_time:.2f}x")

    # 显示结果表格
    print("\n" + "=" * 70)
    print("WordCount性能测试结果")
    print("=" * 70)
    print(f"{'Size':<10}\t{'Hadoop(s)':<15}\t{'Local(s)':<15}\t{'加速比':<10}")
    print("-" * 70)

    for result in results:
        local_time_str = f"{result['local_time']:.2f}" if result["local_time"] else "N/A"
        speedup_str = f"{result['speedup']:.2f}x" if result["speedup"] else "N/A"
        print(f"{result['size']:<10}\t{result['hadoop_time']:<15.2f}\t{local_time_str:<15}\t{speedup_str:<10}")

    # 绘制性能图表
    plot_results(results)


def plot_results(results):
    """绘制性能对比图表"""
    sizes = [result["size"] for result in results]
    hadoop_times = [result["hadoop_time"] for result in results]

    plt.figure(figsize=(12, 5))

    # 子图1: 执行时间对比
    plt.subplot(1, 2, 1)
    # 颜色模式
    plt.plot(sizes, hadoop_times, color="#4ECDC4", marker="o", linestyle="-", label="Hadoop", linewidth=2, markersize=8)

    # 只绘制有本地时间的数据点
    local_sizes = [result["size"] for result in results if result["local_time"]]
    local_times = [result["local_time"] for result in results if result["local_time"]]
    if local_times:
        plt.plot(local_sizes, local_times, color="#45B7D1", marker="o", linestyle="-", label="Local", linewidth=2, markersize=8)

    plt.xlabel("Data Size")
    plt.ylabel("Execution Time (seconds)")
    plt.title("WordCount Performance Comparison")
    plt.legend()
    plt.grid(True, alpha=0.3)

    # 设置y轴为对数刻度，便于观察大范围的时间差异
    plt.yscale("log")

    # 子图2: 加速比
    plt.subplot(1, 2, 2)
    speedup_sizes = [result["size"] for result in results if result["speedup"]]
    speedups = [result["speedup"] for result in results if result["speedup"]]

    if speedups:
        bars = plt.bar(speedup_sizes, speedups, color="green", alpha=0.7)
        plt.xlabel("Data Size")
        plt.ylabel("Speedup Ratio")
        plt.title("Hadoop Speedup Performance")

        # 在柱状图上添加数值标签
        for bar, speedup in zip(bars, speedups):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2.0, height + 0.1, f"{speedup:.1f}x", ha="center", va="bottom")

        # 添加参考线y=1（无加速）
        plt.axhline(y=1, color="red", linestyle="--", alpha=0.5, label="No Speedup")
        plt.legend()

    plt.tight_layout()
    plt.savefig("wordcount_performance.png", dpi=300, bbox_inches="tight")
    print("\n性能图表已保存为: wordcount_performance.png")


if __name__ == "__main__":
    main()
