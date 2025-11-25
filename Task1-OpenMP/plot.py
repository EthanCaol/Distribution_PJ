import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 设置英文字体和样式
plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial", "Helvetica"]
plt.rcParams["axes.unicode_minus"] = False


def plot_all_data_sizes():
    # 读取数据
    try:
        df = pd.read_csv("result.csv")
    except FileNotFoundError:
        print("Error: Results file 'result.csv' not found")
        print("Please run the C++ program first to generate test results")
        return

    # 获取所有数据量
    data_sizes = sorted(df["数据量"].unique())

    # 计算子图布局
    n_sizes = len(data_sizes)
    cols = min(3, n_sizes)
    rows = (n_sizes + cols - 1) // cols

    # 创建图表
    fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 5 * rows))
    fig.suptitle("Parallel QuickSort Performance Analysis by Data Size", fontsize=16, fontweight="bold")

    # 专业科技蓝配色方案
    speedup_color = "#2E86AB"  # 深蓝色
    ideal_color = "#A23B72"  # 深紫色
    efficiency_color = "gray"  # 橙色

    # 如果只有一个子图，axes不是数组
    if n_sizes == 1:
        axes = [axes]
    elif rows > 1 and cols > 1:
        axes = axes.flatten()

    # 为每个数据量创建子图
    for i, size in enumerate(data_sizes):
        ax = axes[i] if n_sizes > 1 else axes[0]
        subset = df[df["数据量"] == size]

        # 绘制加速比
        line1 = ax.plot(subset["线程数"], subset["加速比"], "o-", color=speedup_color, linewidth=2.5, markersize=8, markerfacecolor="white", markeredgecolor=speedup_color, markeredgewidth=2, label="Actual Speedup")

        # 绘制理想加速比
        line2 = ax.plot(subset["线程数"], subset["线程数"], "--", color=ideal_color, alpha=0.8, linewidth=2, label="Ideal Speedup")

        # 绘制效率
        ax2 = ax.twinx()
        bars = ax2.bar(subset["线程数"], subset["效率(%)"], alpha=0.5, color=efficiency_color, width=0.6, label="Efficiency")

        # 设置轴标签
        ax.set_xlabel("Thread Count", fontsize=12)
        ax.set_ylabel("Speedup Ratio", color=speedup_color, fontsize=12)
        ax2.set_ylabel("Parallel Efficiency (%)", color=efficiency_color, fontsize=12)

        # 设置轴颜色
        ax.tick_params(axis="y", labelcolor=speedup_color)
        ax2.tick_params(axis="y", labelcolor=efficiency_color)

        # 设置标题
        ax.set_title(f"Data Size: {size:,} elements", fontsize=14, pad=10)
        ax.set_xticks(subset["线程数"])
        ax.grid(True, alpha=0.2, linestyle="-")

        # 创建图例
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", framealpha=0.9)

    # 隐藏多余的子图
    for i in range(n_sizes, rows * cols):
        if rows * cols > 1:
            axes[i].set_visible(False)

    plt.tight_layout()
    filename = "result.png"
    plt.savefig(filename, dpi=300, bbox_inches="tight", facecolor="white")
    print(f"Chart saved as: {filename}")


if __name__ == "__main__":
    plot_all_data_sizes()
