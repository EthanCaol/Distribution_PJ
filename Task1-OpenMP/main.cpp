#include <iostream>
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>
#include <omp.h>
#include <stack>

class ImprovedParallelQuickSort {
private:
    static const int INSERTION_THRESHOLD = 50;
    static const int PARALLEL_THRESHOLD = 1000;

    template <typename T>
    int partition(std::vector<T>& arr, int left, int right)
    {
        // 更好的基准选择：三数取中
        int mid = left + (right - left) / 2;
        if (arr[mid] < arr[left])
            std::swap(arr[left], arr[mid]);
        if (arr[right] < arr[left])
            std::swap(arr[left], arr[right]);
        if (arr[right] < arr[mid])
            std::swap(arr[mid], arr[right]);

        T pivot = arr[mid];
        std::swap(arr[mid], arr[right - 1]);

        int i = left;
        int j = right - 1;

        while (true) {
            while (arr[++i] < pivot)
                ;
            while (j > left && arr[--j] > pivot)
                ;
            if (i >= j)
                break;
            std::swap(arr[i], arr[j]);
        }
        std::swap(arr[i], arr[right - 1]);
        return i;
    }

    template <typename T>
    void insertion_sort(std::vector<T>& arr, int left, int right)
    {
        for (int i = left + 1; i <= right; i++) {
            T key = arr[i];
            int j = i - 1;
            while (j >= left && arr[j] > key) {
                arr[j + 1] = arr[j];
                j--;
            }
            arr[j + 1] = key;
        }
    }

    template <typename T>
    void sequential_quick_sort(std::vector<T>& arr, int left, int right)
    {
        if (right - left <= INSERTION_THRESHOLD) {
            insertion_sort(arr, left, right);
            return;
        }

        int pivot_index = partition(arr, left, right);
        sequential_quick_sort(arr, left, pivot_index - 1);
        sequential_quick_sort(arr, pivot_index + 1, right);
    }

public:
    // 改进的并行快速排序
    template <typename T>
    void parallel_sort(std::vector<T>& arr, int left, int right, int depth = 0)
    {
        int size = right - left + 1;

        // 小数组使用插入排序
        if (size <= INSERTION_THRESHOLD) {
            insertion_sort(arr, left, right);
            return;
        }

        // 中等数组使用顺序排序，避免并行开销
        if (size <= PARALLEL_THRESHOLD || depth > omp_get_max_threads()) {
            sequential_quick_sort(arr, left, right);
            return;
        }

        int pivot_index = partition(arr, left, right);

        // 根据子数组大小决定是否并行
        int left_size = pivot_index - left;
        int right_size = right - pivot_index;

        // 动态决定是否创建新线程
        if (left_size > PARALLEL_THRESHOLD && right_size > PARALLEL_THRESHOLD) {
#pragma omp task shared(arr) firstprivate(left, pivot_index, depth)
            {
                parallel_sort(arr, left, pivot_index - 1, depth + 1);
            }
#pragma omp task shared(arr) firstprivate(right, pivot_index, depth)
            {
                parallel_sort(arr, pivot_index + 1, right, depth + 1);
            }
#pragma omp taskwait
        } else {
            // 顺序执行以避免过多线程开销
            parallel_sort(arr, left, pivot_index - 1, depth + 1);
            parallel_sort(arr, pivot_index + 1, right, depth + 1);
        }
    }

    template <typename T>
    void parallel_sort(std::vector<T>& arr)
    {
#pragma omp parallel
        {
#pragma omp single nowait
            {
                parallel_sort(arr, 0, arr.size() - 1, 0);
            }
        }
    }

    template <typename T>
    void sequential_sort(std::vector<T>& arr)
    {
        sequential_quick_sort(arr, 0, arr.size() - 1);
    }
};

// 优化的性能测试类
class BetterPerformanceTester {
private:
    std::vector<int> generate_random_data(int size)
    {
        std::vector<int> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(1, size * 10);

#pragma omp parallel for
        for (int i = 0; i < size; i++) {
            data[i] = dis(gen);
        }
        return data;
    }

    bool is_sorted(const std::vector<int>& arr)
    {
        for (size_t i = 1; i < arr.size(); i++) {
            if (arr[i] < arr[i - 1])
                return false;
        }
        return true;
    }

    double measure_time(std::vector<int>& data, bool parallel)
    {
        auto data_copy = data; // 创建副本

        auto start = std::chrono::high_resolution_clock::now();

        ImprovedParallelQuickSort sorter;
        if (parallel) {
            sorter.parallel_sort(data_copy);
        } else {
            sorter.sequential_sort(data_copy);
        }

        auto end = std::chrono::high_resolution_clock::now();

        if (!is_sorted(data_copy)) {
            throw std::runtime_error("排序结果不正确!");
        }

        return std::chrono::duration<double, std::milli>(end - start).count();
    }

public:
    void run_tests()
    {
        std::vector<int> test_sizes = { 5000, 10000, 100000, 500000, 1000000, 5000000 };
        std::vector<int> thread_counts = { 1, 2, 4, 8, 12 };

        std::cout << "改进的并行快速排序性能测试\n";
        std::cout << "=========================================\n";

        for (int size : test_sizes) {
            std::cout << "\n数据量: " << size << " 个元素\n";
            std::cout << "线程数 | 顺序时间(ms) | 并行时间(ms) | 加速比 | 效率\n";
            std::cout << "-------|-------------|-------------|--------|------\n";

            auto data = generate_random_data(size);
            double seq_time = 0;

            try {
                // 先测量顺序时间（多次测量取平均）
                int runs = (size < 10000) ? 5 : 3;
                for (int i = 0; i < runs; i++) {
                    seq_time += measure_time(data, false);
                }
                seq_time /= runs;

                for (int threads : thread_counts) {
                    omp_set_num_threads(threads);

                    double par_time = 0;
                    for (int i = 0; i < runs; i++) {
                        par_time += measure_time(data, true);
                    }
                    par_time /= runs;

                    double speedup = seq_time / par_time;
                    double efficiency = speedup / threads * 100;

                    printf(
                        "%6d | %11.3f | %11.3f | %6.2f | %5.1f%%\n", threads, seq_time, par_time, speedup, efficiency);
                }
            } catch (const std::exception& e) {
                std::cout << "错误: " << e.what() << std::endl;
            }
        }
    }
};

int main()
{
    BetterPerformanceTester tester;
    tester.run_tests();
    return 0;
}