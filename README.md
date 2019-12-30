# top100url

## 思路

#### Map阶段

1. 使用多线程将一个大文件划分到固定大小的块处理，分别统计url频率。如图，每个线程拥有一个文件指针的偏移量，从而在不同位置分别读取url。

   ![div](img/div.png)

2. 使用STL中的hashmap分别统计每一块的url频率，计算url的hash值，根据不同的hash值将“(url-count)对”写入不同对应的文件。

#### Reduce阶段

1. 遍历文件，建立容量为100的最大堆。若新文件中有比堆中更高频的url，使用其替换掉堆中频率最小的节点。

2. 输入最大堆中的答案。

## 实验

由于机器限制，没有对100GB的文件进行测试。最大测试文件为20GB。实验中内存没有超过1GB的限制，并得到了正确答案。

### 数据

脚本为"genURL.py", 根据url的正则表达式生成url。

### 内存使用情况

由于map阶段限制了处理文件块的大小，reduce阶段只只用了hashmap和容量为100的最大堆，内存用量得到了控制。经过实验，内存使用量不超过800MB。
