# guidance

- Read this [guide](https://thesquareplanet.com/blog/students-guide-to-raft/) for Raft specific advice.
- Advice on [locking](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt) in labs.
- Advice on [structuring](https://pdos.csail.mit.edu/6.824/labs/raft-structure.txt) your Raft lab.
- This [Diagram of Raft interactions](https://pdos.csail.mit.edu/6.824/notes/raft_diagram.pdf) may help you understand code flow between different parts of the system.
- Dprint and colorful print
- debuging   笔记： debugging as a formal process

[debugging as a formal process](https://www.wolai.com/)

# lab 1
## 基础任务
- worker
    - 接受 map 和 reduce 任务
    - 执行任务
    - 反馈任务
- master
    1. 任务下发（并发）
    2. 任务完成标记
    3. 错误处理

## 实现细节
### 文件读写
ioutil.TempFile
### 并发控制
1. master切换状态的时候，worker正在读取该状态
2. chan 初始化后，不应该被指向新的chan
3. map中的job 下发时写时间，check中又在读时间
4. rwmap读锁锁定时，被赋予新的值
5. master状态切换的时候，又下发了两个任务。
### 测试
### 错误处理

# lab2 raft

![](https://secure2.wostatic.cn/static/vWgx6djPyQLMrihy5pjdJm/image.png)

[raft 论文](https://www.wolai.com/)

[advice](https://www.wolai.com/)

![](https://secure2.wostatic.cn/static/vPBXeHAMpie6U6A5LvYEef/election.png)

## guide

### livelocks

系统没有阻塞。但是状态来回转换，没有进展。比如多节点同时竞选。

当收到心跳、自己参与竞选、收到竞选申请时，三种情况下，都要重置election timeout 计时

### Incorrect RPC handler

- 如果reply false，快速结束，不要执行其余子过程
- entries为null的rpc也要处理
- 日志处理的rule5是必要的

### Failure to follow rule

- 确保apply只进行了一次
- 周期检查commit和apply或者每次commit时apply
- 如果append 由于term不一致被拒绝，不要更新nextIndex
- 不能更新以前term的commit

### Term confusion

- 当获得来自以前term的reply。如果term不同，不处理
-

## 2A：leader election

官方hints

- RequestVote   参加竞选  handler 参与投票。**5秒内选出新leader**
- AppendEntries 心跳。 **每秒不超过十次**

犯的几个错：

- 忘了维护votedFor的值
- 心跳的间隔时间要确保小于随机check时间
- 旧主以及竞选失败者 收到心跳时，没有退化
- 旧主disconnects，进入election，但是没有竞选
- leader 也给自己发了心跳
- 收到竞选，停止心跳检查，停止参加竞选
- 计票统计，没有并发控制
- 参与竞选，要并发执行

几个实现：

- log 标准化，每次打印当前 raft的状态的，并用emoji来区分
- log.SetFlags(log.Lmicroseconds)
## 2B：log

官方hints

- 实现 election restriction
- 可能会出现多主的bug，请查看timer的实现或者不要立刻发心跳
- 通过condition 或者 sleep，不要让检查状态的进程死循环

犯的错

- 锁重入会导致阻塞
- chan一定要初始化
- commit以后没有通知
- 有一个地方term 写成了index，我吐了。为什么要这么久才发现呢