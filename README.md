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
