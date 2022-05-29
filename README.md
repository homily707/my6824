# lab 1
## 基础任务
  - worker
    - 接受 map 和 reduce 任务
    - 执行任务
    - 反馈任务
  - master 
    1. 实现 map 任务下发 
    2. 所有map完成，下发reduce
    
## 实现细节
### 文件读写
ioutil.TempFile
### 并发控制
闲置worker的处理

### 测试
### 错误处理
