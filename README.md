<<<<<<< HEAD
![LDBC Logo](ldbc-logo.png)

# LDBC FinBench Driver
### AUTOMATIC_TEST
基于硬件设备预执⾏来进⾏初始参数预估，并通过⾃动调参实现符合要求的最佳性能。

在运行 src/main/java/org/ldbcouncil/finbench/driver/driver/Driver.java 时，需加上参数 -P src/main/resources/example/sf1_finbench_create_validation.properties

## 代码改动
在原[ldbc_finbench_driver](https://github.com/ldbc/ldbc_finbench_driver)代码基础上更改：
- 新建了一个 org.ldbcouncil.finbench.driver.driver.AutomaticTestMode类
- Driver 第86行左右新增 case
- OperationMode 新增枚举常量
- WorkloadResultsSnapshot 第 56行，将 throughput 的值赋给 AutomaticTestMode.throughput
- ConsoleAndFileDriverConfiguration 
  - 第 163 行，将 timeCompressionRatio 常量修改为 变量
- 第 171行，将 warmupCount常量修改为 变量
- 第174行开始，添加private final 常量：estimateTestTime，accurateTestTime， dichotomyErrorRange， tcrLeft，tcrRigh。以及他们的伴生常量、方法等
- WorkloadRunner
    - 第 71行左右，新增一个 getFuture(int milli)重载方法
    - 第132行左右，新增一个startThread(int milli)重载方法
    - 第 34 行，修改运行器轮询间隔为 500 毫秒
- ResultsLogValidator 第49、51行左右，添加map.get()为空的判断
- PoolingOperationHandlerRunnerFactory 第 123 行左右，添加了 shutdownTest() 方法，关闭 innerOperationHandlerRunnerFactory 和 operationHandlerRunnerPool，并且不用等待
- Db 第 75 行左右，添加了 reInitTest() 方法

### 新增配置参数

- **estimate**：快速预估阶段每次测试的时长，(默认是: 300000)，如果是-1，则完成 operation_count 数量的操作就结束
- **accurate**：精准调参阶段每次测试的时长，(默认是: 7200000)，如果是-1，则完成 operation_count 数量的操作就结束
- **error_range**：二分结束条件，容差范围，(默认是: 1E-5)
- **tcr_min**：时间压缩比的最小值限制，(默认是:1E-9)
- **tcr_max**：时间压缩比的最大值限制，(默认是: 1)

