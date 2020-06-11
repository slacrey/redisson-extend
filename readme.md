# Getting Started

### Reference Documentation
扩展了redisson 2.x版本 添加可计数布隆过滤器

### 引入依赖包
```xml
<dependency>
  <groupId>com.github.slacrey</groupId>
  <artifactId>redisson-extend</artifactId>
  <version>1.1.2150</version>
</dependency>
```

### 创建Redisson连接
```java
Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        RedissonClientExtend redissonClientExtend = RedissonExtend.create(config);
```

### 创建过滤器
```java
RCountingBloomFilter<String> bloomFilter = redissonClientExtend.getCountingBloomFilter("test2", 3);
```
getCountingBloomFilter(name, repeat)
- name 过滤器名称
- repeat 可重复次数

### 初始化过滤器
```java
bloomFilter.tryInit(10000, 0.0001D);

boolean tryInit(long expectedInsertions, double falseProbability);

```
- expectedInsertions 容量
- falseProbability 错误率

### 使用方式
```java
bloomFilter.add("test1");
bloomFilter.contains("test1")
bloomFilter.remove("test1")

```
