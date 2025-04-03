## apollo config C++(20) client

### apollo or nacos
- 两个最小化部署简单试用了一下
- 最终选择 apollo 主要有是：
  - 权限隔离：修改、提交的权限可以隔离开，实际应用中这点还挺重要的（nacos可能也有类似功能，暂时没有使用到）
  - 页面配置方式更简洁
  - 灰度的支持
  - 之前用过更熟悉

### client 实现

- **核心功能**
  - 配置拉取与缓存
  - 线程安全配置访问
  - HTTP 长轮询（long poll）监听变更
  - 多命名空间隔离管理
  - IP 感知路由支持(灰度)

- **其他**
  - 熔断器（circuit breaker）
  - `callback thread pool` TODO: 线程池已经实现，但是总觉得这里放一个线程池有点重，期望是做成切片的方式，由外部决定 callback 怎么处理，但是又会涉及另一个问题如果外部是阻塞的，那么代码里的那个锁会很长。
  - 而且这里涉及一次数据拷贝，暂时还没太想好怎么做才最好 （`trigger_callbacks` 函数调用 `cb(key, value);` 的地方）

- **过程中的问题记录**
  - `apollo` 服务端 status = 304 时没有 body，`cpp-httplib` 不设置 `response function` 的情况下会取不到 `304` 这个 status code，考虑了下还是需要就加上了
  - `apollo` 的返回 header 里没有 `content-length` .... 不太友好 `response_body` 的 `reserve` 没有意义了（还是保留在这里了，之后看下 apollo 实现看能不能改掉）
  - `cpp-httplib` 里的 `encode_url` 结果对 `apollo` 是不够的，所以有了这个函数：`strict_url_encode`
  - `apollo` 有更新再拉取的时候也是全量的，一度考虑做个比较器，比较后只 callback 增量给应用方....（暂时还没有实现，`namespace_data` 结构里保留了 notification_id、messages 信息，应该可以用这两个信息先对比下再看是否要遍历区分这个namespace里是否有更新）
  - `process_config_response` 的实现是直接替换了整个 `namespace_data`, 主要是省事，后面 `process_notifications` 会再把 `notificationId`, `messages` 信息更新掉，再次 `fetch_config` 时没有变更也会只得到 `304` 的 status code


### 依赖要求
- C++20 编译器
- [cpp-httplib](https://github.com/yhirose/cpp-httplib) (v0.12.1+)
- [nlohmann/json](https://github.com/nlohmann/json) (v3.11.2+)

### 基础使用
```cpp
void apollo_test() {
  using namespace apollo::client;

  // 创建客户端
  auto client = apollo_client::create("http://localhost:8080", "cats", "dev", "192.168.100.1");

  // 注册回调
  client->register_callback("application", "login.port", [](std::string_view key, std::string_view value) {
    std::cout << "application login.port updated to: " << value << "\n";
  });

  client->register_callback("application", "game.port", [](std::string_view key, std::string_view value) {
    std::cout << "application game.port changed to: " << value << "\n";
  });

  // 添加命名空间
  client->add_namespace("application");
  client->add_namespace("cats.tables");

  // 启动客户端
  client->start();

  // 主循环
  while (true) {
    // if (auto val = client.get_value("database", "timeout")) {
    //   std::cout << "Current timeout: " << *val << "\n";
    // }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}
```

### 其他

#### 熔断器
- 这个熔断器比较简单，失败超过一定次数就以幂值延长等待时间

```cpp
util::circuit_breaker::config cb_cfg{
  .max_failures_before_cooldown = 10,
  .get_delay = util::circuit_breaker::config::create_exponential_backoff(500, 1.5),
  .cooldown_duration = std::chrono::seconds(45)
};

client->set_breaker_config(cb_cfg); // 自定义熔断策略
```

#### 线程池
- 相比传统线程池，增加了按照 hash_key 固定任务到某个线程
- 主要也是想给游戏服务端的存储DB使用，玩家ID作为 hash_key，这样单个玩家的数据库操作是在一个线程有序执行的
- （也可能是这个线程池设计如此的关系，觉得直接用在 apollo client 里有点重度

```cpp
util::thread_pool pool(8);

pool.submit("user_123", []{
    // 保证相同路由键的任务顺序执行
});
```

