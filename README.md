# smss-client
golang版本的smss客户端，提供以下能力：

* 发送消息及其他操作
  * 发送消息
  * 发送延迟消息，在发送消息时指的延迟的时间，mq收到延迟消息后存储，待到期时自行发送消息
  * 创建topic，topic支持生命周期，创建是可以指定存活时长，到期后自动删除
  * 删除topic
  * 获取指定topic的信息
  * 获取所有topic的信息
  * 探活，判断当前连接或者mq服务端是否还存活
* 订阅消息
 

# 发送消息及其他操作

PubClient结构体提供所有的操作支持，构建PubClient对象有两种方法，

* 调用NewPubClient方法产出一个简单的、底层socket是非池化的对象
* 调用NewPubClientPool构建一个PubClient池，调用pool的Borrow方法获取池化的对象

生成环境中建议使用NewPubClientPool，简单的测试可以使用NewPubClient。

## 示例

## 构建PubClient对象
### 非池化

```go
    pc, err := client.NewPubClient("localhost", 12301, time.Second*50000)
    if err != nil {
        logger.Infof("%v\n", err)
        return
    }
    defer pc.Close()
	// 业务代码
```

### 池化

```go
    pcPool := client.NewPubClientPool(pool.NewDefaultConfig(), "localhost", 12301, time.Second*5)
    defer pcPool.ShutDown()
  
    pc, err := pcPool.Borrow()
    if err != nil {
        logger.Infof("%v\n", err)
        return
    }
    defer pc.Close()
	
	// 业务代码
```

## 发送消息
smss消息支持header，可以通过AddHeader方法来添加header

```go
    base := "ggppmm-hello world,haha."
	for i := 0; i < 100000; i++ {
		buf := []byte(base + strconv.Itoa(i))
		msg := client.NewMessage(buf)
		msg.AddHeader("traceId", fmt.Sprintf("tid-pub-%d", i))
		err = pc.Publish("order", msg, "tid-999pxxfdb11")
		if err != nil {
			logger.Infof("%v\n", err)
			break
		}
		if i%50 == 0 {
			logger.Infof("finish %d\n", i)
		}
	}
```

### 发送延迟消息

```go
    i := 11

	msg := client.NewMessage([]byte("delay-test 99999-" + strconv.Itoa(i)))
	msg.AddHeader("traceId", fmt.Sprintf("tid-%d", i))
	err = pc.PublishDelay("order", msg, 10*60*1000, "tid-777777")
	logger.Infof("%v\n", err)
```

## 创建永久topic

```go
err := pc.CreateTopic("order", 0, "tid-2209991")
```

### 创建有生命周期的topic

```go
    expireAt := time.Now().Add(time.Second * 120).UnixMilli()
	err = pc.CreateTopic("order", expireAt, "tid-2209991")
```

## 删除topic

```go
    err := pc.DeleteTopic("order", "tid-9999del33")
```

## 获取topic信息,返回json

```go
    var j string
	j, err = pc.GetTopicInfo(topicName, "tid-99yymm009")

	log.Println(j, err)

```

## 获取所有topic信息，返回json

```go
    var j string
	j, err = pc.GetTopicList("tid-99yymm009")
```

# 订阅

smss的订阅支持类似kafka的group订阅，每一个group只能有一个客户端实例来定义，在smss订阅时，可以指定who参数，代表谁来订阅，
smss 服务端内部会检测是否有两个相同的who来订阅同一个topic，如果有，第二个将被拒绝，但这是一种兜底方案，正常的处理方式是需要
smss客户端使用分布式锁来协调，保证只有一个who可以访问一个topic。

smss客户端支持两种订阅方式：
* 简单订阅，当业务上确认只有一个who订阅消息时可以使用这种模式，一般情况下，指的是只有可以客户端的实例被部署
* 支持分布式锁的订阅，客户端可以部署多个实例，通过分布式锁来协调只有一个实例可以订阅到消息

## 分布式锁订阅
### 分布式锁
DLock接口定义了分布式锁的描述，你可以自行实现，比如基于zookeeper、etcd或者redis实现自己的分布式锁。

#### redis分布式锁

smss-client实现了基于redis的分布式锁，dlock/redis下是对应的实现代码。redis的分布式锁高效、简单，因此被缺省实现，但redis锁也有缺点，
就是不能像zookeeper一样实时的监控锁的状态，redis锁要通过轮询的方式来监听锁的状态。

* 使用setnx+超时尝试获取锁，超时即获取锁的租约时间，key是who，value是一个uuid，代表当前实例id
  * 如果没有获取到锁，则以一个固定周期继续尝试获取到锁
* 获取到锁，则通知业务开启业务处理，同时指定一个固定的时间周期性的续约，这个周期要小于租约时间
  * 为了保证需要的准确性，在续约时需要携带uuid，需要判断当前value==uuid时才能需要，为了保证原子性，可以使用lua脚本
  * 如果使用了不支持lua的类redis，比如pika， 可以降低准确性，通过更短的续约周期来保证当前所是被当前实例持有的
* 如果续约失败，意味着锁丢失，那通知业务终止业务处理，并尝试再次获取锁
* 循环

### 示例

```go
    // 不支持lua
    locker := redisLock.NewRedisLock("localhost", 6379, true)
	lsub := client.NewDLockSub("order", who, "localhost", 12301, time.Second*5, locker, true)

	count := int64(0)

	err := lsub.Sub(eventId, 5, time.Second*10, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {

			var body string
			if len(msg.GetPayload()) > 500 {
				body = string(msg.GetPayload()[len(msg.GetPayload())-500:])
			} else {
				body = string(msg.GetPayload())
			}
			if count%50 == 0 {
				logger.Infof("ts=%d, eventId=%d, fileId=%d, pos=%d, body is: %s", msg.Ts, msg.EventId, msg.FileId, msg.Pos, body)
			}
			count++
		}
		return client.Ack
	})
	logger.Infof("dlockSub err:%v", err)

```

## 简单订阅

```go
    sc, err := client.NewSubClient("order", who, "localhost", 12301, time.Second*5)
	if err != nil {
		logger.Infof("%v\n", err)
		return
	}

	defer sc.Close()

	count := int64(0)
	// 311041
	err = sc.Sub(eventId, 5, time.Second*10, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {

			var body string
			if len(msg.GetPayload()) > 500 {
				body = string(msg.GetPayload()[len(msg.GetPayload())-500:])
			} else {
				body = string(msg.GetPayload())
			}
			logger.Infof("ts=%d, eventId=%d, fileId=%d, pos=%d, body is: %s\n", msg.Ts, msg.EventId, msg.FileId, msg.Pos, body)
			count++
		}
		return client.Ack
	})
	if err != nil {
		logger.Infof("%v\n", err)
		return
	}
```