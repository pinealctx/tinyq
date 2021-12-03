## hookq

hookq是生产者嵌入的消息队列，它以类似于MySQL这样的关系数据库为媒介来实现消息队列。
这样的好处是，不需要部署额外的中间件服务，生产者以引用库的方式来产生消息，消费者也只需要扫描数据库即可，实现很简单。
坏处是，只能有一个生产者，因为嵌入的方式会导致没有服务来维护消息队列的生产，只有一个生产者会让事情变得更简单，当然，很多场景下只有一个生产者是适用的。

### 消息队列的事务
独立的消息队列可以看着一种存储方式，如果业务场景中需要同时写入别的存储服务(例如关系数据库或别的NOSQL数据库)，将消息队列与业务需要的存储做成逻辑上的事务是很困难的，因为这涉及到两个异构系统。如果要做到这样的功能，需要在这些之外引入WAL机制。   
嵌入的方式可以将消息队列与业务的数据库弄在同一个数据源中，这样避免了引入WAL机制。但相应的，串行化会让效率降低。

### Producer具体实现与使用

Producer以嵌入的方式，使用Producer的模块直接调用Producer方法，投递消息的方式实质上就是写入一条记录到数据库中。调用模块需要实现接口：

 ```go
//Hooker -- write data with message hook
type Hooker interface {
    //Save : save function
    Save() func(*gorm.DB) error
    //Msg : generate msg
    Msg() interface{}
}
```

其中Save方法返回模块需要保存的业务数据方法，Msg方法返回模块需要投递的消息结构体。
例如，取消订单后，需要投递消息，则相关的代码可以如下：

```go

type OrderAction struct {
	ID int64 `msgpack:"id"`
	AT ActionType `msgpack:"at"`
}

type CancelOrderWrap CancelOrderReq

func (c *CacelOrderWrap) Save() func(*gorm.DB) error {
	return func(db *gorm.DB) error {
		return db.Table("order_table").Updates(map[string]interface{}{
		    "state": Cancelled,
		    "updated_at": time.Now(),
        })
    }
}

func (c *CancelOrderWrap) Msg() interface{} {
	return &OrderAction {
	    ID: c.ID,
	    AT: Cancelled,
    }
}

func cancelOrderLogic(req *CancelOrderReq) error {
	...
	var c = (*CancelOrderWrap)req
	err = producer.WriteHook(db, c)
	...
}
```

### Consumer具体实现与使用

Consumer基本原理是定时去数据库顺序查询，这些查询出来的数据实际上就是消息内容，在处理了消息内容后，根据需要提交已经消费的消息ID，下次再取消息的时候，并不会再取已经消费的消息。
每个Consumer都有对应的已经消费的ID记录，这个ID取决于Consumer的提交。事实上，如果Consumer提交的频率比较低，极有可能在重启后获取到已经消费的消息，这需要消费消息过程实现可重入的幂等设计。

Consumer的参数：
1. MySQL配置
2. ID--客户端ID
3. N -- 每次从数据库获取的消息数量
4. Tick -- 每次获取消息的间隔
5. Retry -- 重试次数
6. CatchN -- 当消费的消息ID与上次提交的已消费ID的差异大过此数量时，会提交当前已消费ID
7. CatchT -- 当前时间与上次提交的时间超过此数量时，如果有新的消费消息产生，会提交当前已消费ID

