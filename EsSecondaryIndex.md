Cassandra二级索引插件EsSecondaryIndex使用说明
====================

## 安装

1. 将jar包放在目录lib下，

2. 修改`/conf/logback.xml`文件，加上`com.ericsson.godzilla`的log用于打印信息，

3. 在`/conf`目录下创建索引配置文件`es-index.properties`，添加第一条属性`unicast-hosts=http://eshost:9200`。即要连接的Elasticsearch http接口。

## 使用

1. 创建 keyspace

```cql
CREATE KEYSPACE genesys WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

2. 创建表

```cql
create table genesys.emails (
	id UUID PRIMARY KEY,
	subject text,
	body text,
	userid int
);
```

3. 创建cassandra二级索引

```cql
create custom index on genesys.emails() using 'com.ericsson.godzilla.cassandra.index.EsSecondaryIndex' with options = {'unicast-hosts': 'localhost:9200'};
```

4. 不需要自己建mapping的方式，依靠Elasticsearch动态mapping来插入数据

```cql
insert into genesys.emails (id, subject, body, userid) values (904b88b2-9c61-4539-952e-c179a3805b22, 'Hello world', 'Cassandra is great, but it''s even better with EsIndex and Elasticsearch', 42);
```

5. 按照Elasticsearch语法查询

```cql
select id, subject, body, userid from genesys.emails where expr(emails_idx, '{"query":{"wildcard":{"body":{"value": "cassan*"}}}}');
 id                                   | subject     | body                                                                    | userid | query
--------------------------------------+-------------+-------------------------------------------------------------------------+--------+-------
 904b88b2-9c61-4539-952e-c179a3805b22 | Hello world | Cassandra is great, but it's even better with EsIndex and Elasticsearch |     42 | 
 {
	"_index": "genesys_emails_index@",
	"_type": "emails",
	"_id": "904b88b2-9c61-4539-952e-c179a3805b22",
	"_score": 0.24257512,
	"_source": {
		"id": "904b88b2-9c61-4539-952e-c179a3805b22"
	},
	"took": 4,
	"timed_out": false,
	"_shards": {
		"total": 5,
		"successful": 5,
		"failed": 0
	},
	"hits": {
		"total": 1,
		"max_score": 0.24257512
	}
}

(1 rows)
```

6. 也可以自定义Elasticsearch的mapping，以修正Elasticsearch的数据类型，

```cql
CREATE CUSTOM INDEX ON genesys.emails() 
USING 'com.ericsson.godzilla.cassandra.index.EsSecondaryIndex'
WITH OPTIONS = {
    'unicast-hosts': 'localhost:9200',
	'json-schema-fields': 'userid,subject,body',
    'mapping-emails': '
        {
           "emails":{
              "date_detection":false,
              "numeric_detection":false,
              "properties":{
                 "id":{
                    "type":"keyword"
                 },
                 "userid":{
                    "type":"long"
                 },
                 "subject":{
                    "type":"text",
                    "fields":{
                       "keyword":{
                          "type":"keyword",
                          "ignore_above":256
                       }
                    },
					"index": "false"
                 },
                 "body":{
                    "type":"text"
                 },
                 "IndexationDate":{
                    "type":"date",
                    "format":"yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
                 },
                 "_cassandraTtl":{
                    "type":"long"
                 }
              }
           }
        }
    '};
```

7. query支持Elasticsearch 的DSL查询，

```cql
select id, subject, body, userid from genesys.emails where expr(emails_idx,'{"query":{"range":{"userid":{"gte":10,"lte":50}}}}');

@ Row 1
---------+-------------------------------------------------------------------------
 id      | 904b88b2-9c61-4539-952e-c179a3805b22
 subject | Hello world
 body    | Cassandra is great, but it's even better with EsIndex and Elasticsearch
 userid  | 42
```

8. 属性配置在`/conf/es-index.properties`文件中添加，例如添加一个自定义的Elasticsearch的mapping属性，

```properties
mapping-emails={"date_detection":false,"numeric_detection":false,"properties":{"id":{"type":"keyword"},"userid":{"type":"long"},"subject":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"body":{"type":"text"},"IndexationDate":{"type":"date","format":"yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"},"_cassandraTtl":{"type":"long"}}}
```

所有options配置都可以定义在`es-index.properties`中，这样就不需要再create custom index设置了。

9. 关于Cassandra的TTL，需要在选项中设置`force-delete=true`选项，Elasticsearch才自动删除。

10. 高亮显示文本日志信息，在环境变量或系统参数中加上`-Dgodzilla-es-show-values=true`。

11. 序列化字段。`'json-schema-fields':'subject, userid'`。使用逗号分别，不用考虑主键字段，全局唯一不可修改。


## 关于处理联合二级索引







