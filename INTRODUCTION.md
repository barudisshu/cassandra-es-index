Elasticsearch index
========

# Background and requirements

Because Cassandra partitions data across multiple nodes, each node must maintain its own copy of a secondary index based on the data stored in partitions it owns. For this reason, queries involving a secondary index typically involve more nodes, making them significantly more expensive.

Secondary indexes are not recommended for several specific cases:

- Columns with high cardinality. For example, indexing on the user.addresses column could be very expensive, as the vast majority of addresses are unique.
- Columns with very low data cardinality. For example, it would make little sense to index on the user.title column in order to support a query for every "Mrs." in the user table, as this would result in a massive row in the index.
- Columns that are frequently updated or deleted. Indexes built on these columns can generate errors if the amount of deleted data (tombstones) builds up more quickly than the compacktion process can handle.

## Secondary Index Pitfalls

what the old documentation alludes to ( and what the new documentation explicitly mentions as an anti-pattern) is that there are performance impact implications when an index is built over a column with lots of indistinct values - such as in the user-by-email case described above. This stems from how Cassandra stores primary versus secondary indexes. [A primary index is global, whereas a secondary index is local.](http://www.datastax.com/documentation/cassandra/2.1/cassandra/planning/architecturePlanningAntiPatterns_c.html)

So, let's say you're running Cassandra on a ring of five machines, with a primary index of user IDs and a secondary index of user emails. If you were to query for a user by their ID - or by their primary indexed key - any machine in the ring would know which machine has a record of that user.. One query, one read from disk. However to query a user by their email - or their secondary indexed value - each machine has to query its own record of users. One query, five reads from disk. By either scaling the number of users system wide, or by scaling the number of machines in the ring, the noise to signal-to-ratio increases and the overall efficiency of reading drops - in some cases to the point of timing out on API calls.

## Usage(not implements!!!!!)

Package this maven project to your cassandra lib directory `/usr/share/cassandra/lib`, restart the cassandra service.

The Elastic index implementation exists alongside traditional secondary indexes, and you can create a Elastic index with the CQL CREATE CUSTOM INDEX command:

```cql
CREATE CUSTOM INDEX user_last_name_elastic_idx ON user (last_name) USING 'com.ericsson.godzilla.cassandra.index.ElasticIndex';
```

Elastic indexes do offer functionality beyond the traditional secondary index implementation, such as the ability to do inequality (greater than or less than) searches on indexed columns. You can also use the new CQL `LIKE` keyword to do text searches against indexed columns. For example, you could use the following query to find users whose last name begins with "N":

```cql
SELECT * FROM user WHERE last_name LIKE 'N%';
```

while Elastic indexes do perform better than traditional indexes by eliminating the need to read from additional tables, they require reads from a distribution elasticsearch node which supplied from `cassandra.yaml` configuration.


