package org.sunbird.dp.core.cache

import com.google.common.base.Charsets
import com.google.common.hash.{BloomFilter, Funnels}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisException


class DedupEngine(redisConnect: RedisConnect, store: Int, expirySeconds: Int) extends Serializable {

  private val serialVersionUID = 6089562751616425354L
  private[this] var redisConnection: Jedis = redisConnect.getConnection
  redisConnection.select(store)

  // Per-instance bloom filter: 10M entries, 0.01% false positive rate (~24MB).
  // Definitely-unique events skip the Redis EXISTS call entirely.
  // Cross-instance duplicates (different TM slots) still hit Redis — correct behavior.
  @transient private lazy val bloomFilter: BloomFilter[CharSequence] =
    BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 10000000, 0.0001)

  @throws[JedisException]
  def isUniqueEvent(checksum: String): Boolean = {
    if (!bloomFilter.mightContain(checksum)) return true
    var unique = false
    try {
      unique = !redisConnection.exists(checksum)
    } catch {
      case ex: JedisException =>
        ex.printStackTrace()
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(this.store, backoffTimeInMillis = 10000)
        unique = !this.redisConnection.exists(checksum)
    }
    unique
  }

  @throws[JedisException]
  def storeChecksum(checksum: String): Unit = {
    bloomFilter.put(checksum)
    try
      redisConnection.setex(checksum, expirySeconds, "")
    catch {
      case ex: JedisException =>
        ex.printStackTrace()
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(this.store, backoffTimeInMillis = 10000)
        this.redisConnection.select(this.store)
        this.redisConnection.setex(checksum, expirySeconds, "")
    }
  }

  def getRedisConnection: Jedis = redisConnection

  def closeConnectionPool(): Unit = {
    redisConnection.close()
  }
}
