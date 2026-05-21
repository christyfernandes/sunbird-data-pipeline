package org.sunbird.dp.core.cache

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.job.BaseJobConfig
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class RedisConnect(redisHost: String, redisPort: Int, jobConfig: BaseJobConfig) extends java.io.Serializable {

  private val serialVersionUID = -396824011996012513L

  val config: Config = jobConfig.config
  private val logger = LoggerFactory.getLogger(classOf[RedisConnect])

  @transient private lazy val pool: JedisPool = {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(10)
    poolConfig.setMaxIdle(5)
    poolConfig.setMinIdle(1)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setBlockWhenExhausted(true)
    poolConfig.setMaxWaitMillis(5000)
    logger.info(s"Creating JedisPool for $redisHost:$redisPort (maxTotal=10)")
    new JedisPool(poolConfig, redisHost, redisPort, jobConfig.redisConnectionTimeout)
  }

  def getConnection(db: Int, backoffTimeInMillis: Long): Jedis = {
    if (backoffTimeInMillis > 0) try Thread.sleep(backoffTimeInMillis)
    catch { case e: InterruptedException => e.printStackTrace() }
    val jedis = pool.getResource
    jedis.select(db)
    jedis
  }

  def getConnection(db: Int): Jedis = {
    val jedis = pool.getResource
    jedis.select(db)
    jedis
  }

  def getConnection: Jedis = getConnection(db = 0)

  def closePool(): Unit = if (pool != null && !pool.isClosed) pool.close()
}
