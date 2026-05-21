package org.sunbird.dp.denorm.functions

import java.util
import java.util.Collections
import java.util.concurrent.{CompletableFuture, TimeUnit}

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.google.gson.Gson
import io.lettuce.core.{RedisClient, RedisURI}
import io.lettuce.core.api.async.RedisAsyncCommands
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.domain.EventsPath
import org.sunbird.dp.core.job.{JobMetrics, Metrics}
import org.sunbird.dp.denorm.`type`._
import org.sunbird.dp.denorm.domain.Event
import org.sunbird.dp.denorm.task.DenormalizationConfig
import org.sunbird.dp.denorm.util.CacheResponseData

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}

/**
 * Async de-normalization function: replaces the blocking window+pipeline approach.
 *
 * Each event issues non-blocking Lettuce Redis commands. While the network I/O
 * is in-flight the operator thread is free to accept new events. Up to `capacity`
 * events are in-flight simultaneously.
 *
 * Caffeine local caches (50k entries, 5-min TTL) short-circuit the Redis calls for
 * repeated device/user/content IDs — typical cache-hit rate is 50-70%.
 *
 * P2.4 skip: LOG, ERROR, AUDIT, INTERRUPT events are forwarded immediately without
 * any Redis lookup, saving ~20-30% of denorm Redis calls.
 */
class DenormalizationAsyncFunction(config: DenormalizationConfig)(implicit val eventTypeInfo: TypeInformation[Event])
  extends RichAsyncFunction[Event, Event] with JobMetrics {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizationAsyncFunction])

  // Lettuce async command handles — one per Redis instance (each pre-selected to its DB)
  @transient private var contentAsync: RedisAsyncCommands[String, String]  = _
  @transient private var deviceAsync: RedisAsyncCommands[String, String]   = _
  @transient private var userAsync: RedisAsyncCommands[String, String]     = _
  @transient private var dialcodeAsync: RedisAsyncCommands[String, String] = _

  // In-process caches to short-circuit Redis on repeated IDs
  @transient private var contentCache: Cache[String, MMap[String, AnyRef]]  = _
  @transient private var deviceCache: Cache[String, MMap[String, AnyRef]]   = _
  @transient private var userCache: Cache[String, MMap[String, AnyRef]]     = _
  @transient private var dialcodeCache: Cache[String, MMap[String, AnyRef]] = _

  @transient private var deviceDenorm: DeviceDenormalization     = _
  @transient private var userDenorm: UserDenormalization         = _
  @transient private var dialcodeDenorm: DialcodeDenormalization = _
  @transient private var contentDenorm: ContentDenormalization   = _
  @transient private var locationDenorm: LocationDenormalization = _
  @transient private var metrics: Metrics = _
  @transient private var gson: Gson = _

  // Event types that carry no enrichable data — skip all Redis lookups
  private val SKIP_ENRICHMENT = Set("LOG", "ERROR", "AUDIT", "INTERRUPT")

  def metricsList(): List[String] = List(
    config.eventsExpired, config.userTotal, config.userCacheHit, config.userCacheMiss,
    config.contentTotal, config.contentCacheHit, config.contentCacheMiss,
    config.deviceTotal, config.deviceCacheHit, config.deviceCacheMiss,
    config.dialcodeTotal, config.dialcodeCacheHit, config.dialcodeCacheMiss,
    config.locTotal, config.locCacheHit, config.locCacheMiss, config.eventsSkipped
  )

  override def open(parameters: Configuration): Unit = {
    gson    = new Gson()
    metrics = registerMetrics(metricsList())
    metricsList().foreach { m =>
      getRuntimeContext.getMetricGroup.addGroup(config.jobName)
        .gauge[Long, ScalaGauge[Long]](m, ScalaGauge[Long](() => metrics.getAndReset(m)))
    }

    def lettuceAsync(host: String, port: Int, db: Int): RedisAsyncCommands[String, String] =
      RedisClient.create(RedisURI.builder().withHost(host).withPort(port).withDatabase(db).build())
        .connect().async()

    contentAsync  = lettuceAsync(config.contentRedisHost,  config.contentRedisPort,  config.contentStore)
    deviceAsync   = lettuceAsync(config.deviceRedisHost,   config.deviceRedisPort,   config.deviceStore)
    userAsync     = lettuceAsync(config.userRedisHost,     config.userRedisPort,     config.userStore)
    dialcodeAsync = lettuceAsync(config.dialcodeRedisHost, config.dialcodeRedisPort, config.dialcodeStore)

    def caffeineCache(): Cache[String, MMap[String, AnyRef]] =
      Caffeine.newBuilder()
        .maximumSize(50000L)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build[String, MMap[String, AnyRef]]()

    contentCache  = caffeineCache()
    deviceCache   = caffeineCache()
    userCache     = caffeineCache()
    dialcodeCache = caffeineCache()

    deviceDenorm   = new DeviceDenormalization(config)
    userDenorm     = new UserDenormalization(config)
    dialcodeDenorm = new DialcodeDenormalization(config)
    contentDenorm  = new ContentDenormalization(config)
    locationDenorm = new LocationDenormalization(config)
  }

  override def asyncInvoke(event: Event, resultFuture: ResultFuture[Event]): Unit = {
    if (event.isOlder(config.ignorePeriodInMonths)) {
      metrics.incCounter(config.eventsExpired)
      resultFuture.complete(Collections.emptyList())
      return
    }

    val summaryList = List("ME_WORKFLOW_SUMMARY", "SUMMARY")
    if (!(summaryList.contains(event.eid()) ||
        !(event.eid().contains("SUMMARY") || config.eventsToskip.contains(event.eid())))) {
      metrics.incCounter(config.eventsSkipped)
      resultFuture.complete(Collections.emptyList())
      return
    }

    // P2.4: skip all Redis lookups for event types that yield no enrichable fields
    if (SKIP_ENRICHMENT.contains(event.eid().toUpperCase)) {
      resultFuture.complete(Collections.singleton(event))
      return
    }

    asyncFetch(event, resultFuture)
  }

  private def asyncFetch(event: Event, resultFuture: ResultFuture[Event]): Unit = {
    val objectType = event.objectType()
    val objectId   = event.objectID()
    val did        = event.did()
    val actorId    = event.actorId()
    val actorType  = event.actorType()

    val needsContent  = event.isValidEventForContentDenorm(config, objectId, objectType, event.eid())
    val needsDialcode = null != objectType && List("dialcode", "qr").contains(objectType.toLowerCase())
    val needsDevice   = null != did && did.nonEmpty
    val needsUser     = null != actorId && actorId.nonEmpty &&
      !"anonymous".equalsIgnoreCase(actorId) &&
      ("user".equalsIgnoreCase(Option(actorType).getOrElse("")) || "ME_WORKFLOW_SUMMARY" == event.eid())

    val empty: CompletableFuture[MMap[String, AnyRef]] =
      CompletableFuture.completedFuture(MMap[String, AnyRef]())

    // Fetches a string-valued Redis key (JSON), checks Caffeine first
    def fetchJson(key: String, cmds: RedisAsyncCommands[String, String],
                  cache: Cache[String, MMap[String, AnyRef]], fields: List[String]): CompletableFuture[MMap[String, AnyRef]] = {
      val hit = cache.getIfPresent(key)
      if (hit != null) CompletableFuture.completedFuture(hit)
      else cmds.get(key).toCompletableFuture.thenApply[MMap[String, AnyRef]] { raw =>
        val m = parseJson(raw, fields)
        if (m.nonEmpty) cache.put(key, m)
        m
      }
    }

    // Fetches a hash Redis key (HGETALL), checks Caffeine first
    def fetchHash(key: String, cmds: RedisAsyncCommands[String, String],
                  cache: Cache[String, MMap[String, AnyRef]], fields: List[String]): CompletableFuture[MMap[String, AnyRef]] = {
      val hit = cache.getIfPresent(key)
      if (hit != null) CompletableFuture.completedFuture(hit)
      else cmds.hgetAll(key).toCompletableFuture.thenApply[MMap[String, AnyRef]] { raw =>
        val m = parseHash(raw, fields)
        if (m.nonEmpty) cache.put(key, m)
        m
      }
    }

    val contentF = if (needsContent) fetchJson(objectId, contentAsync, contentCache, config.contentFields) else empty
    val collId   = if (needsContent && event.checkObjectIdNotEqualsRollUpId(EventsPath.OBJECT_ROLLUP_L1))
                     Option(event.objectRollUpl1ID()) else None
    val collF    = collId.fold(empty)(id => fetchJson(id, contentAsync, contentCache, config.contentFields))
    val l2Id     = if (needsContent && event.checkObjectIdNotEqualsRollUpId(EventsPath.OBJECT_ROLLUP_L2))
                     Option(event.objectRollUpl2ID()) else None
    val l2F      = l2Id.fold(empty)(id => fetchJson(id, contentAsync, contentCache, config.contentFields))
    val deviceF  = if (needsDevice) fetchHash(did, deviceAsync, deviceCache, config.deviceFields) else empty
    val userKey  = config.userStoreKeyPrefix + actorId
    val userF    = if (needsUser) fetchHash(userKey, userAsync, userCache, config.userFields) else empty
    val dialKey  = if (needsDialcode) objectId.toUpperCase() else ""
    val dialF    = if (needsDialcode) fetchJson(dialKey, dialcodeAsync, dialcodeCache, config.dialcodeFields) else empty

    CompletableFuture.allOf(contentF, collF, l2F, deviceF, userF, dialF)
      .thenAccept { _ =>
        try {
          val cacheData = CacheResponseData(
            content    = contentF.get(),
            collection = collF.get(),
            l2data     = l2F.get(),
            device     = deviceF.get(),
            user       = userF.get(),
            dialCode   = dialF.get()
          )
          deviceDenorm.denormalize(event, cacheData, metrics)
          userDenorm.denormalize(event, cacheData, metrics)
          dialcodeDenorm.denormalize(event, cacheData, metrics)
          contentDenorm.denormalize(event, cacheData, metrics)
          locationDenorm.denormalize(event, metrics)
          resultFuture.complete(Collections.singleton(event))
        } catch {
          case e: Exception =>
            logger.error(s"Denorm async error for mid=${event.mid()}", e)
            resultFuture.completeExceptionally(e)
        }
      }
  }

  private def parseJson(raw: String, fields: List[String]): MMap[String, AnyRef] = {
    if (raw == null || raw.isEmpty) return MMap[String, AnyRef]()
    val m = gson.fromJson(raw, classOf[java.util.HashMap[String, AnyRef]])
    if (fields.nonEmpty) m.keySet().retainAll(fields.asJava)
    m.values().removeAll(Collections.singleton(""))
    m.asScala
  }

  private def parseHash(raw: java.util.Map[String, String], fields: List[String]): MMap[String, AnyRef] = {
    if (raw == null || raw.isEmpty) return MMap[String, AnyRef]()
    if (fields.nonEmpty) raw.keySet().retainAll(fields.asJava)
    raw.values().removeAll(Collections.singleton(""))
    toComplexTypes(raw.asScala)
  }

  private def toComplexTypes(data: scala.collection.mutable.Map[String, String]): MMap[String, AnyRef] = {
    val result = MMap[String, AnyRef]()
    data.foreach { case (k, v) =>
      val t = v.trim
      result += k -> (
        if (t.startsWith("["))      gson.fromJson(t, classOf[java.util.ArrayList[AnyRef]])
        else if (t.startsWith("{")) gson.fromJson(t, classOf[java.util.HashMap[String, AnyRef]])
        else                        t.replace("\\", "")
      )
    }
    result
  }
}
