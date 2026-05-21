package org.sunbird.dp.preprocessor.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.dp.extractor.functions.{DeduplicationFunction, ExtractionFunction, RedactorFunction}
import org.sunbird.dp.extractor.task.TelemetryExtractorConfig
import org.sunbird.dp.preprocessor.domain.Event
import org.sunbird.dp.preprocessor.functions.PipelinePreprocessorFunction

/**
 * Merged intake job: replaces the separate telemetry-extractor and pipeline-preprocessor jobs.
 *
 * Eliminates the telemetry.raw Kafka round-trip (~200k I/O ops/sec at 100k TPS).
 * telemetry.raw is still written as a shadow for SECOR/LPA archival (30-day drain period).
 *
 * Config uses nested namespaces (extractor.kafka.*, preprocessor.kafka.*) to avoid
 * key conflicts between the two configs (e.g. redis.database.duplicationstore.id differs).
 *
 * Chain: [telemetry.ingest] → Dedup → Extract → Redact ─┬─ shadow → [telemetry.raw]
 *                                                         └─ map to Event → Preprocess → [route topics]
 */
class TelemetryIntakeStreamTask(
  extractorConfig: TelemetryExtractorConfig,
  preprocessorConfig: PipelinePreprocessorConfig,
  kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -3827649182345612938L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(extractorConfig)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    // --- Extractor chain (identical to TelemetryExtractorStreamTask) ---
    val deDupStream =
      env.addSource(kafkaConnector.kafkaStringSource(extractorConfig.kafkaInputTopic), extractorConfig.telemetryExtractorConsumer)
        .uid(extractorConfig.telemetryExtractorConsumer)
        .setParallelism(extractorConfig.kafkaConsumerParallelism)
        .rebalance()
        .process(new DeduplicationFunction(extractorConfig))
        .name("ExtractorDeduplicationFn").uid("ExtractorDeduplicationFn")
        .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    val extractionStream =
      deDupStream.getSideOutput(extractorConfig.uniqueEventOutputTag)
        .process(new ExtractionFunction(extractorConfig))
        .name(extractorConfig.extractionFunction).uid(extractorConfig.extractionFunction)
        .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    val redactorStream =
      extractionStream.getSideOutput(extractorConfig.assessRedactEventsOutputTag)
        .process(new RedactorFunction(extractorConfig))
        .name(extractorConfig.redactorFunction).uid(extractorConfig.redactorFunction)
        .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    // Extractor side-output sinks (unchanged from standalone extractor job)
    deDupStream.getSideOutput(extractorConfig.duplicateEventOutputTag)
      .addSink(kafkaConnector.kafkaMapSink(extractorConfig.kafkaDuplicateTopic))
      .name(extractorConfig.extractorDuplicateProducer).uid(extractorConfig.extractorDuplicateProducer)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    deDupStream.getSideOutput(extractorConfig.failedBatchEventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(extractorConfig.kafkaBatchFailedTopic))
      .name(extractorConfig.extractorBatchFailedEventsProducer).uid(extractorConfig.extractorBatchFailedEventsProducer)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(extractorConfig.logEventsOutputTag)
      .addSink(kafkaConnector.kafkaMapSink(extractorConfig.kafkaLogRouteTopic))
      .name(extractorConfig.extractorLogEventsProducer).uid(extractorConfig.extractorLogEventsProducer)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(extractorConfig.auditEventsOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(extractorConfig.kafkaLogRouteTopic))
      .name(extractorConfig.extractorAuditEventsProducer).uid(extractorConfig.extractorAuditEventsProducer)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(extractorConfig.failedEventsOutputTag)
      .addSink(kafkaConnector.kafkaMapSink(extractorConfig.kafkaFailedTopic))
      .name(extractorConfig.extractorFailedEventsProducer).uid(extractorConfig.extractorFailedEventsProducer)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    redactorStream.getSideOutput(extractorConfig.assessRawEventsOutputTag)
      .addSink(kafkaConnector.kafkaMapSink(extractorConfig.kafkaAssessRawTopic))
      .name(extractorConfig.assessRawEventsProducer).uid(extractorConfig.assessRawEventsProducer)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    // Union both raw-event streams (extraction + redactor) for shadow write and preprocessor feed
    val rawFromExtraction = extractionStream.getSideOutput(extractorConfig.rawEventsOutputTag)
    val rawFromRedactor   = redactorStream.getSideOutput(extractorConfig.rawEventsOutputTag)
    val mergedRaw         = rawFromExtraction.union(rawFromRedactor)

    // Shadow write keeps SECOR and LPA backup consumers reading telemetry.raw intact
    mergedRaw.addSink(kafkaConnector.kafkaMapSink(extractorConfig.kafkaSuccessTopic))
      .name(extractorConfig.extractorRawEventsProducer).uid(extractorConfig.extractorRawEventsProducer)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    // --- Preprocessor chain (identical to PipelinePreprocessorStreamTask, fed in-memory) ---
    val eventStream =
      mergedRaw
        .map(m => new Event(m.asInstanceOf[java.util.Map[String, Any]]))
        .setParallelism(extractorConfig.downstreamOperatorsParallelism)
        .process(new PipelinePreprocessorFunction(preprocessorConfig))
        .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.validationFailedEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink(preprocessorConfig.kafkaFailedTopic))
      .name(preprocessorConfig.invalidEventProducer).uid(preprocessorConfig.invalidEventProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.duplicateEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaDuplicateTopic))
      .name(preprocessorConfig.duplicateEventProducer).uid(preprocessorConfig.duplicateEventProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.logEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaLogRouteTopic))
      .name(preprocessorConfig.logRouterProducer).uid(preprocessorConfig.logRouterProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.errorEventOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaErrorRouteTopic))
      .name(preprocessorConfig.errorRouterProducer).uid(preprocessorConfig.errorRouterProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.auditRouteEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaAuditRouteTopic))
      .name(preprocessorConfig.auditRouterProducer).uid(preprocessorConfig.auditRouterProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.auditRouteEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaPrimaryRouteTopic))
      .name(preprocessorConfig.auditEventsPrimaryRouteProducer).uid(preprocessorConfig.auditEventsPrimaryRouteProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.primaryRouteEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaPrimaryRouteTopic))
      .name(preprocessorConfig.primaryRouterProducer).uid(preprocessorConfig.primaryRouterProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.denormSecondaryEventsRouteOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaDenormSecondaryRouteTopic))
      .name(preprocessorConfig.denormSecondaryEventProducer).uid(preprocessorConfig.denormSecondaryEventProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.denormPrimaryEventsRouteOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaDenormPrimaryRouteTopic))
      .name(preprocessorConfig.denormPrimaryEventProducer).uid(preprocessorConfig.denormPrimaryEventProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.shareItemEventOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaPrimaryRouteTopic))
      .name(preprocessorConfig.shareItemsPrimaryRouterProducer).uid(preprocessorConfig.shareItemsPrimaryRouterProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    eventStream.getSideOutput(preprocessorConfig.cbAuditRouteEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](preprocessorConfig.kafkaCbAuditRouteTopic))
      .name(preprocessorConfig.cbAuditRouterProducer).uid(preprocessorConfig.cbAuditRouterProducer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    env.execute("TelemetryIntakeJob")
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object TelemetryIntakeStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val rawConfig = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("telemetry-intake.conf").withFallback(ConfigFactory.systemEnvironment()))

    // Each sub-config resolves against its own namespace; top-level base-config keys are the fallback
    val extractorConfig    = new TelemetryExtractorConfig(rawConfig.getConfig("extractor").withFallback(rawConfig))
    val preprocessorConfig = new PipelinePreprocessorConfig(rawConfig.getConfig("preprocessor").withFallback(rawConfig))
    val kafkaUtil          = new FlinkKafkaConnector(extractorConfig)
    val task               = new TelemetryIntakeStreamTask(extractorConfig, preprocessorConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$
