package org.nicktorwald.mq

import io.vertx.config.ConfigRetriever
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import io.vertx.kotlin.config.configRetrieverOptionsOf
import io.vertx.kotlin.config.configStoreOptionsOf
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging

class MqService : CoroutineVerticle() {
  companion object : KLogging()

  private lateinit var kafkaConsumer: KafkaConsumer<String, JsonObject>
  private lateinit var kafkaProducer: KafkaProducer<String, JsonObject>

  override suspend fun start() {
    val config = ConfigRetriever.create(
      vertx,
      configRetrieverOptionsOf(
        stores = listOf(configStoreOptionsOf(type = "env"))
      )
    ).config.await()

    val kafkaUrl: String = config[ENV_MQ_SERVICE_KAFKA_CONNECT] ?: "localhost:9094"
    val inTopic: String = config[ENV_MQ_SERVICE_MQ_IN_TOPIC] ?: "number.request"
    val outTopic: String = config[ENV_MQ_SERVICE_MQ_OUT_TOPIC] ?: "number.result"
    val processDelay: Long = config[ENV_MQ_SERVICE_PROCESS_DELAY] ?: 1000

    logger.info {
      """
        #MQ Service configuration:
        #  Kafka servers: $kafkaUrl
        #  Input topic: $inTopic
        #  Output topic: $outTopic
        #  Processing delay: $processDelay
      """.trimMargin("#")
    }
    initKafkaClients(kafkaUrl)

    kafkaConsumer.handler { record ->
      launch {
        delay(processDelay)
        val number = (1..100).random()
        val reqNo = record.value().getString("reqNo")
        val result = KafkaProducerRecord.create(
          outTopic,
          record.key(), json { obj("reqNo" to reqNo, "number" to number) }
        )
        try {
          kafkaProducer.send(result).await()
        } catch (error: Exception) {
          logger.error { "Failed to send a result $record" }
        }
      }
    }

    kafkaConsumer.subscribe(inTopic)
  }

  private fun initKafkaClients(kafkaUrl: String?) {
    val consumerKafkaConfig = mapOf(
      "bootstrap.servers" to kafkaUrl,
      "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" to "io.vertx.kafka.client.serialization.JsonObjectDeserializer",
      "group.id" to "numbers",
      "auto.offset.reset" to "earliest",
      "enable.auto.commit" to "true"
    )

    val producerKafkaConfig = mapOf(
      "bootstrap.servers" to kafkaUrl,
      "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" to "io.vertx.kafka.client.serialization.JsonObjectSerializer",
      "acks" to "1"
    )

    kafkaConsumer = KafkaConsumer.create(vertx, consumerKafkaConfig)
    kafkaProducer = KafkaProducer.create(vertx, producerKafkaConfig)
  }

  override suspend fun stop() {
    coroutineScope {
      launch { kafkaConsumer.close().await() }
      launch { kafkaProducer.close().await() }
    }
    logger.info { "All Kafka clients closed" }
  }
}
