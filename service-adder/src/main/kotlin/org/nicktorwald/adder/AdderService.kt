package org.nicktorwald.adder

import io.vertx.config.ConfigRetriever
import io.vertx.core.http.HttpVersion
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.predicate.ResponsePredicate
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import io.vertx.kotlin.config.configRetrieverOptionsOf
import io.vertx.kotlin.config.configStoreOptionsOf
import io.vertx.kotlin.core.http.httpServerOptionsOf
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.ext.web.client.webClientOptionsOf
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import mu.KLogging

/**
 *
 */
class AdderService : CoroutineVerticle() {
  companion object : KLogging()

  private lateinit var mongoClient: MongoClient;
  private lateinit var restClient: WebClient;

  private lateinit var kafkaConsumer: KafkaConsumer<String, JsonObject>;
  private lateinit var kafkaProducer: KafkaProducer<String, JsonObject>;

  private val kafkaRequestContext = object {
    private var reqNo = 0UL;
    private val awaitingRequests = mutableMapOf<String, Channel<Int>>()
    fun nextReqNo() = (reqNo++).toString()
    operator fun get(reqNo: String) = awaitingRequests[reqNo]
    operator fun set(reqNo: String, value: Channel<Int>) {
      awaitingRequests[reqNo] = value
    }
  }

  override suspend fun start() {
    val config = ConfigRetriever.create(
      vertx,
      configRetrieverOptionsOf(
        stores = listOf(configStoreOptionsOf(type = "env"))
      )
    ).config.await()

    val kafkaUrl: String = config[ENV_ADDER_SERVICE_KAFKA_CONNECT] ?: "localhost:9094"
    val outTopic: String = config[ENV_ADDER_SERVICE_MQ_OUT_TOPIC] ?: "number.request"
    val inTopic: String = config[ENV_ADDER_SERVICE_MQ_IN_TOPIC] ?: "number.result"
    val restServiceHost: String = config[ENV_ADDER_SERVICE_REST_HOST] ?: "localhost"
    val restServicePort: Int = config[ENV_ADDER_SERVICE_REST_PORT] ?: 8222
    val mongoHost: String = config[ENV_ADDER_SERVICE_MONGO_HOST] ?: "localhost"
    val mongoPort: Int = config[ENV_ADDER_SERVICE_MONGO_PORT] ?: 27017
    val listenPort: Int = config[ENV_ADDER_SERVICE_LISTEN_PORT] ?: 8111

    logger.info {
      """
        #MQ Service configuration:
        #  Kafka servers: $kafkaUrl
        #  Input topic: $inTopic
        #  Output topic: $outTopic
        #  Rest server: $restServiceHost:$restServicePort
        #  MongoDB server: $mongoHost:$mongoPort
        #  Listen port: $listenPort
      """.trimMargin("#")
    }

    initKafkaClients(kafkaUrl)
    initMongoClient(mongoHost, mongoPort)
    initRestClient(restServiceHost, restServicePort)

    kafkaConsumer.handler { record ->
      launch {
        val reqNo = record.value().getString("reqNo")
        val number = record.value().getNumber("number").toInt()

        val channel = kafkaRequestContext[reqNo]
        channel?.send(number)
      }
    }
    kafkaConsumer.subscribe(inTopic)

    val mongoNumberTags = listOf("number1", "number2", "number3")
    val router = Router.router(vertx)
    router.get("/complex-number")
      .produces("application/json")
      .handler() { request ->
        launch {
          val restNumberDeferred = async { loadRestNumber() }
          val mqNumberDeferred = async { loadMqNumber(outTopic) }
          val dbNumberDeferred = async { loadDbNumber(mongoNumberTags.random()) }

          val numbers = listOf(
            restNumberDeferred.await(),
            mqNumberDeferred.await(),
            dbNumberDeferred.await()
          )

          val result = json {
            obj(
              "numbers" to array(numbers),
              "sum" to numbers.sum()
            )
          }

          request.response()
            .putHeader("content-type", "application/json")
            .end(result.encode())
        }
      }

    val options = httpServerOptionsOf(
      acceptBacklog = 1024
    )
    val httpServer = vertx
      .createHttpServer(options)
      .requestHandler(router)
      .listen(listenPort)
      .await();

    println("Adder service started on ${httpServer.actualPort()}");
  }

  private suspend fun loadRestNumber(): Int {
    val response = restClient.get("/number")
      .putHeader("Accept", "application/json")
      .`as`(BodyCodec.jsonObject())
      .expect(ResponsePredicate.SC_SUCCESS)
      .expect(ResponsePredicate.JSON)
      .send()
      .await()

    return response.body().getNumber("number").toInt()
  }

  private suspend fun loadDbNumber(tagName: String): Int {
    val doc = mongoClient.findOne("numbers", json { obj("tag" to tagName) }, null).await()
    return doc.getNumber("number").toInt()
  }

  private suspend fun loadMqNumber(outTopic: String): Int {
    val reqNo = kafkaRequestContext.nextReqNo()
    val record = KafkaProducerRecord.create(
      outTopic,
      reqNo,
      json { obj("reqNo" to reqNo) }
    )
    val awaitingKafkaResult = Channel<Int>(1)
    kafkaRequestContext[reqNo] = awaitingKafkaResult
    try {
      kafkaProducer.send(record).await()
    } catch (error: Exception) {
      logger.error { "Failed to send a request $record" }
    }
    return awaitingKafkaResult.receive()
  }

  private fun initKafkaClients(kafkaUrl: String) {
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

  private fun initMongoClient(mongoHost: String, mongoPort: Int) {
    mongoClient = MongoClient.create(vertx, json {
      obj(
        "host" to mongoHost,
        "port" to mongoPort,
        "db_name" to "cto-king-problem",
        "maxPoolSize" to 32,
        "minPoolSize" to 16,
      )
    })
  }

  private fun initRestClient(restServiceHost: String, restServicePort: Int) {
    val httpClient = vertx.createHttpClient(
      webClientOptionsOf(
        protocolVersion = HttpVersion.HTTP_2,
        http2MaxPoolSize = 1,
        http2MultiplexingLimit = 1024,
        defaultHost = restServiceHost,
        defaultPort = restServicePort,
        userAgent = "AdderService/1.0",
      )
    )

    httpClient.connectionHandler { connection ->
      logger.info { "Rest client connected to $connection: ${connection.remoteAddress()}" }
    }
    restClient = WebClient.wrap(httpClient)
  }

  override suspend fun stop() {
    coroutineScope {
      launch { kafkaConsumer.close().await() }
      launch { kafkaProducer.close().await() }
      launch { mongoClient.close().await() }
      restClient.close()
    }
    logger.info { "All Kafka clients closed" }
  }
}
