package org.nicktorwald.rest

import io.vertx.config.ConfigRetriever
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router
import io.vertx.kotlin.config.configRetrieverOptionsOf
import io.vertx.kotlin.config.configStoreOptionsOf
import io.vertx.kotlin.core.http.http2SettingsOf
import io.vertx.kotlin.core.http.httpServerOptionsOf
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging

/**
 *
 */
class RestService : CoroutineVerticle() {
  companion object : KLogging()

  private lateinit var httpServer: HttpServer;

  override suspend fun start() {
    val config = ConfigRetriever.create(
      vertx,
      configRetrieverOptionsOf(
        stores = listOf(configStoreOptionsOf(type = "env"))
      )
    ).config.await()

    val listenPort: Int = config[ENV_REST_SERVICE_LISTEN_PORT] ?: 8222
    val processDelay: Long = config[ENV_REST_SERVICE_PROCESS_DELAY] ?: 1000

    logger.info {
      """
        #REST Service configuration:
        #  Listen port: $listenPort
        #  Processing delay: $processDelay
      """.trimMargin("#")
    }

    val router = Router.router(vertx)
    router.get("/number")
      .produces("application/json")
      .handler() { request ->
        launch {
          delay(processDelay)

          val number = (1..100).random()
          val body = json {
            obj("number" to number)
          }

          request.response()
            .putHeader("content-type", "application/json")
            .end(body.encode())
        }
      }

    val options = httpServerOptionsOf(
      initialSettings = http2SettingsOf(
        maxConcurrentStreams = 1024
      )
    )
    val httpServer = vertx
      .createHttpServer(options)
      .requestHandler(router)
      .listen(listenPort)
      .await();

    println("REST service started on ${httpServer.actualPort()}");
  }

  override suspend fun stop() {
    httpServer.close().await()
    logger.info { "REST server closed" }
  }
}
