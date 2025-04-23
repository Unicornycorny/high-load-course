package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2))
        .build()

    private val semaphore = Semaphore(parallelRequests)

    private val rateLimiter = RateLimiter.of(
        "paymentRateLimiter-$accountName",
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(rateLimitPerSec)
            .timeoutDuration(Duration.ofSeconds(5))
            .build()
    )

    private val executor: ExecutorService = Executors.newFixedThreadPool(parallelRequests)
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
            logger.warn("[$accountName] PaymentId: $paymentId exceeds the deadline.")

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
            }

            return
        }

        if (!semaphore.tryAcquire()) {
            logger.warn("[$accountName] Semaphore unavailable for paymentId: $paymentId. Retrying later.")

            scheduler.schedule({
                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            }, 100, TimeUnit.MILLISECONDS)

            return
        }

        if (!rateLimiter.acquirePermission()) {
            logger.warn("[$accountName] Rate limiter unavailable for paymentId: $paymentId. Retrying later.")

            semaphore.release()

            scheduler.schedule({
                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            }, 100, TimeUnit.MILLISECONDS)

            return
        }

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
            logger.warn("[$accountName] PaymentId: $paymentId exceeds the deadline.")

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
            }

            semaphore.release()
            return
        }

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                semaphore.release()

                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(
                        transactionId.toString(),
                        paymentId.toString(),
                        false,
                        e.message
                    )
                }

                val retryableResponseCodes = setOf(200, 400, 401, 403, 404, 405)

                if (response.statusCode() !in retryableResponseCodes || !body.result) {
                    val delayDuration = response.headers().firstValue("Retry-After")
                        .map { Duration.parse(it) }
                        .orElse(Duration.ofMillis(100))

                    scheduler.schedule({
                        performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                    }, delayDuration.toMillis(), TimeUnit.MILLISECONDS)

                    return@thenApply null
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(true, now(), transactionId, reason = body.message)
                }
            }
            .exceptionally { e ->
                semaphore.release()

                logger.error(
                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                    e
                )

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }

                scheduler.schedule({
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                }, 200, TimeUnit.MILLISECONDS)

                return@exceptionally null
            }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()