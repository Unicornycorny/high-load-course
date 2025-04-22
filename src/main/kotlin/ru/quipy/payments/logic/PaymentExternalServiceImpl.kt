package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

class TransientHttpException(val code: Int, message: String?) : Exception(message)

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        private val emptyBody = RequestBody.create(null, ByteArray(0))

        private val mapper = ObjectMapper().registerKotlinModule()

        private val retryableHttpCodes = setOf(429, 500, 502, 503, 504)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private var rateLimiter: RateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(1300L))// <--- весь вызов оборвётся через 1.3 сек
        .build()
    /* ----- Одновременные запросы: неблокирующее окно -- */
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")
// Логируем факт отправки (потребность тест‑пайплайна)
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        /* ---------- Ограничение по параллельным запросам (non‑blocking) ---- */
        while (true) {
            val windowResponse = ongoingWindow.putIntoWindow()
            if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                break
            }
            if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                logger.warn("[$accountName] Parallel requests limit timeout for payment $paymentId. Aborting external call.")
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
                return
            }
        }

        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        var delay = 1000L
        var maxRetries = 10
        var attempt = 0

        while (attempt < maxRetries) {
            try {
                while (!rateLimiter.tick()) {
                    if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                        throw SocketTimeoutException()
                    }
                }

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return
                    }

                    when (response.code) {
                        400, 401, 403, 404, 405 -> {
                            throw RuntimeException("Client error code: ${response.code}")
                        }
                        500 -> delay = 0
                    }

                    attempt++
                    Thread.sleep(delay)
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            } finally {
                ongoingWindow.releaseWindow()
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()