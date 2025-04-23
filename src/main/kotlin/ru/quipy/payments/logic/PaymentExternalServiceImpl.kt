package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.ConnectionPool
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

private val internalLogger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)


class AdaptiveTimeoutInterceptor(private val timeoutProvider: () -> Long) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val millis = timeoutProvider().toInt()
        return chain
            .withConnectTimeout(millis, TimeUnit.MILLISECONDS)
            .withReadTimeout(millis, TimeUnit.MILLISECONDS)
            .withWriteTimeout(millis, TimeUnit.MILLISECONDS)
            .proceed(chain.request())
    }
}

class PaymentExternalSystemAdapterImpl(
    private val cfg: PaymentAccountProperties,
    private val esService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
) : PaymentExternalSystemAdapter {

    companion object {
        private val emptyBody: RequestBody = RequestBody.create(null, ByteArray(0))
        private val jsonMapper: ObjectMapper = ObjectMapper().registerKotlinModule()
    }

    /* ---------------------------- inner DTO ---------------------------- */
    private sealed class Outcome<out T> {
        data class Success<out T>(val data: T) : Outcome<T>()
        object Retry : Outcome<Nothing>()
    }

    private val serviceLabel = cfg.serviceName
    private val merchantAccount = cfg.accountName
    private val avgLatency = cfg.averageProcessingTime
    private val maxRps = cfg.rateLimitPerSec
    private val parallelism = cfg.parallelRequests
    private val retryAttempts = 3

    /* ---------------------------- runtime metrics ---------------------------- */
    private var percentile90: Duration = Duration.ofMillis(avgLatency.toMillis() * 5)
    private var histMax = avgLatency.toMillis() * 8
    private val latencyHistogram = Histogram(avgLatency.toMillis(), histMax, 2)

    /* ---------------------------- coroutine machinery ---------------------------- */
    @OptIn(ExperimentalCoroutinesApi::class)
    private val scope = CoroutineScope(Dispatchers.IO.limitedParallelism(parallelism) + SupervisorJob())

    /* ---------------------------- http client ---------------------------- */
    private val httpClient = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(18, 6, TimeUnit.MINUTES))
        .addInterceptor(AdaptiveTimeoutInterceptor { percentile90.toMillis() })
        .build()

    private val rateLimiter = TokenBucketRateLimiter(
        rate = maxRps,
        bucketMaxCapacity = maxRps,
        window = avgLatency.toMillis(),
        timeUnit = TimeUnit.MILLISECONDS,
    )
    private val gate = Semaphore(parallelism)

    private val backoffBase = 200L
    private val backoffCap = 1_000L

    override fun performPaymentAsync(paymentId: UUID, amount: Int, startedAt: Long, deadline: Long) {
        scope.launch {
            internalLogger.info("[$merchantAccount] enqueue payment $paymentId")

            val txId = UUID.randomUUID()
            esService.update(paymentId) {
                it.logSubmission(success = true, txId, now(), Duration.ofMillis(now() - startedAt))
            }

            val requestUrl = buildString {
                append("http://localhost:1234/external/process?")
                append("serviceName=$serviceLabel&accountName=$merchantAccount&")
                append("transactionId=$txId&paymentId=$paymentId&amount=$amount&$percentile90")
            }
            val httpRequest = Request.Builder().url(requestUrl).post(emptyBody).build()

            val requestStart = System.currentTimeMillis()

            try {
                gate.acquire()

                var attempt = 1
                var completedSuccessfully = false

                while (attempt <= retryAttempts && !completedSuccessfully && !outOfTime(deadline)) {
                    rateLimiter.tick()

                    val outcome = withTimeoutOrNull(percentile90.toMillis()) {
                        dispatchCall(httpRequest, paymentId, txId)
                    }

                    when (outcome) {
                        is Outcome.Success -> {
                            completedSuccessfully = outcome.data
                            if (completedSuccessfully) break
                        }
                        is Outcome.Retry -> {
                            val d = backoffDelay(attempt, deadline)
                            if (d > 0) delay(d) else break
                        }
                        null -> break // timeout reached
                    }
                    attempt++
                }

                if (!completedSuccessfully) {
                    esService.update(paymentId) {
                        it.logProcessing(false, now(), txId, reason = "Max retries exceeded")
                    }
                }
            } catch (ex: Exception) {
                manageException(ex, paymentId, txId)
            } finally {
                updateMetrics(requestStart)
                gate.release()
            }
        }
    }

    private suspend fun dispatchCall(req: Request, paymentId: UUID, txId: UUID): Outcome<Boolean> =
        try {
            httpClient.newCall(req).execute().use { resp ->
                val body = try {
                    jsonMapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                } catch (parseErr: Exception) {
                    internalLogger.error("[$merchantAccount] invalid json for payment $paymentId", parseErr)
                    return Outcome.Retry
                }

                esService.update(paymentId) {
                    it.logProcessing(body.result, now(), txId, reason = body.message)
                }

                when {
                    body.result -> Outcome.Success(true)
                    resp.code == 429 -> {
                        noteRateLimit(resp)
                        Outcome.Retry
                    }
                    resp.code in 500..599 -> Outcome.Retry
                    else -> Outcome.Success(false)
                }
            }
        } catch (ex: Exception) {
            if (ex is SocketTimeoutException) {
                internalLogger.error("[$merchantAccount] timeout tx=$txId pay=$paymentId", ex)
                esService.update(paymentId) {
                    it.logProcessing(false, now(), txId, reason = "Request timeout")
                }
            }
            Outcome.Retry
        }


    private fun noteRateLimit(resp: Response) {
        val retryAfterMs = resp.header("Retry-After")?.toLongOrNull()?.times(1_000) ?: backoffBase
        internalLogger.warn("[$merchantAccount] 429 received, will retry after $retryAfterMs ms")
    }

    private fun outOfTime(deadline: Long): Boolean = now() > deadline - percentile90.toMillis()

    private fun backoffDelay(attempt: Int, deadline: Long): Long {
        val delay = minOf(backoffBase * (1L shl (attempt - 1)), backoffCap)
        return if (now() + delay < deadline) delay else -1
    }

    private fun manageException(err: Exception, paymentId: UUID, txId: UUID) {
        val reason = if (err is SocketTimeoutException) "Request timeout" else err.message ?: "Unknown error"
        internalLogger.error("[$merchantAccount] failure tx=$txId pay=$paymentId", err)
        esService.update(paymentId) { it.logProcessing(false, now(), txId, reason) }
    }

    private fun updateMetrics(start: Long) {
        val duration = System.currentTimeMillis() - start
        latencyHistogram.recordValue(duration)
        percentile90 = Duration.ofMillis(minOf(latencyHistogram.getValueAtPercentile(90.0), histMax))
    }

    override fun price() = cfg.price
    override fun isEnabled() = cfg.enabled
    override fun name() = cfg.accountName
}

fun now(): Long = System.currentTimeMillis()