package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.*
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds

private val internalLogger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

class AdaptiveTimeoutInterceptor(private val timeoutProvider: () -> Long) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response =
        chain
            .withConnectTimeout(timeoutProvider().toInt(), TimeUnit.MILLISECONDS)
            .withReadTimeout(timeoutProvider().toInt(), TimeUnit.MILLISECONDS)
            .withWriteTimeout(timeoutProvider().toInt(), TimeUnit.MILLISECONDS)
            .proceed(chain.request())
}

class PaymentExternalSystemAdapterImpl(
    private val cfg: PaymentAccountProperties,
    private val esService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
) : PaymentExternalSystemAdapter {

    companion object {
        private val emptyBody: RequestBody = RequestBody.create(null, ByteArray(0))
        private val jsonMapper: ObjectMapper = ObjectMapper().registerKotlinModule()
    }

    /* ----------------- cfg shortcuts ----------------- */
    private val serviceLabel = cfg.serviceName
    private val merchantAccount = cfg.accountName
    private val avgLatency = cfg.averageProcessingTime
    private val maxRps = cfg.rateLimitPerSec
    private val parallelism = cfg.parallelRequests
    private val retryAttempts = 2 // <= 1 повторная попытка + первичный вызов

    /* ----------------- runtime metrics ----------------- */
    private var p90: Duration = Duration.ofMillis(avgLatency.toMillis() * 5)
    private val histMax = avgLatency.toMillis() * 8
    private val latencyHistogram = Histogram(avgLatency.toMillis(), histMax, 2)

    /* ----------------- coroutine machinery ------------- */
    @OptIn(ExperimentalCoroutinesApi::class)
    private val scope = CoroutineScope(Dispatchers.IO.limitedParallelism(parallelism) + SupervisorJob())

    /* ----------------- http client --------------------- */
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE)) // HTTP/2 без TLS (h2c)
        .connectionPool(ConnectionPool(2, 5, TimeUnit.MINUTES))
        .addInterceptor(AdaptiveTimeoutInterceptor { (p90.toMillis() * 0.9).toLong() })
        .build()

    /* ----------------- limiters ------------------------ */
    private val rpsLimiter = TokenBucketRateLimiter(
        rate = maxRps,
        bucketMaxCapacity = maxRps,
        window = 1_000, // 1‑секундное окно в миллисекундах
        timeUnit = TimeUnit.MILLISECONDS,
    )
    // страховой запас 500 слотов, чтобы случайно не пробить лимит 20k
    private val window = Semaphore((parallelism - 500).coerceAtLeast(1))

    /* ----------------- back‑off params ----------------- */
    private val backoffBase = 350L
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
                append("transactionId=$txId&paymentId=$paymentId&amount=$amount")
            }
            val httpRequest = Request.Builder().url(requestUrl).post(emptyBody).build()

            val startTs = System.currentTimeMillis()
            try {
                acquireSlot()
                var attempt = 1
                var done = false
                while (attempt <= retryAttempts && !done && !outOfTime(deadline)) {
                    rpsLimiter.tick()
                    val outcome = withTimeoutOrNull(callTimeout(deadline)) {
                        dispatchCall(httpRequest, paymentId, txId)
                    }
                    when (outcome) {
                        is Outcome.Success -> {
                            done = outcome.data
                            if (done) break
                        }
                        is Outcome.Retry -> {
                            val d = backoffDelay(attempt, deadline)
                            if (d > 0) delay(d) else break
                        }
                        null -> break // timeout
                    }
                    attempt++
                }
                if (!done) {
                    esService.update(paymentId) {
                        it.logProcessing(false, now(), txId, reason = "Max retries exceeded")
                    }
                }
            } catch (ex: Exception) {
                manageException(ex, paymentId, txId)
            } finally {
                releaseSlot()
                updateMetrics(startTs)
            }
        }
    }

    /* ----------------- helpers ------------------------- */

    private suspend fun acquireSlot() {
        window.acquire()
    }

    private fun releaseSlot() = window.release()

    private fun callTimeout(deadline: Long): Long =
        (deadline - now() - 1_000).coerceAtLeast(1_000) // чуть меньше TTL

    private fun outOfTime(deadline: Long): Boolean = now() > deadline - p90.toMillis()

    private fun backoffDelay(attempt: Int, deadline: Long): Long {
        val delay = (backoffBase * (1L shl (attempt - 1))).coerceAtMost(backoffCap)
        return if (now() + delay < deadline) delay else -1
    }

    private suspend fun dispatchCall(req: Request, paymentId: UUID, txId: UUID): Outcome<Boolean> =
        suspendCancellableCoroutine { cont ->
            httpClient.newCall(req).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    if (e is SocketTimeoutException) {
                        esService.update(paymentId) {
                            it.logProcessing(false, now(), txId, reason = "Request timeout")
                        }
                    }
                    cont.resume(Outcome.Retry, null)
                }

                override fun onResponse(call: Call, resp: Response) {
                    resp.use { r ->
                        val body = try {
                            jsonMapper.readValue(r.body!!.charStream(), ExternalSysResponse::class.java)
                        } catch (err: Exception) {
                            internalLogger.error("[$merchantAccount] invalid json for payment $paymentId", err)
                            cont.resume(Outcome.Retry, null)
                            return
                        }
                        esService.update(paymentId) {
                            it.logProcessing(body.result, now(), txId, reason = body.message)
                        }
                        val outcome = when {
                            body.result          -> Outcome.Success(true)
                            r.code == 429         -> Outcome.Retry // rate‑limit
                            r.code in 500..599    -> Outcome.Retry // transient
                            else                  -> Outcome.Success(false)
                        }
                        cont.resume(outcome, null)
                    }
                }
            })
        }

    private fun manageException(err: Exception, paymentId: UUID, txId: UUID) {
        val reason = if (err is SocketTimeoutException) "Request timeout" else err.message ?: "Unknown error"
        internalLogger.error("[$merchantAccount] failure tx=$txId pay=$paymentId", err)
        esService.update(paymentId) { it.logProcessing(false, now(), txId, reason) }
    }

    private fun updateMetrics(start: Long) {
        val duration = System.currentTimeMillis() - start
        latencyHistogram.recordValue(duration)
        p90 = Duration.ofMillis(minOf(latencyHistogram.getValueAtPercentile(90.0), histMax))
    }

    /* ----------------- DSL / utils --------------------- */
    private sealed class Outcome<out T> {
        data class Success<out T>(val data: T) : Outcome<T>()
        object Retry : Outcome<Nothing>()
    }

    override fun price() = cfg.price
    override fun isEnabled() = cfg.enabled
    override fun name() = cfg.accountName
}

fun now(): Long = System.currentTimeMillis()
