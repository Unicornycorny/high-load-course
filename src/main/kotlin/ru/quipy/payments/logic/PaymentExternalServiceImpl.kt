package ru.quipy.payments.logic

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter   // если пользовался раньше – можно удалить
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resumeWithException
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds


class PaymentExternalSystemAdapterImpl(
    private val cfg: PaymentAccountProperties,
    private val esService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val LOG = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private const val TARGET_RPS = 1_000          // держим запас под лимитом 1 100 rps
        private const val MAX_PARALLEL = 20_000       // окно in-flight по cfg.parallelRequests
        private const val REQ_TIMEOUT_MS = 45_000L    // < processingTimeMillis (50 000)
        private const val MAX_RETRIES = 2             // 3 попытки всего
    }

    /* ---------- HTTP-клиент, настроенный на большую параллель ---------- */
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .dispatcher( //Dispatcher — регулирует, сколько одновременно вызовов OkHttp может поставить в очередь.
            Dispatcher().apply {
                maxRequests = MAX_PARALLEL
                maxRequestsPerHost = MAX_PARALLEL //вручную снимает штатный лимит 5/10, давая каждому соединению сколько угодно потоков
            }
        )
        .connectionPool(ConnectionPool(200, 30, TimeUnit.SECONDS)) //ConnectionPool(200, 30 s) — до 200 TCP-соединений кэшируются 30 s; уменьшает hand-shake.
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(REQ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .writeTimeout(REQ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .callTimeout(REQ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .build()

    /* ---------- Ограничители нагрузки ---------- */
    private val rateLimiter = SmoothRateLimiter(TARGET_RPS) //SmoothRateLimiter — самописный «дроппер» по алгоритму leaky-bucket: ровно 1_000 пропусков/сек.
    private val concurrencyLimiter = Semaphore(MAX_PARALLEL) //Semaphore(MAX_PARALLEL) — не позволяет запустить > 20 000 активных корутин одновременно (bulkhead pattern).

    /* ---------- coroutine-scope ---------- */
    private val scope = CoroutineScope(
        Dispatchers.IO.limitedParallelism(2_000) + SupervisorJob()
        //Dispatchers.IO: пул потоков, оптимизированный под I/O.
        //.limitedParallelism(2_000) — не больше 2000 реальных потоков даже при 20 000 корутин (N ≈ 10 × CPU не рвём JVM).
        //SupervisorJob — ошибка в одной корутине не убивает соседние.
    )

    /* ---------- API из интерфейса ---------- */

    //Захватываем Semaphore → гарантируем, что in-flight < MAX_PARALLEL.
    //
    //rateLimiter.acquire() ‒ может приостановить корутину ❌не поток.
    //
    //sendWithRetry(...) — внутри полный цикл попыток.
    //
    //В блок finally ресурс семафора освобождается.
    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        startedAt: Long,
        deadline: Long
    ) {
        scope.launch {
            concurrencyLimiter.acquire()
            try {
                rateLimiter.acquire() // может приостановить корутину, но не поток
                sendWithRetry(paymentId, amount, deadline)
            } finally {
                concurrencyLimiter.release()
            }
        }
    }

    override fun price(): Int = cfg.price
    override fun isEnabled(): Boolean = cfg.enabled
    override fun name(): String = cfg.accountName

    /* ---------- Внутренняя логика вызова ---------- */

    private suspend fun sendWithRetry(paymentId: UUID, amount: Int, deadline: Long) {
        var attempt = 0
        val txId = UUID.randomUUID()

        //Ограничение по числу попыток и абсолютному deadline (чтобы клиент не ушёл).
        while (attempt <= MAX_RETRIES && System.currentTimeMillis() < deadline) {
            attempt++
            val req = buildHttpRequest(paymentId, amount, txId)
            try {
                val resp = httpClient.newCall(req).await()
                when (resp.code) {
                    in 200..299 -> {          // SUCCESS
                        esService.update(paymentId) {
                            it.logProcessing(true, now(), txId, reason = "HTTP ${resp.code}")
                        }
                        resp.close()
                        return
                    }
                    in 500..599 -> {          // серверная ошибка – можно retry
                        resp.close()
                        delay(backoff(attempt))
                    }
                    429 -> {                  // превышен лимит – подождём hint или минимальный backoff
                        val retryAfter = resp.header("Retry-After")?.toLongOrNull()?.times(1_000) ?: backoff(attempt)
                        resp.close()
                        delay(retryAfter)
                    }
                    else -> {                 // 4xx - бизнес-FAIL
                        esService.update(paymentId) {
                            it.logProcessing(false, now(), txId, reason = "HTTP ${resp.code}")
                        }
                        resp.close()
                        return
                    }
                }
            } catch (e: IOException) {
                LOG.warn("I/O error on attempt $attempt for $paymentId : ${e.message}")
                delay(backoff(attempt))       // сетевой глитч – retry
            }
        }

        // если дошли сюда – все попытки истощены
        esService.update(paymentId) {
            it.logProcessing(false, now(), txId, reason = "Max retries exceeded")
        }
    }

    private fun buildHttpRequest(paymentId: UUID, amount: Int, txId: UUID): Request {
        val url = HttpUrl.Builder()
            .scheme("http")
            .host("localhost")
            .port(1234)
            .addPathSegments("external/process")
            .addQueryParameter("serviceName", cfg.serviceName)
            .addQueryParameter("accountName", cfg.accountName)
            .addQueryParameter("transactionId", txId.toString())
            .addQueryParameter("paymentId", paymentId.toString())
            .addQueryParameter("amount", amount.toString())
            .build()

        return Request.Builder()
            .url(url)
            .post(RequestBody.create(null, ByteArray(0)))
            .build()
    }

    private fun backoff(attempt: Int): Long {
        val base = (100L shl (attempt - 1)).coerceAtMost(1_000L) // 100, 200, 400, capped 1000
        return base + Random.nextLong(base)                      // jitter
    }
}

/* ---------- Утилиты ---------- */

private class SmoothRateLimiter(private val permitsPerSec: Int) {
    private val intervalNanos = 1_000_000_000L / permitsPerSec
    private val lastTime = AtomicLong(System.nanoTime()) //AtomicLong lastTime — храним «следующее допустимое время».

    suspend fun acquire() {
        while (true) {
            val prev = lastTime.get()
            val now = System.nanoTime()
            val next = maxOf(prev, now) + intervalNanos
            if (lastTime.compareAndSet(prev, next)) {
                val waitNs = next - now
                if (waitNs > 0) delay(waitNs / 1_000_000) //приостанавливает корутину; поток свободен.
                return
            }
        }
    }
}

private suspend fun Call.await(): Response = suspendCancellableCoroutine { cont ->
    enqueue(object : Callback {
        override fun onResponse(call: Call, response: Response) =
            cont.resume(response) {}
        override fun onFailure(call: Call, e: IOException) =
            cont.resumeWithException(e)
    })
    cont.invokeOnCancellation { cancel() }
}

private fun now() = System.currentTimeMillis()