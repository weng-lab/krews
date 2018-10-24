package krews

import io.kotlintest.specs.StringSpec
import reactor.core.publisher.*
import java.time.Duration
import java.util.concurrent.Flow
import java.util.stream.Stream


class WorkProcessor<I, O>(private val executeFn: (I) -> O, private val parallelism: Long? = null) {

    private lateinit var subscription: Flow.Subscription
    private lateinit var sink: FluxSink<O>

    private val outFlux: Flux<O>
    init {
        outFlux = Flux.create { sink = it }
        sink.onRequest { subscription.request(it) }
    }

    private val inSub = object : Flow.Subscriber<I> {
        override fun onSubscribe(s: Flow.Subscription) {
            subscription = s
            val initialRequestAmount = parallelism ?: Long.MAX_VALUE
            subscription.request(initialRequestAmount)
        }

        override fun onNext(item: I) {
            val output = executeFn(item)
            sink.next(output)
            subscription.request(1)
        }

        override fun onError(err: Throwable) {
            sink.error(err)
        }

        override fun onComplete() {
            sink.complete()
        }
    }

}

class FluxTests : StringSpec({
    "test" {
        val flux1 = Flux.defer { Flux.fromStream(Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) }

        val tp0 = TopicProcessor.builder<Int>().bufferSize(3).build()
        flux1.subscribe(tp0)

        val flux2 = tp0.delayElements(Duration.ofSeconds(1)).map {
            print("In Flux2: $it\n")
            "${it}a"
        }

        val tp1 = TopicProcessor.create<String>()
        flux2.subscribe(tp1)

        val flux3 = tp1.map {
            print("In Flux3: $it\n")
            "${it}b"
        }
        val flux4 = tp1.map {
            print("In Flux4: $it\n")
            "${it}c"
        }
        val flux5 = flux3.zipWith(flux4)

        flux5.subscribe { print(it) }
        flux5.blockLast()
    }
})