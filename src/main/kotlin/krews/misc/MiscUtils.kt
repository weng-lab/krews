import mu.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Utility function for easily retrying an arbitrary block of code the given number of times before failing.
 */
fun <T> retry(name: String,
              numOfRetries: Int = 3,
              retryCondition: (e: Throwable) -> Boolean = { true },
              block: (attemptNum: Int) -> T): T {
    var throwable: Throwable? = null
    (1..numOfRetries).forEach { attempt ->
        try {
            return block(attempt)
        } catch (e: Throwable) {
            if (retryCondition(e)) {
                throwable = e
                log.error(e) { "Failed $name attempt $attempt / $numOfRetries" }
            } else {
                throw e
            }
        }
    }
    throw throwable!!
}

/**
 * Utility function for easily retrying an arbitrary block of code the given number of times before failing.
 */
suspend fun <T> retrySuspend(name: String,
              numOfRetries: Int = 3,
              retryCondition: (e: Throwable) -> Boolean = { true },
              block: suspend (attemptNum: Int) -> T): T {
    var throwable: Throwable? = null
    (1..numOfRetries).forEach { attempt ->
        try {
            return block(attempt)
        } catch (e: Throwable) {
            if (retryCondition(e)) {
                throwable = e
                log.error(e) { "Failed $name attempt $attempt / $numOfRetries" }
            } else {
                throw e
            }
        }
    }
    throw throwable!!
}