package krews.misc

import java.time.Duration

val Number.days: Duration get() = Duration.ofDays(this.toLong())
val Number.hours: Duration get() = Duration.ofHours(this.toLong())
val Number.minutes: Duration get() = Duration.ofMinutes(this.toLong())