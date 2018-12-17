package krews.core

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonView
import krews.misc.CacheView
import krews.misc.ConfigView
import krews.misc.DisplayView

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@JacksonAnnotationsInside
@JsonView(ConfigView::class, DisplayView::class)
annotation class CacheIgnored

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@JacksonAnnotationsInside
@JsonView(ConfigView::class)
annotation class Sensitive
