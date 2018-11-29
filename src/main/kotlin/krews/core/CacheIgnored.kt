package krews.core

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside
import com.fasterxml.jackson.annotation.JsonProperty

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@JacksonAnnotationsInside
@JsonProperty(access = JsonProperty.Access.READ_ONLY)
annotation class CacheIgnored

