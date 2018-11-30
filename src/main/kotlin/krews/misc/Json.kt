package krews.misc

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper


class KrewsTypeResolverBuilder : ObjectMapper.DefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE) {
    override fun useForType(t: JavaType) = super.useForType(t) &&
                !Map::class.java.isAssignableFrom(t.rawClass) &&
                !Collection::class.java.isAssignableFrom(t.rawClass)
}

/**
 * Our default mapper used for deserializing config params and saving / loading task inputs and outputs as json.
 *
 * Has functionality to automatically detect non-concrete classes and use a "-type" property to resolve.
 */
val mapper by lazy {
    val mapper = jacksonObjectMapper()

    var typer: TypeResolverBuilder<*> = KrewsTypeResolverBuilder()
    typer = typer.init(JsonTypeInfo.Id.CLASS, null)
    typer = typer.inclusion(JsonTypeInfo.As.PROPERTY)
    typer = typer.typeProperty("-type")
    mapper.setDefaultTyping(typer)

    mapper.propertyNamingStrategy = PropertyNamingStrategy.KEBAB_CASE
    mapper
}