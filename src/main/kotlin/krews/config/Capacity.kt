package krews.config

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.JsonNodeType

@JsonDeserialize(using = CapacityDeserializer::class)
data class Capacity(val bytes: Long) {
    constructor (value: Long, type: CapacityType) : this(value * type.bytesMultiplier)
    fun toType(type: CapacityType): Double {
        return bytes / type.bytesMultiplier.toDouble()
    }
}

enum class CapacityType(val bytesMultiplier: Long) {
    B(1),
    KB(1024),
    MB(KB.bytesMultiplier*1024),
    GB(MB.bytesMultiplier*1024),
    TB(GB.bytesMultiplier*1024)
}

internal fun stringToCapacity(str: String): Capacity {
    val regex = """(\d+)\s*([KMGT]?B)""".toRegex()
    val matchResult = regex.find(str.toUpperCase())
    val (value, type) = matchResult!!.destructured
    return Capacity(value.toLong(), CapacityType.valueOf(type))
}

class CapacityDeserializer : StdDeserializer<Capacity>(Capacity::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Capacity {
        val mapper = p.codec as ObjectMapper
        val obj = mapper.readTree<TreeNode>(p) as JsonNode
        var thrown: Throwable? = null
        try {
            if (obj.nodeType == JsonNodeType.STRING) return stringToCapacity(obj.asText())
        } catch (e: Exception) {
            thrown = e
        }
        throw JsonMappingException(p, "Invalid capacity. Must be in the format \"10GB\"", thrown)
    }
}