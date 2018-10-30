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

@JsonDeserialize(using = ParallelismDeserializer::class)
sealed class Parallelism
object UnlimitedParallelism : Parallelism()
data class LimitedParallelism (val limit: Int? = null) : Parallelism()

class ParallelismDeserializer : StdDeserializer<Parallelism>(Parallelism::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Parallelism {
        val mapper = p.codec as ObjectMapper
        val obj = mapper.readTree<TreeNode>(p) as JsonNode
        if (obj.nodeType == JsonNodeType.STRING && obj.asText().toLowerCase() == "unlimited") return UnlimitedParallelism
        if (obj.nodeType == JsonNodeType.NUMBER) return LimitedParallelism(obj.asInt())
        throw JsonMappingException(p, "Invalid parallelism. Must be either a number or \"unlimited\"")
    }
}