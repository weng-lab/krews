package krews.core

import com.fasterxml.jackson.module.kotlin.convertValue
import krews.config.configMapper
import java.lang.IllegalStateException

class Params internal constructor(@PublishedApi internal val map: Map<String, Any>) {

    inline fun <reified P> get(key: String): P {
        return if (map[key] == null) {
            // If Type P is nullable
            if(null !is P)
                null as P
            else
                throw IllegalStateException("Param not found for non-nullable type ${P::class.simpleName}?")
        } else {
            configMapper.convertValue(map[key]!!)
        }
    }
    
}
