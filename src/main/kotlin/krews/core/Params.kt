package krews.core

class Params internal constructor(private val map: Map<String, Any>) {
    @Suppress("UNCHECKED_CAST")
    fun <P> get(key: String) = map[key] as P
}
