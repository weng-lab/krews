package krews.core

import com.fasterxml.jackson.module.kotlin.*
import krews.misc.mapper
import java.nio.file.*
import java.util.zip.*

internal data class WorkflowCache(
    val inputFileCache: Map<String, CachedInputFile> = mapOf(),
    val outputFileCache: Map<String, CachedOutputFile> = mapOf()
)

internal data class CachedInputFile(
    val path: String,
    val remoteLastModTime: Long,
    val cachedCopyLastModTime: Long
)

internal data class CachedOutputFile(
    val path: String,
    val lastModifiedTime: Long,
    val taskName: String,
    val params: Any,
    val input: Any,
    val dockerImage: String,
    val command: String
)

internal fun writeCacheToFile(file: Path, workflowCache: WorkflowCache) {
    val cacheJson = mapper.writeValueAsBytes(workflowCache)
    GZIPOutputStream(Files.newOutputStream(file)).buffered().write(cacheJson)
}

internal fun loadCacheFromFile(file: Path): WorkflowCache {
    val inputStream = GZIPInputStream(Files.newInputStream(file)).buffered()
    return mapper.readValue(inputStream)
}