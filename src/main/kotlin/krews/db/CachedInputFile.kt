package krews.db

import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.IntIdTable


object CachedInputFiles : IntIdTable("cached_input_file") {
    val latestUseWorkflowRunId = CachedOutputs.integer("latest_use_workflow_run_id")
    val path = text("path").uniqueIndex("input_file_by_path")
    val lastModifiedTime = long("last_modified_time")
    val cachedCopyLastModifiedTime = long("cached_copy_last_modified_time")
}

class CachedInputFile(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<CachedInputFile>(CachedInputFiles)

    var latestUseWorkflowRunId by CachedInputFiles.latestUseWorkflowRunId
    var path by CachedInputFiles.path
    var lastModifiedTime by CachedInputFiles.lastModifiedTime
    var cachedCopyLastModifiedTime by CachedInputFiles.cachedCopyLastModifiedTime
}