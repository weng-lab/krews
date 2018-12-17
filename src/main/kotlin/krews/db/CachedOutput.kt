package krews.db

import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.IntIdTable

object CachedOutputs : IntIdTable("cached_output") {
    val latestUseWorkflowRunId = integer("latest_use_workflow_run_id")
    val taskName = text("task_name")
    val inputJson = text("input_json")
    val paramsJson = text("params_json").nullable()
    val command = text("command").nullable()
    val image = text("image")
    val outputJson = text("output_json")
    init {
        uniqueIndex("cached_output_by_inputs", taskName, inputJson, paramsJson, command, image)
    }
}

class CachedOutput(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<CachedOutput>(CachedOutputs)

    var latestUseWorkflowRunId by CachedOutputs.latestUseWorkflowRunId
    var taskName by CachedOutputs.taskName
    var inputJson by CachedOutputs.inputJson
    var paramsJson by CachedOutputs.paramsJson
    var command by CachedOutputs.command
    var image by CachedOutputs.image
    var outputJson by CachedOutputs.outputJson
}