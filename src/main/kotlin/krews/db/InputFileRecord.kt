package krews.db

import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.IntIdTable


object InputFileRecords : IntIdTable("input_file") {
    val workflowRunId = TaskRuns.reference("workflow_run", WorkflowRuns)
    val path = text("path").index("input_file_by_path")
    val lastModifiedTime = datetime("last_modified_time")
}

class InputFileRecord(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<InputFileRecord>(InputFileRecords)

    var workflowRun by WorkflowRun referencedOn TaskRuns.workflowRunId
    var path by InputFileRecords.path
    var lastModifiedTime by InputFileRecords.lastModifiedTime
}