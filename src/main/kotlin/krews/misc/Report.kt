package krews.misc

import krews.db.WorkflowRun
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import kotlinx.html.*
import kotlinx.html.stream.appendHTML
import krews.db.TaskRun
import krews.db.TaskRuns
import krews.executor.DIAGNOSTICS_DIR
import krews.executor.LOGS_DIR
import krews.executor.OUTPUTS_DIR

internal fun createReport(db: Database, workflowRun: WorkflowRun, out: Appendable) =
    transaction(db) {
        val taskRuns = TaskRun.find { TaskRuns.workflowRunId eq workflowRun.id }
        createReport(workflowRun, taskRuns, out)
    }

private fun createReport(workflowRun: WorkflowRun, taskRuns: Iterable<TaskRun>, out: Appendable) {
    out.appendHTML().html {
        head {
            title("Workflow ${workflowRun.workflowName} Run Report")
            link(rel = "stylesheet", href = "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css")
            link(rel = "stylesheet", href = "https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@9.13.1/build/styles/tomorrow.min.css")
        }
        body {

            div("container-fluid") {
                h1 { +"Workflow ${workflowRun.workflowName} - Run ${workflowRun.startTime}" }

                val workflowStatus = when {
                    workflowRun.completedTime == null -> Status.IN_PROGRESS
                    workflowRun.completedSuccessfully -> Status.SUCCEEDED
                    else -> Status.FAILED
                }

                div("alert alert-${workflowStatus.colorClass}") {
                    when (workflowStatus) {
                        Status.IN_PROGRESS -> {
                            h4 { +"Run still in progress..." }
                        }
                        Status.SUCCEEDED -> {
                            h4 { +"Run completed successfully!" }
                            p {
                                +"You can find your output files under /run/${workflowRun.startTime}/$OUTPUTS_DIR"
                            }
                        }
                        Status.FAILED -> {
                            h4 { +"Run failed" }
                            p {
                                +("Find out what went wrong by checking failed tasks below, logs (/run/${workflowRun.startTime}/$LOGS_DIR) " +
                                        "and diagnostic outputs (/run/${workflowRun.startTime}/$DIAGNOSTICS_DIR")
                            }
                        }
                    }
                }


                h2 { +"Task Runs" }
                div {
                    id = "tasks"
                    for (taskRun in taskRuns) {
                        val taskStatus = when {
                            taskRun.completedTime == null -> Status.IN_PROGRESS
                            taskRun.completedSuccessfully -> Status.SUCCEEDED
                            else -> Status.FAILED
                        }
                        val taskHeadingId = "task-heading-${taskRun.id.value}"
                        val taskBodyId = "task-body-${taskRun.id.value}"

                        div("card mb-1") {
                            div("card-header alert-${taskStatus.colorClass}") {
                                id = taskHeadingId
                                button(classes = "btn btn-link alert-link") {
                                    attributes["data-toggle"] = "collapse"
                                    attributes["data-target"] = "#$taskBodyId"
                                    val statusMessage = when (taskStatus) {
                                        Status.IN_PROGRESS -> "In Progress..."
                                        Status.SUCCEEDED -> "Completed Successfully!"
                                        Status.FAILED -> "Failed"
                                    }
                                    +"${taskRun.taskName} - ${taskRun.id.value} ($statusMessage)"
                                }
                            }
                            div("collapse") {
                                id = taskBodyId
                                attributes["data-parent"] = "#tasks"
                                div("card-body") {
                                    h5 { +"Docker Image" }
                                    p { +taskRun.image }
                                    if (taskRun.command != null) {
                                        h5 { +"Command" }
                                        pre { code("hljs shell") { +taskRun.command!! } }
                                    }
                                    h5 { +"Input" }
                                    pre { code("hljs json") { +taskRun.inputJson } }
                                    if (taskRun.outputJson != null) {
                                        h5 { +"Output" }
                                        pre { code("hljs json") { +taskRun.outputJson!! } }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            script(src = "https://cdn.jsdelivr.net/npm/foundation-sites@6.5.1/dist/js/foundation.min.js") {}
            script(src = "https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@9.13.1/build/highlight.min.js") {}
            script { +"hljs.initHighlightingOnLoad();" }
            script(src = "https://code.jquery.com/jquery-3.2.1.slim.min.js") {}
            script(src = "https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js") {}
            script(src = "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js") {}
        }
    }
}

private enum class Status(val colorClass: String) {
    IN_PROGRESS("secondary"), SUCCEEDED("success"), FAILED("danger")
}