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
import org.joda.time.DateTime
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.*

internal fun createReport(db: Database, workflowRun: WorkflowRun, out: Appendable) =
    transaction(db) {
        val taskRuns = TaskRun.find { TaskRuns.workflowRunId eq workflowRun.id }
        createReport(workflowRun, taskRuns, out)
    }

private val timeFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
private fun durationFormat(duration: Duration) = duration.toString()
    .substring(2)
    .replace("(\\d[HMS])(?!$)", "$1 ")
    .toLowerCase()

private fun createReport(workflowRun: WorkflowRun, taskRuns: Iterable<TaskRun>, out: Appendable) {
    out.appendHTML().html {
        head {
            title("Workflow ${workflowRun.workflowName} Run Report")
            link(rel = "stylesheet", href = "https://maxcdn.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css")
            link(rel = "stylesheet", href = "https://cdn.datatables.net/1.10.19/css/dataTables.bootstrap4.min.css")
        }
        body {
            div("container-fluid") {
                h1(classes = "display-4") {
                    +"Krews Report for ${workflowRun.workflowName}"
                    br()
                    small(classes = "text-muted") { +"Run ${workflowRun.startTime}" }
                }

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
                                        "and diagnostic outputs (/run/${workflowRun.startTime}/$DIAGNOSTICS_DIR)")
                            }
                        }
                    }
                }


                table(classes = "table table-striped table-bordered") {
                    id = "tasks"

                    thead {
                        tr {
                            th { +"Task Name" }
                            th { +"Task Run ID" }
                            th { +"Command" }
                            th { +"Status" }
                            th { +"Start Time" }
                            th { +"Duration" }
                        }
                    }

                    tbody {
                        for (taskRun in taskRuns) {
                            val taskStatus = when {
                                taskRun.completedTime == null -> Status.IN_PROGRESS
                                taskRun.completedSuccessfully -> Status.SUCCEEDED
                                else -> Status.FAILED
                            }
                            tr {
                                td { +taskRun.taskName }
                                td { +"${taskRun.id}" }
                                td(classes = "command-cell") { pre { +"${taskRun.command}" } }
                                td {
                                    when(taskStatus) {
                                        Status.IN_PROGRESS -> span(classes="badge badge-secondary") { +"In Progress" }
                                        Status.SUCCEEDED -> span(classes="badge badge-success") { +"Succeeded" }
                                        Status.FAILED -> span(classes="badge badge-danger") { +"Failed" }
                                    }
                                }
                                td { +timeFormat.format(Date(taskRun.startTime)) }
                                val endTime = taskRun.completedTime ?: System.currentTimeMillis()
                                td { +durationFormat(Duration.ofMillis(endTime - taskRun.startTime)) }
                            }
                        }
                    }
                }
            }

            script(src = "https://code.jquery.com/jquery-3.3.1.slim.min.js") {}
            script(src = "https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js") {}
            script(src = "https://maxcdn.bootstrapcdn.com/bootstrap/4.1.3/js/bootstrap.min.js") {}
            script(src = "https://cdn.datatables.net/1.10.19/js/jquery.dataTables.min.js") {}
            script(src = "https://cdn.datatables.net/1.10.19/js/dataTables.bootstrap4.min.js") {}
            script { unsafe { raw(SCRIPT) } }
            style { unsafe { raw(STYLE) } }
        }
    }
}

private val STYLE =
    """
    .command-cell {
        overflow: auto;
        max-width: 600px;
        max-height: 250px;
    }
    th {
        font-weight: normal;
    }
    """.trimIndent()

private val SCRIPT =
    """
    ${'$'}(document).ready(function() {
        ${'$'}('#tasks').DataTable();
    } );
    """.trimIndent()

private enum class Status(val colorClass: String) {
    IN_PROGRESS("secondary"), SUCCEEDED("success"), FAILED("danger")
}