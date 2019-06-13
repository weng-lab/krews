package krews.misc

import com.fasterxml.jackson.module.kotlin.readValue
import krews.db.WorkflowRun
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import kotlinx.html.*
import kotlinx.html.stream.appendHTML
import krews.core.*
import krews.db.*
import mu.KotlinLogging
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.*


private val log = KotlinLogging.logger {}

internal fun createReport(db: Database, workflowRun: WorkflowRun, out: Appendable) {
    transaction(db) {
        val taskRuns = TaskRun.all()
        createReport(workflowRun, taskRuns, out)
    }
}

private val timeFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
private fun durationFormat(duration: Duration) = duration.toString()
    .substring(2)
    .replace("(\\d[HMS])(?!$)", "$1 ")
    .toLowerCase()
    .trim()

private fun createReport(workflowRun: WorkflowRun, taskRuns: Iterable<TaskRun>, out: Appendable) {
    out.appendHTML().html {
        head {
            title("Workflow ${workflowRun.workflowName} Run Report")
            link(rel = "stylesheet", href = "https://maxcdn.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css")
            link(rel = "stylesheet", href = "https://cdn.datatables.net/1.10.19/css/dataTables.bootstrap4.min.css")
            link(rel = "stylesheet", href = "https://cdn.datatables.net/responsive/2.2.3/css/responsive.bootstrap4.min.css")
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
                        else -> {
                            h4 { +"Run failed" }
                            p {
                                +("Find out what went wrong by checking failed tasks below, logs " +
                                        "(/run/${workflowRun.startTime}/$LOGS_DIR) and diagnostic outputs " +
                                        "(/run/${workflowRun.startTime}/$DIAGNOSTICS_DIR)")
                            }
                        }
                    }
                }


                table(classes = "table table-striped table-bordered dt-responsive nowrap") {
                    id = "tasks"

                    thead {
                        tr {
                            th(classes = "all") { +"Task Name" }
                            th(classes = "all") { +"Task Run ID" }
                            th(classes = "all") { +"Status" }
                            th(classes = "all") { +"Start Time" }
                            th(classes = "all") { +"Duration" }
                            th(classes = "none") { +"Execution Details" }
                        }
                    }

                    tbody {
                        for (taskRun in taskRuns) {
                            val taskStatus = when {
                                taskRun.completedTime == null -> Status.IN_PROGRESS
                                taskRun.completionStatus == "completed" -> Status.SUCCEEDED
                                taskRun.completionStatus == "partially completed" -> Status.PARTIALLY_SUCCEDED
                                else -> Status.FAILED
                            }
                            tr {
                                td { +taskRun.taskName }
                                td { +"${taskRun.id}" }
                                td {
                                    when(taskStatus) {
                                        Status.IN_PROGRESS -> span(classes="badge badge-secondary") { +"In Progress" }
                                        Status.SUCCEEDED -> span(classes="badge badge-success") { +"Succeeded" }
                                        Status.PARTIALLY_SUCCEDED -> span(classes="badge badge-warning") { +"Partially Succeeded" }
                                        Status.FAILED -> span(classes="badge badge-danger") { +"Failed" }
                                    }
                                }
                                td { +timeFormat.format(Date(taskRun.startTime)) }
                                val endTime = taskRun.completedTime ?: System.currentTimeMillis()
                                td { +durationFormat(Duration.ofMillis(endTime - taskRun.startTime)) }

                                td {
                                    createExecutionDetails(taskRun)
                                }
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
            script(src = "https://cdn.datatables.net/responsive/2.2.3/js/dataTables.responsive.min.js") {}
            script(src = "https://cdn.datatables.net/responsive/2.2.3/js/responsive.bootstrap4.min.js") {}
            script { unsafe { raw(SCRIPT) } }
            style { unsafe { raw(STYLE) } }
        }
    }
}

private fun TD.createExecutionDetails(taskRun: TaskRun) {
    table(classes="table table-striped table-bordered mt-2") {
        thead {
            tr {
                th { +"Docker Image" }
                th { +"Command" }
            }
        }
        tbody {
            val taskRunExecutions = mapper.readValue<List<TaskRunExecution>>(taskRun.executionsJson)
            for (taskRunExecution in taskRunExecutions) {
                tr {
                    td {
                        +taskRunExecution.image
                    }
                    td(classes = "command-cell") {
                        pre { +"${taskRunExecution.command}" }
                    }
                }
            }
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
    #tasks {
        width: 100%;
    }
    """.trimIndent()

private val SCRIPT =
    """
    ${'$'}(document).ready(function() {
        ${'$'}('#tasks').DataTable();
    } );
    """.trimIndent()

private enum class Status(val colorClass: String) {
    IN_PROGRESS("secondary"), SUCCEEDED("success"), PARTIALLY_SUCCEDED("warning"), FAILED("danger")
}