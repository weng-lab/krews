package krews.core

import krews.config.*
import krews.file.*

data class WorkflowGraph(
    val headNodes: List<WorkflowGraphNode>,
    val inputFiles: List<InputFile>
)

data class WorkflowGraphNode(
    val taskRunContext: TaskRunContext<*, *>,
    val upstream: MutableSet<WorkflowGraphNode> = mutableSetOf(),
    val downstream: MutableSet<WorkflowGraphNode> = mutableSetOf(),
    var taskRun: TaskRun? = null
)

fun buildGraph(tasks: Set<Task<*, *>>, taskConfigs: Map<String, TaskConfig>): WorkflowGraph {
    val allGraphNodes = mutableListOf<WorkflowGraphNode>()
    val allInputFiles = mutableMapOf<String, InputFile>()
    val pathsToSources = mutableMapOf<String, WorkflowGraphNode>()

    // Create the graph nodes, find the paths for output files they create, and
    // get all input files
    for (task in tasks) {
        val taskRCs = task.createTaskRunContexts(taskConfigs.getValue(task.name).params)
        for (taskRC in taskRCs) {
            val graphNode = WorkflowGraphNode(taskRC)
            allGraphNodes += graphNode

            val inputFilesIn = getInputFilesForObject(taskRC.input)
            for (inputFile in inputFilesIn) {
                allInputFiles[inputFile.path] = inputFile
            }

            val outputFilesOut = getOutputFilesForObject(taskRC.output)
            for (outputFile in outputFilesOut) {
                pathsToSources[outputFile.path] = graphNode
            }
        }
    }

    // Connect all GraphNodes by OutputFile dependencies
    for (graphNode in allGraphNodes) {
        val outputFilesIn = getOutputFilesForObject(graphNode.taskRunContext.input)
        for (outputFile in outputFilesIn) {
            val dependencyNode = pathsToSources[outputFile.path] ?:
                throw Exception("Upstream task not found for OutputFile " +
                        "path ${outputFile.path} from task ${graphNode.taskRunContext.taskName}")
            graphNode.upstream += dependencyNode
            dependencyNode.downstream += graphNode
        }
    }

    val headNodes = allGraphNodes.filter { it.upstream.isEmpty() }
    return WorkflowGraph(headNodes, allInputFiles.values.toList())
}


/*

What's the graph algorithm?

1 - 3 - 4 - 6
2 /   \ 5

We need to keep a queue of jobs that are ready to run.
This queue will start with any jobs with no dependencies.
In this case 1 and 2.

When each task completes, we check downstream dependencies.
Each one that has all of it's dependencies completed
successfully will be added to the queue.

 */