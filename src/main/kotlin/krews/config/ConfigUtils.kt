package krews.config

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.typesafe.config.Config
import com.typesafe.config.ConfigValue
import krews.core.Task
import krews.core.Workflow
import krews.misc.ConfigView
import krews.misc.mapper

const val DEFAULT_TASK_CONFIG_NAME = "default"

@PublishedApi internal val noDefaultTypesMapper by lazy {
    val mapper = jacksonObjectMapper()
    mapper.propertyNamingStrategy = PropertyNamingStrategy.KEBAB_CASE
    mapper
}

@PublishedApi internal inline fun <reified T> convertConfigMap(raw: Map<String, Any>) =
    mapper.readerWithView(ConfigView::class.java).forType(T::class.java).readValue<T>(noDefaultTypesMapper.writeValueAsBytes(raw))


@Suppress("UNCHECKED_CAST")
fun createParamsForConfig(config: Config): Map<String, Any> {
    val configRoot = mutableMapOf<String, Any>()
    deepOverrideConfig(configRoot, config.root())
    val params = configRoot["params"]
    return if (params != null && params is Map<*, *>) params as Map<String, Any> else mapOf()
}

/**
 * Creates workflow config object from raw deserialized HOCON config
 *
 * @param config: Raw HOCON config object from the Typesafe lib
 * @param workflow: The workflow the configuration is being applied to
 *
 * @return
 */
@Suppress("UNCHECKED_CAST")
fun createWorkflowConfig(config: Config, workflow: Workflow): WorkflowConfig {
    val configRoot = mutableMapOf<String, Any>()
    deepOverrideConfig(configRoot, config.root())
    val rawTaskConfigs = if (configRoot["task"] != null) configRoot["task"] as MutableMap<String, Map<String, Any>> else mutableMapOf()
    configRoot.remove("task")
    configRoot["tasks"] = createTaskConfigs(rawTaskConfigs, workflow.tasks.values)

    if (configRoot["params"] == null) {
        configRoot["params"] = mapOf<String, Any>()
    }

    return noDefaultTypesMapper.convertValue(configRoot)
}

/**
 * In our configuration files, task configurations will look a little different than the data model. There will be
 * three types with special key names:
 *
 * - task.default: to be used for all tasks, can be overridden
 * - task.name-mytask: apply only to tasks with the name following "name-"
 * - task.label-mylabel: apply only to tasks with the label following "label-"
 *
 * For names and labels, suffixes (following "name-" and "label-") are matched with modified names and labels.
 * Modified names and labels are made lowercase and have all non-alphanumeric characters replaced with '-'
 *
 * Takes every raw "task-" config maps, each of which can reference many tasks and override other configs,
 * and flattens into one TaskConfig object per task.
 *
 * @param rawTaskConfigs: Raw Map object graph serialized from config file but not mapped to Kotlin objects yet.
 * @param tasks: List of all Tasks that are going to run. rawTaskConfigs will be matched with these.
 *
 * @return New Map object with one config per task that uses task names as keys.
 *
 * @throws IllegalArgumentException if rawTaskConfig given with key that doesn't match any tasks.
 */
fun createTaskConfigs(rawTaskConfigs: Map<String, Map<String, Any>>, tasks: Collection<Task<*, *>>): Map<String, Map<String, Any>> {
    // Gather 3 types of configurations we need: default, by name, and by label
    var defaultTaskConfig: Map<String, Any>? = null
    val tasksToNameConfigs = mutableMapOf<String, Map<String, Any>>()
    val tasksToLabelConfigs = mutableMapOf<String, MutableSet<Map<String, Any>>>()
    rawTaskConfigs.forEach { (key, configVal) ->
        val applicableTasks = tasks.filter { task ->
            // If the key is "default" it applies to all tasks
            if (key == DEFAULT_TASK_CONFIG_NAME) {
                defaultTaskConfig = configVal
                return@filter true
            }

            // If the key matches the task name it applies to this task
            if (key == kebabify(task.name)) {
                tasksToNameConfigs[task.name] = configVal
                return@filter true
            }

            // If the key matches any task labels it applies to this task
            val kebabLabels = task.labels.map { kebabify(it) }
            if (kebabLabels.any { it == key }) {
                tasksToLabelConfigs.getOrPut(task.name) { mutableSetOf() }.add(configVal)
                return@filter true
            }
            return@filter false
        }

        // If the task config key doesn't apply to any tasks, throw an exception
        if (applicableTasks.isEmpty()) throw IllegalArgumentException("Task config $key doesn't apply to any tasks")
    }

    // Merge configs into mapping of task names -> single configs
    val tasksToConfigs = mutableMapOf<String, Map<String, Any>>()
    tasks.forEach { task ->
        val config = mutableMapOf<String, Any>()
        tasksToConfigs[task.name] = config

        if (defaultTaskConfig != null) {
            deepOverrideConfig(config, defaultTaskConfig!!)
        }

        val nameConfig = tasksToNameConfigs[task.name]
        if (nameConfig != null) {
            deepOverrideConfig(config, nameConfig)
        }

        val labelConfigs = tasksToLabelConfigs[task.name]
        labelConfigs?.forEach { labelConfig ->
            deepOverrideConfig(config, labelConfig)
        }
    }
    return tasksToConfigs
}

/**
 * Transforms given free-form strings into kebab case by making them lowercase and replacing all
 * non-alphanumeric characters with dashes.
 *
 * This function does not attempt to insert dashes into implied word breaks for CamelCase words.
 *
 * "MySampleValue_te5t test*&test" should become "mysamplevalue-te5t-test--test"
 */
private fun kebabify(value: String): String {
    return Regex("[^a-z0-9]").replace(value.toLowerCase(), "-")
}

/**
 * Deep clones the given "from" map into a given "to" mutable map.
 * Leaf values that exist in the "from" map are overridden.
 *
 * For example if we have:
 *
 * from = { "a" : { "b" : "b", "c": "c" } }
 * to = { "a" : { "c": "d" }, "e" : "e" }
 *
 * The result should be: { "a" : { "b" : "b", "c": "d" }, "e" : "e" }
 */
private fun deepOverrideConfig(to: MutableMap<String, Any>, from: Map<String, Any>) {
    from.forEach { (key, value) ->
        if (value is Map<*, *>) {
            if (!to.containsKey(key) || to[key] !is MutableMap<*,*>) to[key] = mutableMapOf<String, Any>()
            @Suppress("UNCHECKED_CAST")
            deepOverrideConfig(to[key] as MutableMap<String, Any>, from[key] as Map<String, Any>)
        } else {
            to[key] = if (value is ConfigValue) value.unwrapped() else value
        }
    }
}