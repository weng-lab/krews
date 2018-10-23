package krews

open class Workflow {
    lateinit var name: String
        internal set

    lateinit var params: Map<String, Any>
        internal set

    val tasks: MutableList<Task<*, *>> = mutableListOf()

    internal constructor()
    constructor(name: String) {
        this.name = name
    }

    fun <I : Any, O : Any> task(name: String, init: TaskBuilder<I, O>.() -> Unit): Task<I, O> {
        val builder = TaskBuilder<I, O>(this, name)
        builder.init()
        val task = builder.build()
        tasks.add(task)
        return task
    }

}
