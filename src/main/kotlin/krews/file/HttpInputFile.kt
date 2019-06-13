package krews.file

data class HttpInputFile(val url: String, override val path: String) : InputFile() {
    override fun downloadFileImage() = "alpine:3.9"
    override fun downloadFileCommand(containerBaseDir: String) = "wget $url -o $containerBaseDir/$path"
}