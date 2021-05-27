package krews.file

data class HttpInputFile(val url: String, override val path: String, val dockerimage: String? = "alpine:3.9") : InputFile() {
    override fun downloadFileImage() = "$dockerimage"
    override fun downloadFileCommand(containerBaseDir: String) = "wget $url -O $containerBaseDir/$path"
}