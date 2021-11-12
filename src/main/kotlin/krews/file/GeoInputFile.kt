package krews.file

data class GeoInputFile(val sraaccession: String, val read: ReadCategory, override val path: String, val dockerimage: String? = "ncbi/sra-tools") : InputFile() {
    override fun downloadFileImage() = "$dockerimage"
    override fun downloadFileCommand(containerBaseDir: String) = "fasterq-dump --outfile $containerBaseDir/$sraaccession.fastq -p $sraaccession"
    
}
enum class ReadCategory {
    READ_1, READ_2, SINGLE_END
}