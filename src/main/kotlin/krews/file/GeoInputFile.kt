package krews.file

data class GeoInputFile(val sraaccession: String, override val path: String, val dockerimage: String? = "ncbi/sra-tools") : InputFile() {
    override fun downloadFileImage() = "$dockerimage"
    override fun downloadFileCommand(containerBaseDir: String) = "fasterq-dump --outfile $containerBaseDir/$path -e 8 -p $sraaccession"
    
}