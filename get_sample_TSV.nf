params.sample_file = ''
params.bed_file = ''
params.upload = 'false'

nextflow.enable.dsl=2

process processOutput {
    // there is a problem to work it with another venv when using the HUJI slurm
    label "medium_slurm"
   tag "process_${chrom}"

   publishDir params.outputDir , mode: 'copy'
    
    input:
    path chrom_tsv
    path regionFile
    path sampleFile
    
    output:
    path "${regionFile.simpleName}.${sampleFile.simpleName}.parquet"
    
    script:
    outputName = "${regionFile.simpleName}.${sampleFile.simpleName}"  
    """
    source ${params.VENV}
    process_output.py ${outputName} ${chrom_tsv.join(' ')}
    """
}

process uploadData {
  label "small_slurm"
   tag 'upload'
    
    input:
    path parquetFile
    path sampleFile
    
    script:
    if (params.upload){
        println "upload files"
        """
        current_date=`date +'%d_%m_%y_%H'`

        ${params.DBXCLI} put $parquetFile $params.uploadDir/\$current_date/$parquetFile
        ${params.DBXCLI} put $sampleFile $params.uploadDir/\$current_date/$sampleFile
        """
    }
    else{
        println "not uploading"
        """
            echo "not uploading"
        """
    }
}

process createRegionFile{

    input:
        val chrom
        path regionFile
    output:
        path "${chrom}.${regionFile.simpleName}.bed*"
        
    script:
    output="${chrom}.${regionFile.simpleName}.bed"
    """
    grep -E '^${chrom}\\b' ${regionFile} > "${output}" 
    line_count=\$(wc -l < "$output")
    # Check if the line count is greater than 1000
    # if [ "\$line_count" -gt ${params.MAX_REGIONS} ]; then
    if [ "\$line_count" -gt ${params.REGION_SPLIT_SIZE} ]; then # fOR DEBUGGING!!!!
    split -l 1000 "${output}" "${output}_"
    fi
    """
}


process getSamples{
   publishDir params.sampleRawDir , mode: 'copy'
   label "medium_slurm"
   tag "process_${pathFile.split("/")[-1].replace('.vcf.gz','')}"

    input:
        tuple  val(pathFile), path(regionFile)
        path data
    output:
         path output
        // stdout
    script:
    output="${pathFile.split("/")[-1].replace('.vcf.gz','')}.tsv" 
    """
    # Loading necessary modules
    module load hurcs bcftools
    getSamples.sh ${pathFile} ${regionFile} ${output}
    """
}

def checkIfExistsResult(regionFile, sampleFile){

        if (file(params.outputDir).exists()){
            println "The file $regionFile.simpleName already processed with the samples in $sampleFile.simpleName \
            \n find the result in $params.outputDir "
         //   exit 1
        }

}

process mergeChrom {
    label "small_slurm"
    tag "merge_${chrom}"
    
    input:
    tuple val(chrom), path(gnomAD), path(gnomADtbi)
    path file_list

    output:
    path "${chrom}.tsv"
    
    script:
    outputName = "${chrom}"  
    """
    source ${params.VENV}
    merge_chrom.py ${chrom} ${gnomAD} ${outputName} ${file_list.join(' ')}
    """
}



process getGnomAD {
     label "big_slurm"
    tag "gnomAD_${chrom}"
    
    input:
        path regionFile
    
     output:
        tuple val(chrom) , path(output)
        
    
    script:
    split_name=regionFile.name.split('\\.')
    chrom="${split_name[0]}"
    if (split_name[-1].split('_').size() > 1){
        extension = regionFile.name.split('_')[-1]
        output = "${regionFile.baseName}.${extension}.tsv"
    }else{
        output = "${regionFile.baseName}.tsv"
    }
   
    """
    # Load necessary modules
    module load hurcs bcftools
    getGnomAD.sh ${chrom} ${regionFile} ${output}
    """  
}


process mergeGnomAD {

    publishDir params.curGnomADDir, pattern: '*.gz' , mode: 'copy' 
    publishDir params.curGnomADDir, pattern: '*.tbi', mode: 'copy'
    label "small_slurm"
    tag "merge_gnomAD_${chrom}"
    
    input:
    tuple val(chrom), path(gnomAD)
    path data

    output:
    tuple val(chrom), path("${output}.gz"),  path("${output}.gz.tbi")
    // stdout
    
    script:
    regionName = gnomAD[0].name.split("\\.")[1]
    output = "${chrom}.${regionName}.tsv"

    if (gnomAD instanceof List){

        """
        mergeGnomAD.sh ${output}  ${gnomAD.join(' ')}
        """
    }else{
            """
            # Compress the output file using bgzip
            bgzip ${output}
            
            # Create an index for the compressed output file using tabix
            tabix -s1 -b2 -e2 ${output}.gz
            """
    }
}


process proxyGnomAD {
    tag "proxyGnomAD_${chrom}"
    
    input:
        val chrom 
        path regionFile
        path dataDir
    
     output:
        tuple val(chrom), path("gnomAD.tsv"), path("gnomAD.tbi")
        
    
    script:
    """
    ln -s "${params.curGnomADDir}/${chrom}.${regionFile.simpleName}.tsv.gz" gnomAD.tsv
    ln -s "${params.curGnomADDir}/${chrom}.${regionFile.simpleName}.tsv.gz.tbi" gnomAD.tbi
    """
}

process proxySamples {
    tag "proxySamples_${output.replace('.tsv','')}"
    
    input:
        val sampleFile
        path dataDir

     output:
        path "${output}"
    
    script:
    output="${sampleFile.split("/")[-1].replace('.vcf.gz','')}.tsv"
    """
    ln -s "${params.sampleRawDir}/${output}" ${output}
    """
}

process gnomADReport {
    label "small_slurm"

    publishDir params.curGnomADDir, mode: 'move' 
    input:
        val gnomAD
        path regionFile
        path dataDir

     output:
        path "${output}"
    
    script:
    output="${regionFile.simpleName}.report"
    """
    # Load necessary modules
    module load hurcs bcftools

    gnomAD_report.sh ${regionFile} ${regionFile.simpleName} ${params.curGnomADDir} ${output}
    """
}


workflow {
    // using default parameters if they werent provided
    if (params.sample_file == ''){
        sampleFile = params.all_samples
    } else {
        sampleFile = params.sample_file
    }
    
    if (params.bed_file == ''){
        regionFile = params.cur_regions
    } else {
        regionFile = params.bed_file
    }

    // Extract the base name of the bed_file parameter
    regionFile = file(regionFile)
    sampleFile = file(sampleFile)
    dataDir = file('data/')
    params.outputDir = "$params.output_dir/$regionFile.simpleName/$sampleFile.simpleName"
    params.curGnomADDir = "$params.gnomADByRegionDir/$regionFile.simpleName/gnomAD"
    params.sampleRawDir=  "$params.gnomADByRegionDir/$regionFile.simpleName/samples_raw"
    checkIfExistsResult(regionFile, sampleFile)

    log.info """
        V C F - T S V   P I P E L I N E 
         regionFile: ${regionFile}
         sampleFile: ${sampleFile} 
         """
         .stripIndent()
    
    println "starting"

    chromosomes = []
    numChromosomes = 23

    for (int i = 1; i <= numChromosomes; i++) {
        if (i == 23) {
            chromosomes.add("chrX")
            chromosomes.add("chrY")
        } else {
            chromosomes.add("chr" + i)
        }
    }

    // getting gnomAD data if not exists
    
    Channel.from(chromosomes).branch {
            // the following checks if the gnomAD file exist
            noGnomAD: file("${params.curGnomADDir}/${it}.${regionFile.simpleName}.tsv.gz").exists()
            needGnomAD: true
            }.set { chrom }

    chrom.needGnomAD.view {"needGnomAD $it"}
    chrom.noGnomAD.view {"already have gnomAD $it"}
    // create region file for each chromosome
    region = createRegionFile(chrom.needGnomAD, regionFile)
    // if the region file have more peaks than MAX_REGIONS, it will be split
    splitGnomAD = getGnomAD(region.flatten()) //.groupTuple()
    splitGnomAD.groupTuple().view()
    // merge gnomAD data from same chromosome
    gnomAD = mergeGnomAD(splitGnomAD.groupTuple(),dataDir).concat(proxyGnomAD(chrom.noGnomAD,regionFile, dataDir))
    // generates a report to see if the process done well
    gnomADReport(gnomAD.toList(),regionFile, dataDir)

    // processing the sample files, 
    // checks if the file were processed before
    println "${params.sampleRawDir}"
    Channel.from(sampleFile.readLines()).branch {
            haveRawFile: file("${params.sampleRawDir}/${it.split("/")[-1].replace('.vcf.gz','')}.tsv" ).exists()
            noRawFile: true
            }.set { samples }

    sampleInput = samples.noRawFile.combine([regionFile])
    samplesOutput = getSamples(sampleInput, dataDir).collect().concat(proxySamples(samples.haveRawFile, dataDir))

    // merge gnomAD data & the sample data
    merged = mergeChrom(gnomAD, samplesOutput).collect()
    // doing some process of the file
    cleanD = processOutput(merged, regionFile, sampleFile)


    // upload the data to the dropbox, if params.upload == true
    uploadData(cleanD, sampleFile)  

}
