Notes on diet matrix [fbDietMatrix20170613.tsv](https://github.com/globalbioticinteractions/dnw/blob/aa46017f70e7ad7539971e884428671a2ec1c910/fbDietMatrix20170613.tsv) created on 13 June 2017 using [buildFBDietMatrix.scala](https://github.com/globalbioticinteractions/dnw/blob/aa46017f70e7ad7539971e884428671a2ec1c910/buildFBDietMatrix.scala) 
Also, the notes include generation of associated references [fbDietMatrixReferences20170613.tsv](https://github.com/globalbioticinteractions/dnw/blob/aa46017f70e7ad7539971e884428671a2ec1c910/fbDietMatrixReferences20170613.tsv) created on 13 June 2017 using [buildReferenceList.scala](https://github.com/globalbioticinteractions/dnw/blob/aa46017f70e7ad7539971e884428671a2ec1c910/buildReferenceList.scala) 

The first two columns are fishbase specCode, and GBIF taxonid respectively. The third column is the total number of prey categories reported through GloBI for the fish species. The numeric values for the individual prey categories is an indicator for the number of individual predator-prey GloBI records. This might be an indicator for how well studies a particular species is. For instance, Gadus morhua, http://www.globalbioticinteractions.org/?interactionType=interactsWith&sourceTaxon=FBC%3AFB%3ASpecCode%3A69 Fishbase SpecCode 69, has a ton of interaction records available.    

# prereqs

1. Spark version 2.1.1
1. Scala version 2.11.6
1. interaction.tsv (sha256: c46e398e19b1e43f9317518c3e6b53b3f96833c55ea2514c08708e0a3ec6a751) / taxonCache.tsv (sha256: dd0b2e1a76aa45965d1bc8a85157c69163308b1dd9828c7d7513890044c2b9a6) retrieved from http://globalbioticinteractions.org/references 

# configuration
the following spark configuration (```[spark dir]/conf/spark-defaults.conf```) was used:
```
spark.driver.memory              1g
spark.default.parallelism        400
spark.debug.maxToStringFields    250
```

# running

```
./bin/spark-shell
scala>:load "buildFBDietMatrix.scala"
scala>:load "buildReferenceList.scala"
```
produces the fish diet matrix with configured prey categories and associated references.

# post processing

The resulting diet matrix can now be used to link to fishbase resources (see https://github.com/jhpoelen/fishbase_archiver/releases by SpecCode) or using R libraries like rgbif, to retrieve occurrence counts or other information.

## example ecology_fishbase.tsv integation

```R
ecology <- read.csv('ecology_fishbase.tsv', sep='\t')
diet <- read.csv('fbDietMatrix20170610.tsv', sep='\t')
dietEcology <- merge(x = diet, y = ecology, by = 'SpecCode')
```

## example of retrieving occurrence counts using rgbif

```R
install.packages('httr')
diet <- read.csv('fbDietMatrix20170610.tsv', sep='\t')
message('retrieving occurrence counts for gbif taxon ids')
diet$gbif.taxon.occ.count <- lapply(diet$gbif.taxon.id, function(taxonKey) {
	resp <- httr::GET(paste('http://api.gbif.org/v1/occurrence/count?taxonKey=',taxonKey, sep=""))
	message('.', appendLF=FALSE)
	httr::stop_for_status(resp)
	httr::content(resp)
})
message('all done.')
```

# remaining issues

- [x] associate fresh/marine habitat with fish species (can be done in R, example provided)
- [x] associate gbif occurrence count with fish species (can be done in R, example provided)
- [x] choose GBIF equivalent for categories that GBIF does not know:
Cat("Medusozoa", "ITIS", 718920))),
Cat("Clitellata", "ITIS", 718920))),//,
Cat("Crustacea", "ITIS", 83677),
Cat("Chondrichthyes", "ITIS", 914180),

# notes
Chondrichthyes have been consolidated in a group that contains both Elasmobranchii (GBIF:121) and Holocephali (GBIF:120). Please see categories in buildFBDietMatrix.scala for more information.

# checking taxon ids
The taxon ids are numeric for sake of computational efficiency.

fishbase speccodes be checked using urls like:

http://www.globalbioticinteractions.org/?interactionType=interactsWith&sourceTaxon=FBC%3AFB%3ASpecCode%3A893

or by visiting fishbase directly:

http://fishbase.org/summary/893

GBIF taxon ids, similar using:

http://www.globalbioticinteractions.org/?interactionType=interactsWith&sourceTaxon=GBIF%3A2388994

or by visiting GBIF directly:

http://www.gbif.org/species/2388994


