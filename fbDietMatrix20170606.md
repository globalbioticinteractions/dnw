Notes on diet matrix [fbDietMatrix20170606.tsv](https://gist.github.com/jhpoelen/18b5ffe083f32dd1dca360a83e34cfd3#file-fbdietmatrix20170606-tsv) created on 6 June 2017 using [buildFBDietMatrix.scala](https://gist.github.com/jhpoelen/18b5ffe083f32dd1dca360a83e34cfd3#file-buildfbdietmatrix-scala) 

The first two columns are fishbase specCode, and GBIF taxonid respectively. The third column is the total number of prey categories reported through GloBI for the fish species. The numeric values for the individual prey categories is an indicator for the number of individual predator-prey GloBI records. This might be an indicator for how well studies a particular species is. For instance, Gadus morhua, http://www.globalbioticinteractions.org/?interactionType=interactsWith&sourceTaxon=FBC%3AFB%3ASpecCode%3A69 Fishbase SpecCode 69, has a ton of interaction records available.    

# prereqs

1. Spark version 2.1.1
1. Scala version 2.11.6
1. interaction.tsv / taxonCache.tsv retrieved from http://globalbioticinteractions.org/references 

# configuration
the following spark configuration (```[spark dir]/conf/spark-defaults.conf```) was used:
```
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory              1g
spark.default.parallelism        400
spark.debug.maxToStringFields    250
```

# running

```
./bin/spark-shell
scala>:load "buildFBDietMatrix.scala"
```
produces the fish diet matrix with configured prey categories.

# post processing

The resulting diet matrix can now be used to link to fishbase resources (see https://github.com/jhpoelen/fishbase_archiver/releases by SpecCode) or using R libraries like rgbif, to retrieve occurrence counts or other information.

## example ecology_fishbase.tsv integation

```R
ecology <- read.csv('ecology_fishbase.tsv', sep='\t')
diet <- read.csv('fbDietMatrix20170606.tsv', sep='\t')
dietEcology <- merge(x = diet, y = ecology, by = 'SpecCode')
```

## example of retrieving occurrence counts using rgbif

```R
install.packages('httr')
diet <- read.csv('fbDietMatrix20170606.tsv', sep='\t')
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
- [ ] choose GBIF equivalent for categories that GBIF does not know:
Cat("Medusozoa", "ITIS", 718920))),
Cat("Clitellata", "ITIS", 718920))),//,
Cat("Crustacea", "ITIS", 83677),
Cat("Chondrichthyes", "ITIS", 914180),

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


