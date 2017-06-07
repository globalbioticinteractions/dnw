
import spark.implicits._                                                                                                           

val dietMatrix = Seq((id: Int) => id == 1) 


var baseDir = "/home/jorrit/proj/globi/hayden2017"
var numberOfPartitions = 200
//val interactionsFile = "interactions1k.tsv"
val interactionsFile = "interactions.tsv"

val taxonCacheFile = "taxonCache.tsv"
//val taxonCacheFile = "taxonCache1k.tsv"

def readTsv(file: String) = {
  spark.read.option("delimiter", "\t").option("header", true).csv(s"$file")
}

def toId(ids: String, prefix: String): Seq[Int] = {
  if (ids == null) List[Int]()
  else ids.split("\\|").map(_.trim).filter(_.trim.startsWith(prefix)).map(_.replace(prefix, "").toInt)
}

val interactions = readTsv(s"$baseDir/$interactionsFile")

val sourceTarget = interactions.select("sourceTaxonIds","interactionTypeName","targetTaxonIds").as[(String, String, String)]

val trophicOnly = sourceTarget.filter( x => List("eats","eatenBy","preysOn","preyedUponBy").contains(x._2)) 

val consumerFood = trophicOnly.map { trophic => trophic match { case (food, "eatenBy", consumer) => (consumer, food) case (food, "preyedUponBy", consumer) => (consumer, food) case (consumer, _, food) => (consumer, food) } }

val fishFood = consumerFood.filter(_._1.contains("FBC:FB:SpecCode:")).filter(_._1.contains("GBIF:")).filter(_._2.contains("GBIF:"))
val fishConsumerIdsFoodIds = fishFood.flatMap { case(fishIds, foodIds) => 
	val ids = fishIds.split("\\|").map(_.trim)
	val idFB = ids.filter(_.startsWith("FBC:FB:SpecCode")).map(_.replace("FBC:FB:SpecCode:", "")).head
	val idGBIF = ids.filter(_.startsWith("GBIF:")).map(_.replace("GBIF:", "")).head 
	val idsFood = foodIds.split("\\|").map(_.trim).filter(_.startsWith("GBIF")).map(_.replace("GBIF:", "").toInt)
	idsFood.map(foodId => ((idFB.toInt, idGBIF.toInt, foodId), 1))
}

val fishDiets = fishConsumerIdsFoodIds.rdd.reduceByKey(_ + _)
val fishDietCount = fishDiets.map { case ((fb,gbif,food), count) => ((fb, gbif), (food, count)) }

// get path ids for food items

val taxa = readTsv(s"$baseDir/$taxonCacheFile")

val gbifPathIds = taxa.select("id","pathIds").as[(String, String)].filter(_._1 != null).filter(_._1.startsWith("GBIF:")).filter(_._2 != null).filter(_._2.nonEmpty).filter(_._2.contains("GBIF:"))

val taxonIdPathId = gbifPathIds.map { case(id, ids) => 
	val taxonId = id.replace("GBIF:", "").toInt
	(toId(ids, "GBIF:"), taxonId)
}.map { case(pathIds, taxonId) => (taxonId, pathIds) }

val dietForFish = fishDietCount.map { case((fb, gbif), (food, count)) => (food, (count, (fb, gbif))) }

val dietReducedForFish = dietForFish.map { case (food, (count, (fb, gbif))) => ((fb, gbif, food), count) }.reduceByKey(_ + _)
val dietPathIdsForFish = dietReducedForFish.map { case ((fb, gbif, food), count) => (food, (count, (fb, gbif))) }

var dietPathEntriesFish = taxonIdPathId.rdd.join(dietPathIdsForFish).map { case(food, (pathIds, (count, (fb, gbif)))) => ((fb, gbif), pathIds, count) }

case class Cat(name: String, ns: String, id: Int, hasSubCategories: Boolean = true)
case class CategoryOther(parentTaxon: Cat, childCategories: Seq[Cat])

     val categoriesOther = Seq(
      CategoryOther(Cat("Cnidaria", "GBIF", 43),
        Seq(Cat("Anthozoa", "GBIF", 206), Cat("Medusozoa", "ITIS", 718920))),
      CategoryOther(Cat("Annelida", "GBIF", 42),
        Seq(Cat("Polychaeta", "GBIF", 256), Cat("Clitellata", "ITIS", 718920))),//,
          //Cat("Archiannelida", "no:match"))), // this taxon appears to be dubious (https://en.wikipedia.org/wiki/Haplodrili)
      CategoryOther(Cat("Mollusca", "GBIF", 52),
        Seq(Cat("Polyplacophora", "GBIF", 346), Cat("Bivalvia", "GBIF", 137),
          Cat("Gastropoda", "GBIF", 225), Cat("Cephalopoda", "GBIF", 136))),
      CategoryOther(Cat("Arthropoda", "GBIF", 54),
        Seq(Cat("Ostracoda", "GBIF", 353), Cat("Maxillopoda", "GBIF", 203),
          Cat("Malacostraca", "GBIF", 229),
          Cat("Insecta", "GBIF", 216), Cat("Brachiopoda", "GBIF", 110))),
      CategoryOther(Cat("Echinodermata", "GBIF", 50),
        Seq(Cat("Ophiuroidea", "GBIF", 350), Cat("Echinoidea", "GBIF", 221),
          Cat("Holothuroidea", "GBIF", 222))),
      CategoryOther(Cat("Chordata", "GBIF", 44),
        Seq(Cat("Actinopterygii", "GBIF", 204), Cat("Chondrichthyes", "ITIS", 914180),
          Cat("Amphibia", "GBIF", 131), Cat("Reptilia", "GBIF", 358),
          Cat("Aves", "GBIF", 212), Cat("Mammalia", "GBIF", 359))),
      CategoryOther(Cat("Plantae", "GBIF", 6), Seq()),
      CategoryOther(Cat("Animalia", "GBIF", 1),
        Seq(Cat("Cnidaria", "GBIF", 43, false), Cat("Annelida", "GBIF", 42, false),
          Cat("Mollusca", "GBIF", 52, false), Cat("Arthropoda", "GBIF", 54, false),
          Cat("Echinodermata", "GBIF", 50, false), Cat("Chordata", "GBIF", 44, false))))

     val dietMatrix = categoriesOther.flatMap(cat => {
       val idChildren = cat.childCategories.map(_.id)
       val otherCat = List((x: Seq[Int]) => x.contains(cat.parentTaxon.id) && x.intersect(idChildren).isEmpty)
       val childCats = cat.childCategories.filter(_.hasSubCategories).map(child => (x1: Seq[Int]) => x1.contains(cat.parentTaxon.id) && x1.contains(child.id))
       otherCat ++ childCats
     })

val dm = dietPathEntriesFish.map { case ((fb, gbif), pathIds, count) => ((fb, gbif), dietMatrix.map(_.apply(pathIds)).map(x => if (x) count else 0)) }

val dmReduced = dm.reduceByKey(_.zip(_).map(p => p._1 + p._2))

val fbNames = taxa.select("id","name").as[(String, String)].filter(_._1 != null).filter(_._1.startsWith("FBC:FB:SpecCode:")).map{ case (id, name) => (id.replace("FBC:FB:SpecCode:","").toInt, name) }.rdd

// add fishbase species names for visual inspection and cross check
val dmReducedWithName = dmReduced.map { case ((fb, gbif), dmRow) => (fb, ((fb, gbif), dmRow)) }.join(fbNames).map { case (_, (((fb, gbif), dmRow), name)) => ((name, fb, gbif), dmRow) }

val dmFlattened = dmReducedWithName.map { case((name, fb, gbif), dmRow) => List(name, fb, gbif) ++ List(dmRow.filter(_ > 0).size) ++ dmRow }.map(row => row.map(_.toString))

val columnNames = List("predator.name", "SpecCode", "gbif.taxon.id", "dietary.niche.width") ++ categoriesOther.flatMap(cat => List(cat.parentTaxon.name + ".other") ++ cat.childCategories.map(cat => if (cat.hasSubCategories) cat.name else s"${cat.name}-sub")) 

val columns = columnNames.zipWithIndex.map{ case(v,i) => $"value"(i) as s"$v" }

dmFlattened.toDF.select(columns: _*).coalesce(1).write.option("header",true).option("delimiter", "\t").mode("overwrite").csv(s"$baseDir/fbConsumerAndFood.tsv")
