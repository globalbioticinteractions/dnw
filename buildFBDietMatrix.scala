
import spark.implicits._                                                                                                           

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
	val idsFood = foodIds.split("\\|").map(_.trim).filter(_.startsWith("GBIF:")).map(_.replace("GBIF:", "").toInt)
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

case class Cat(name: String, ns: String, partOf: (Seq[Int] => Boolean), hasSubCategories: Boolean = true)
case class CategoryOther(parentTaxon: Cat, childCategories: Seq[Cat])

def CatMatchMulti(ids: Seq[Int]) = {
  (x: Seq[Int]) => (x intersect ids).nonEmpty
}
def CatMatch(id: Int) = {
  (x: Seq[Int]) => x contains id
}

     val categoriesOther = Seq(
      CategoryOther(Cat("Cnidaria", "GBIF", CatMatch(43)),
        Seq(Cat("Anthozoa", "GBIF", CatMatch(206)))),
      CategoryOther(Cat("Annelida", "GBIF", CatMatch(42)),
        Seq(Cat("Polychaeta", "GBIF", CatMatch(256)), Cat("Clitellata", "GBIF", CatMatch(255)))),//,
          //Cat("Archiannelida", "no:match"))), // this taxon appears to be dubious (https://en.wikipedia.org/wiki/Haplodrili)
      CategoryOther(Cat("Mollusca", "GBIF", CatMatch(52)),
        Seq(Cat("Polyplacophora", "GBIF", CatMatch(346)), Cat("Bivalvia", "GBIF", CatMatch(137)),
          Cat("Gastropoda", "GBIF", CatMatch(225)), Cat("Cephalopoda", "GBIF",CatMatch(136)))),
      CategoryOther(Cat("Brachiopoda", "GBIF", CatMatch(110)), Seq()),
      CategoryOther(Cat("Arthropoda", "GBIF", CatMatch(54)),
        Seq(Cat("Ostracoda", "GBIF", CatMatch(353)), Cat("Maxillopoda", "GBIF", CatMatch(203)),
          Cat("Malacostraca", "GBIF", CatMatch(229)),
          Cat("Insecta", "GBIF", CatMatch(216)))),
      CategoryOther(Cat("Echinodermata", "GBIF", CatMatch(50)),
        Seq(Cat("Ophiuroidea", "GBIF", CatMatch(350)), Cat("Echinoidea", "GBIF", CatMatch(221)),
          Cat("Holothuroidea", "GBIF", CatMatch(222)))),
      CategoryOther(Cat("Chordata", "GBIF", CatMatch(44)),
        Seq(Cat("Actinopterygii", "GBIF", CatMatch(204)),
          Cat("Elasmobranchii.or.Holocephali", "GBIF", CatMatchMulti(Seq(121, 120))),
          Cat("Amphibia", "GBIF", CatMatch(131)), Cat("Reptilia", "GBIF", CatMatch(358)),
          Cat("Aves", "GBIF", CatMatch(212)), Cat("Mammalia", "GBIF", CatMatch(359)))),
      CategoryOther(Cat("Plantae", "GBIF", CatMatch(6)), Seq()),
      CategoryOther(Cat("Animalia", "GBIF", CatMatch(1)),
        Seq(Cat("Cnidaria", "GBIF", CatMatch(43), false), Cat("Annelida", "GBIF", CatMatch(42), false),
          Cat("Mollusca", "GBIF", CatMatch(52), false), Cat("Arthropoda", "GBIF", CatMatch(54), false),
          Cat("Brachiopoda", "GBIF", CatMatch(110), false), Cat("Echinodermata", "GBIF", CatMatch(50), false),
          Cat("Chordata", "GBIF", CatMatch(44), false))))

     val dietMatrix = categoriesOther.flatMap(cat => {
       val childMatchers = cat.childCategories.map(_.partOf)
       val otherCat = List((x: Seq[Int]) => {
         def noChildMatches = {
           val matches = childMatchers.map(_.apply(x))
           if (matches.isEmpty) true else !matches.reduce(_ || _)
         }
         cat.parentTaxon.partOf(x) && noChildMatches
       })
       val childCats = cat.childCategories.filter(_.hasSubCategories).map(child => (x1: Seq[Int]) => cat.parentTaxon.partOf(x1) && child.partOf(x1))
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

dmFlattened.toDF.select(columns: _*).distinct.coalesce(1).write.option("header",true).option("delimiter", "\t").mode("overwrite").csv(s"$baseDir/fbConsumerAndFood.tsv")
