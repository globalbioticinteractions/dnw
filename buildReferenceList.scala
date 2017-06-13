
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

val sourceTarget = interactions.select("sourceTaxonIds","interactionTypeName","targetTaxonIds", "referenceCitation","sourceArchiveURI").as[(String, String, String, String, String)]

val trophicOnly = sourceTarget.filter( x => List("eats","eatenBy","preysOn","preyedUponBy").contains(x._2)) 

val consumerFood = trophicOnly.map { trophic => trophic match { case (food, "eatenBy", consumer, ref, src) => (consumer, food, ref, src) case (food, "preyedUponBy", consumer, ref, src) => (consumer, food, ref, src) case (consumer, _, food, ref, src) => (consumer, food, ref, src) } }

val fishFood = consumerFood.filter(_._1.contains("FBC:FB:SpecCode:")).filter(_._1.contains("GBIF:")).filter(_._2.contains("GBIF:"))

val consumers = fishFood.map{ case(a,b,c,d) => ((d,c), a.split("""\|""").map(_.trim).filter(_.startsWith("FBC:FB")).map(_.replace("FBC:FB:SpecCode:", "")).head) }

val consumersDistinctBySrcRef = consumers.distinct.map { case((src, ref), speccode) => ((src, ref), 1) }.rdd.reduceByKey(_ + _)

val consumerRecordsBySrcRef = consumers.map { case((src, ref), speccode) => ((src, ref), 1) }.rdd.reduceByKey(_ + _)

val consumerSummaryBySrcRef = consumersDistinctBySrcRef.join(consumerRecordsBySrcRef).map { case ((src, ref), (consumerCount, recordCount)) => (src, ref, consumerCount, recordCount) }

val columnNames = List("sourceArchiveURI","reference","consumer.distinct","consumer.records")

consumerSummaryBySrcRef.toDF(columnNames: _*).coalesce(1).write.option("header",true).option("delimiter", "\t").mode("overwrite").csv(s"$baseDir/fbConsumerSummary.tsv")

