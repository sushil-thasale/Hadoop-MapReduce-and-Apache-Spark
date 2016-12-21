package main

import collection.immutable.BitSet

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.{ DataInput, DataOutput, IOException }

object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Page Rank").setMaster("local")
    
    // using lz4 for best possible compression
    // to decrease the amount of data transferred over network while shuffling
    conf.set("spark.io.compression.codec", "lz4")
    
    // using garbage collector for optimization
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    
    val sc = new SparkContext(conf)

    val convergence = 10

    // parse each line using Java's Bz2 parser
    val parsedPages = sc.textFile(args(0))
                                  .mapPartitions{
                                        lineIter => 
                                        val bz: Bz2WikiParser = new Bz2WikiParser()
                                        lineIter.map(line => bz.parseLine(line))
                                        }                                  
                                  .filter(page => !(page == ""))
                                  .map(line => line.split(" => "))
                                  .map(fields => getPageData(fields))
                                  .cache

    // compute initial page-rank and assign it to each page
    val pageCount = parsedPages.count
    val initialPageRank = 1.0 / pageCount
    var newPageList = parsedPages.map(page => (page._1, (page._2, initialPageRank)))
    
    // refine page rank until convergence
    for (iteration <- 1 to convergence) {
      
      // compute sum of page rank of all sink nodes
      var sinkSum = newPageList.filter{ case (page, (outlinks, pageRank)) => (outlinks(0) == "")}
                               .map{ case (page, (outlinks, pageRank)) => pageRank}
                               .sum()
                               
      // compute current page's contribution towards page-rank of
      // each of its outlinks
      var newPageRanks = newPageList
                                .filter { case (page, (outlinks, pageRank)) => !(outlinks(0) == "") }
                                .flatMap { case (page, (outlinks, pageRank)) => outlinks.map(out => (out, pageRank / outlinks.size)) }
                                .reduceByKey((accum, y) => accum + y)
                                .mapValues(v => computePageRank(v, pageCount, sinkSum))
            
      // join is performed to fetch outlinks back and continue with next iteration
      // to consider dead links we could have performed
      // newPageList = newPageRanks.leftOuterJoin(parsedPages)
      // However, here I have chosen to eliminate dead links 
      newPageList = parsedPages.join(newPageRanks)
    }
    
    // sort pages on descending order of page-rank values
    // and take top 100 values
    val sortedPageRanks = newPageList.sortBy(_._2._2, false)                                                                          
                                     .take(100)                                     
    
    // multiplying page rank by -1
    // for using repartitionAndSortWithinPartitions's default ascending order partitioning
    val top100 = sc.parallelize(sortedPageRanks)
                   .map(page => (page._2._2*(-1), page._1))
                   .cache                                     
    
    // unpersist cached data
    parsedPages.unpersist()
    
    val rPartitioner = new org.apache.spark.RangePartitioner(1, top100)
    
    // using a single reducer
    // sorting in ascending order of key
    // actually it would be descending order of page rank
    // finally convert back to original page rank by multiplying by -1
    val top100_Final = top100.repartitionAndSortWithinPartitions(rPartitioner)
                             .map(page => (page._2, page._1*(-1)))
    
    top100_Final.saveAsTextFile(args(1))
    
    top100.unpersist()
    
    sc.stop()
  }

  // convert string containing outlinks into Array[String]
  // and return (page, outlinks)
  def getPageData(fields: Array[String]) = {
    val page = fields(0)

    // check for sink nodes
    if (fields.length == 2) {
      val outlinks = fields(1).split(",")
      (page, outlinks)
      
    } else {
      val outlinks = Array("")
      (page, outlinks)
    }
  }

  // computes final page rank considering teleportation factor and sink nodes
  def computePageRank(pr: Double, pageCount: Long, sinkSum: Double) = {
    (0.15 / pageCount + (0.85*pr) + (0.85*sinkSum/pageCount))    
  }
}
