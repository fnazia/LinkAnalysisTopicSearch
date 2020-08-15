import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Hits{
  def main(args: Array[String]): Unit={
    val logFile = args(0)
    
    val spark = SparkSession.builder.appName("hits search").config("spark.master", "yarn").getOrCreate()
    val sc = spark.sparkContext
    val logData = sc.textFile(logFile)
    
    val searchTerm = args(6).toLowerCase() //"surfing" //args(4)
    val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x => (x + 1)).map(_.swap)
    val rootSet = titles.filter{case (id, line) => line.toLowerCase().contains(searchTerm)}
    
    val rootid = rootSet.keys.collect.toSet
    
    val notlogData = logData.filter{line => line.split(":\\s+").size > 1}
    val modlogData = notlogData.map{line =>
      val outs = line.split(":\\s+")
      (outs(0), outs(1))
    }
    //modlogData.foreach(println)
    val rootfilter = modlogData.filter{case (id, links) =>
      val linkset = (links.split("\\s+") :+ id).toSet
      val nn = linkset.map(_.toLong)
      nn.intersect(rootid).nonEmpty     
    }
    val flatrootfilter = rootfilter.flatMapValues{tolinks => tolinks.split("\\s+")}
    
    val hubBase = flatrootfilter.map{case (x, y) => (x.toLong, y.toLong)}.join(rootSet).map{case (x, y) => (x, y._1)}
    val authorityBase = flatrootfilter.map{case (x, y) => (y.toLong, x.toLong)}.join(rootSet).map{case (x, y) => (y._1, x)}
    
    val baseSet = (hubBase.union(authorityBase)).distinct()

    val baseUnique = sc.parallelize((baseSet.keys.collect ++ baseSet.values.collect).distinct)
    
    var authorityscore = baseUnique.map{line => (line, 1.0)}
    var hubscore = baseUnique.map{line => (line, 1.0)}
    
    val authoritytohub = baseSet.map(_.swap)//.groupByKey()
    val basemaptozero = baseUnique.map{line => (line, 0)}
    
    val authorityMatrix = (basemaptozero.leftOuterJoin(authoritytohub)).mapValues{case (v1, v2) => v2.getOrElse(0.toDouble)}
    val hubMatrix = (basemaptozero.leftOuterJoin(baseSet)).mapValues{case (v1, v2) => v2.getOrElse(0.toDouble)}
    
    val hubScoreSearchMatrix = authorityMatrix.map{case (a, h) =>
      if (h != 0){
        (h.toString().toLong, a)
      }else{
        (a, 0.0)
      }
    }
    
    val authorityScoreSearchMatrix = hubMatrix.map{case (h, a) =>
      if (a != 0){
        (a.toString().toLong, h)
      }else{
        (h, 0.0)
      }
    }
    
    for(i <- 1 to 30){
      val hubScoreWithAuthority = hubScoreSearchMatrix.join(hubscore)
      
      //hubScoreWithAuthority.foreach(println)
      
      val updatedAuthority = hubScoreWithAuthority.map{case (k, v) =>
        if (v._1 != 0){
          (v._1.toString().toLong, v._2)
        }else{
          (k, 0.0)
        }
      }
      
      //updatedAuthority.foreach(println)
      
      authorityscore = updatedAuthority.reduceByKey{(x, y) => x + y}
      
      val totalAuthority = authorityscore.values.sum()
      
      authorityscore = authorityscore.map{case (k, v) => (k, v/totalAuthority)}
      
      val authorityScoreWithHub = authorityScoreSearchMatrix.join(authorityscore)
      
      val updatedHub = authorityScoreWithHub.map{case (k, v) =>
        if (v._1 != 0){
          (v._1.toString().toLong, v._2)
        }else{
          (k, 0.0)
        }
      }
      
      hubscore = updatedHub.reduceByKey{(x, y) => x + y}
      
      val totalHub = hubscore.values.sum()
      
      hubscore = hubscore.map{case (k, v) => (k, v/totalHub)}
      
    }
    
    val allAuthority = authorityscore.coalesce(1, shuffle = true)
    val sortedAuthority = allAuthority.sortBy(_._2, false)
    val topAuthority = sc.parallelize(sortedAuthority.take(50))
    val finalAuthority = titles.join(topAuthority).coalesce(1, shuffle = true).sortBy(_._2._2, false)
    finalAuthority.saveAsTextFile(args(2))
    
    
    val allHub = hubscore.coalesce(1, shuffle = true)
    val sortedHub = allHub.sortBy(_._2, false)
    val topHub = sc.parallelize(sortedHub.take(50))
    val finalHub = titles.join(topHub).coalesce(1, shuffle = true).sortBy(_._2._2, false)
    finalHub.saveAsTextFile(args(3))
    
    rootSet.coalesce(1, shuffle = true).saveAsTextFile(args(4))
    
    val baseSetPrint = (titles.join(basemaptozero.map{case (x, y) => (x.toLong, y)})).map{case (x, y) => (x.toInt, y._1)}.coalesce(1, shuffle = true).sortBy(_._1, true)
    baseSetPrint.saveAsTextFile(args(5))
 
    spark.stop()
  }
}

