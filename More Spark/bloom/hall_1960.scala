
import collection.immutable.BitSet

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.{ DataInput, DataOutput, IOException }

object Hall1960 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hall of Fame 1960").setMaster("local")
    val sc = new SparkContext(conf)

    // read Batting.csv
    val batting = sc.textFile("baseball/Batting.csv")
                    .map(line => line.split(","))
                    .filter(fields => fields.length > 11)
                    .map(k => ((k(0),k(1),k(3)),k(11)))

    // read Salaries.csv
    val salaries = sc.textFile("baseball/Salaries.csv")
                      .map(line => line.split(","))
                      .map(k => ((k(3),k(0),k(1)),k(4)))
      
    // read Masters(playerID, Fname, Lname)                  
    val master = sc.textFile("baseball/Master.csv")
                    .map(line => line.split(","))
                    .map(k => (k(0),(k(13),k(14))))

    // read Teams(teamID, yearsID, Tname)
    val teams = sc.textFile("baseball/Teams.csv")
                    .map(line => line.split(","))
                    .map(k => ((k(2),k(0)),k(40))).distinct()      
    
    /*----------------------*/  

    // join two pairRDDs - batting and salaries on (playerID, yearID, teamID)
    val bat_sal = batting.join(salaries)
                 .map(k => ((k._1._1),(k._1._2,k._1._3,k._2._1,k._2._2)))

    // join bat_sal and master on (playerID)
    val bat_sal_master = bat_sal.join(master)
                        .map(p => ((p._2._1._2,p._2._1._1),(p._1,p._2._1._3,p._2._1._4,p._2._2._1,p._2._2._2)))
    
    // join bat_sal_master and team on (teamID, yearID)
    // filter out records having salary or home runs = 0
    val bat_sal_master_team = bat_sal_master.join(teams).filter(p => ((toInt(p._2._1._3) != 0) && (toInt(p._2._1._2) != 0)))
    
    // compute salary per home run for each record
    val tobesorted = bat_sal_master_team.map(p => {
      val (teamID, yearID) = p._1;
      val (playerID,hr,salary,fname,lname) = p._2._1;
      val tname = p._2._2;
      val salperhr = toInt(salary) / toInt(hr);
      (playerID,yearID,fname,lname,tname,salary,hr,salperhr)
    })
    
    // sort RDD on salary per home run
    val sortedRecords = tobesorted.sortBy(p => (p._8,false))
    
    sortedRecords.saveAsTextFile("FinalRecords")
    
    // Output
    val top5 = sortedRecords.take(5).map(p => {
      val (playerID,yearID,fname,lname,tname,salary,hr,salperhr) = p;
      
        "" + playerID + "\t" + yearID + "\t" + fname + "," + lname + "\t" + tname +"\t" + salary + "\t" + hr + "\t" + salperhr;
    })

    top5.foreach(println)

    sc.stop()
  }

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }
}