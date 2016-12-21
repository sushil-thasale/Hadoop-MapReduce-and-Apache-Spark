package main

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator
import scala.collection.JavaConversions._

object Demo {
  def main(args: Array[String]) {
    println("Demo: startup")

    // Configure log4j to actually log.
    BasicConfigurator.configure();

    // Make a job
    val job = Job.getInstance()
    job.setJarByClass(Demo.getClass)
    job.setJobName("Demo")

    // set mapper output classes
    job.setMapOutputKeyClass(classOf[PlayerYearPair])
    job.setMapOutputValueClass(classOf[AttributeNameValue])

    // Set reducer class
    job.setReducerClass(classOf[SummaryReducer])

    // set reducer output classes
    job.setOutputKeyClass(classOf[PlayerYearPair])
    job.setOutputValueClass(classOf[Text])

    // Set up number of reducers
    job.setNumReduceTasks(2)

    // read Batting.csv
    MultipleInputs.addInputPath(job, new Path("baseball/Batting.csv"),
      classOf[TextInputFormat], classOf[BattingMapper])

    // read Salaries.csv    
    MultipleInputs.addInputPath(job, new Path("baseball/Salaries.csv"),
      classOf[TextInputFormat], classOf[SalaryMapper])

    FileOutputFormat.setOutputPath(job, new Path("out"))

    // Actually run the thing.
    job.waitForCompletion(true)
  }
}

class BattingMapper extends Mapper[Object, Text, PlayerYearPair, AttributeNameValue] {
  type Context = Mapper[Object, Text, PlayerYearPair, AttributeNameValue]#Context

  override def map(_k: Object, vv: Text, ctx: Context) {
    val cols = vv.toString.split(",")
    if (cols(0) == "playerID" || (cols.length < 12)) {
      return
    }

    val playerID = cols(0)
    val yearID = cols(1).toInt
    val HR = cols(11)
    

    if (yearID != 0 && !(playerID == "")) {
      ctx.write(new PlayerYearPair(playerID, yearID), new AttributeNameValue("HR", HR))
    }
  }
}

class SalaryMapper extends Mapper[Object, Text, PlayerYearPair, AttributeNameValue] {
  type Context = Mapper[Object, Text, PlayerYearPair, AttributeNameValue]#Context

  override def map(_k: Object, vv: Text, ctx: Context) {
    val cols = vv.toString.split(",")
    if (cols(0) == "yearID") {
      return
    }

    val yearID = cols(0).toInt
    val playerID = cols(3)
    val salary = cols(4)
    val teamID = cols(1)

    if (yearID != 0 && !(playerID == "") && !(teamID == "")) {
      ctx.write(new PlayerYearPair(playerID, yearID), new AttributeNameValue("team", teamID))
      ctx.write(new PlayerYearPair(playerID, yearID), new AttributeNameValue("salary", salary))
    }
  }
}
