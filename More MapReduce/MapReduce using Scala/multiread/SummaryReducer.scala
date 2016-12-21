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
import java.util.Iterator

class SummaryReducer extends Reducer[PlayerYearPair, AttributeNameValue, PlayerYearPair, Text] {
  type Context = Reducer[PlayerYearPair, AttributeNameValue, PlayerYearPair, Text]#Context

  // info about player with minimun salary per home run
  var minSalaryPerHR = 1000000
  var minTeamID = ""
  var minYear = 0
  var minPlayer = ""
  var minSalary = 0
  var minHR = 0

  override def setup(ctx: Context) = {

    minSalaryPerHR = 1000000
    minTeamID = ""
    minYear = 0
    minPlayer = ""
    minSalary = 0
    minHR = 0
  }

  override def reduce(key: PlayerYearPair, values: java.lang.Iterable[AttributeNameValue], ctx: Context) = {

    var salary = 0
    var hr = 0
    var team = ""

    for (value <- values) {

      var attrName = value.AttributeName.toString

      var attrValue = value.AttributeValue.toString

      if (attrName.equals("salary")) {
        salary = toInt(attrValue)
      } else if (attrName.equals("HR")) {
        hr = toInt(attrValue)
      } else if (attrName.equals("team")) {
        team = attrValue
      }
    }

    var salaryPerHR = 0

    if (hr == 0)
      salaryPerHR = 0
    else
      salaryPerHR = salary / hr

    if (salaryPerHR != 0) {

      if (salaryPerHR < minSalaryPerHR) {
        minSalaryPerHR = salaryPerHR
        minTeamID = team
        minHR = hr
        minSalary = salary
        minPlayer = key.playerID.toString
        minYear = toInt(key.yearID.toString)
      }

      val summary = "TeamID:" + team + "\tSalary:" + salary + "\tHome runs:" + hr + "\tSalary per home run:" + salaryPerHR

      ctx.write(key, new Text(summary))
    }
  }

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }

  override def cleanup(ctx: Context) = {
    println("\n\n\n Deatils about -> Minimun salary per home run : ")
    println("Year:"+minYear)
    println("Player:"+minPlayer)
    println("Team:"+minTeamID)
    println("Salary:"+minSalary)
    println("Home runs:"+minHR)
    println("Salary per home run:"+minSalaryPerHR)
  }

}