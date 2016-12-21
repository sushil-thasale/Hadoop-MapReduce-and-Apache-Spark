package main

import java.io.{ DataInput, DataOutput, IOException }
import org.apache.hadoop.io.{ IntWritable, Text, WritableComparable }

case class PlayerYearPair(playerID: Text, yearID: IntWritable) extends WritableComparable[PlayerYearPair] {

    /* converting text to string || same as java */
    def player: String = playerID.toString()
    def year: Int = yearID.get()

    /* parameterized constructor in scala */
    def this(player: String, year: Int) = this(new Text(player), new IntWritable(year))

    /* default constructor in scala */
    def this() = this(new Text(), new IntWritable())

    /* override readfields and write || same as java */
    override def readFields(input: DataInput): Unit = {
      playerID.readFields(input);
      yearID.readFields(input);
    }

    override def write(output: DataOutput): Unit = {
      playerID.write(output);
      yearID.write(output);
    }

    override def compareTo(other: PlayerYearPair): Int = {

      /* ascending order of playerID */
      /* if playerID is same then ascending order of yearID */
      val yearDiff = year - other.year
      var yearResult = 0

      if (yearDiff < 0) {
        yearResult = -1
      } else if (yearDiff > 0) {
        yearResult = 1
      } else {
        yearResult = 0
      }

      val result = player.compareTo(other.player);

      if (result != 0) {
        result
      } else {
        yearResult
      }

    }
  }