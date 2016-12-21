package main

import java.io.{ DataInput, DataOutput, IOException }
import org.apache.hadoop.io.{ IntWritable, Text, WritableComparable }

case class AttributeNameValue(AttributeName: Text, AttributeValue: Text) extends WritableComparable[AttributeNameValue] {

    /* converting text to string || same as java */
    def name: String = AttributeName.toString()
    def value: String = AttributeValue.toString()

    /* parameterized constructor in scala */
    def this(name: String, value: String) = this(new Text(name), new Text(value))

    /* default constructor in scala */
    def this() = this(new Text(), new Text())

    /* override readfields and write || same as java */
    override def readFields(input: DataInput): Unit = {
      AttributeName.readFields(input);
      AttributeValue.readFields(input);
    }

    override def write(output: DataOutput): Unit = {
      AttributeName.write(output);
      AttributeValue.write(output);
    }
    
        override def compareTo(other: AttributeNameValue): Int = {
        0
    }
  }