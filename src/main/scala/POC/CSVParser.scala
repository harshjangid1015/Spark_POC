package POC

import org.apache.spark._
import java.io.StringReader
import java.io.StringWriter

import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.{SparkConf, SparkContext}

object CSVParser extends App {

  case class Person(name: String, favoriteAnimal: String)

  val conf = new SparkConf().setMaster("Parse CSV").setMaster("local")

  val sc = new SparkContext(conf)

  val inputFile = args(0)
  val outputFile = args(1)

  val input = sc.textFile(inputFile)
  val result = input.map { line =>
    val reader = new CSVReader(new StringReader(line));
    reader.readNext();
  }

  val people = result.map(x => Person(x(0), x(1)))
  val pandaLovers = people.filter(person => person.favoriteAnimal == "panda")
  pandaLovers.map(person => List(person.name, person.favoriteAnimal).toArray).mapPartition { people =>
    val stringWriter = new StringWriter();
    val cSVWriter = new CSVWriter(stringWriter);
    cSVWriter.writeAll(people.toList)
    Iterator(stringWriter.toString)
  }.saveAsTextFile(outputFile)


}
