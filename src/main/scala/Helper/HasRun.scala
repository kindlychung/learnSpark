package Helper

import java.net.URI

import org.apache.spark.Partitioner

/**
  * Created by kaiyin on 2/1/16.
  */
trait HasRun {
  def run: Unit
  def newRun: Unit = {
    println("\n==============================")
    run
  }
  def newRun(msg: String): Unit = {
    val msgLen = msg.length
    val sepLen: Int = (70 - msgLen) / 2
    val sep = "=" * sepLen
    println("\n" + sep + msg + sep)
    run
  }
}

case class Store(val name: String)

case class Person(name: String, lovesPandas: Boolean)


// custom partitioner
class DomainNamePartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    val domain = new URI(key.toString).getHost
    val code = (domain.hashCode % numPartitions)
    if(code < 0) {
      code + numPartitions
    } else {
      code
    }
  }
  override def equals(other: Any): Boolean = other match {
    case dnp: DomainNamePartitioner => dnp.numPartitions == numPartitions
    case _ => false
  }
}

case class Car(val mpg: Double, val cyl: Double, val disp: Double,
               val hp: Double, val drat: Double, val wt: Double,
               val qsec: Double, val vs: Double, val am: Double,
               val gear: Double, val carb: Double)

case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
case class Department(id: Int, name: String)

case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

case class Company(name: String, foundingYear: Int, numEmpoyees: Int)
case class Company1(name: String, foundingYear: Int, numEmpoyees: Int)
