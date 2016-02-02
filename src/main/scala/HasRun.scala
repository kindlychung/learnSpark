import java.net.URI

import org.apache.spark.Partitioner

/**
  * Created by kaiyin on 2/1/16.
  */
trait HasRun {
  def run: Unit
}

case class Store(val name: String)


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
