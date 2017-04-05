/**
  * Created by nehapulipati on 15/03/16.
  */
import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag


object SparkMultiplication {
  def main(args: Array[String]): Unit ={
    val numPartitions = 100
    val n = 1000000000

    val sc = new SparkContext("local[*]", "SparkMultiplication")

    val colA_RDD: SequenceRDD[DenseVector[Double]] = new SequenceRDD(sc, numPartitions, n, new VectorCreator(2))
    val rowB_RDD: SequenceRDD[DenseVector[Double]] = new SequenceRDD(sc, numPartitions, n, new VectorCreator(3))

    val aColsBrows =  colA_RDD.zip(rowB_RDD)

    val cParts: RDD[DenseMatrix[Double]] = aColsBrows.map{ case (colA: DenseVector[Double], rowB: DenseVector[Double]) =>
      val partialMatrix = DenseMatrix.zeros[Double](colA.length, rowB.length)
      for (i <- 0 to colA.length-1){
        partialMatrix(i,::) := (rowB.t :* colA(i))
      }
      partialMatrix
    }

    val c: DenseMatrix[Double] = cParts.reduce(_ + _)
    println(c)
  }
}

class VectorCreator(length: Int) extends Function[Int,DenseVector[Double]] with Serializable{
  def apply(i: Int): DenseVector[Double] = DenseVector.rand(length)
}

class SequenceRDD[T: ClassTag](sc: SparkContext, numPartitions: Int, rddLength: Int, createItem: Int => T) extends RDD[T](sc,Nil){
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val fullPartitionsLength = if(numPartitions == 1) rddLength else rddLength / (numPartitions - 1)
    val leftOverPartitionsLength = if(numPartitions == 1) 0 else rddLength % (numPartitions - 1)
    val isFullPartition = if(numPartitions == 1) true else split.index != numPartitions - 1

    val length = if(isFullPartition)
      fullPartitionsLength
    else
      leftOverPartitionsLength


    Stream.from(split.index*fullPartitionsLength).take(length).map(createItem).iterator
  }


  override protected def getPartitions: Array[Partition] = (0 until numPartitions).map(p => new DummyPartition(p)).toArray
}

class DummyPartition(val partition: Int) extends Partition with Serializable {
  override val index: Int = partition
}

