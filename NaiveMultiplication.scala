/**
  * Created by nehapulipati on 14/03/16.
  */
import breeze.linalg.{DenseMatrix, DenseVector}
object NaiveMultiplication {
  def main(args: Array[String]): Unit = {
    val n = 1000
    val a: DenseMatrix[Double] = DenseMatrix.rand(2,n)
    val b: DenseMatrix[Double] = DenseMatrix.rand(n,3)
    val cNormal = a * b
    println(s"Dot product of a and b is \n$cNormal")
  }
}

