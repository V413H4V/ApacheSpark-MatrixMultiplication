import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/*
		Project: Matrix Multiplication with Apache SparkConf
		Author: Vaibhav Murkute
*/

@SerialVersionUID(123L)
case class Matrix_M ( index_i: Int, index_j: Int, value: Double)
      extends Serializable {}

@SerialVersionUID(123L)
case class Matrix_N ( index_j: Int, index_k: Int, value: Double)
      extends Serializable {}

@SerialVersionUID(123L)
case class ResultMatrix ( index_i: Int, index_k: Int, value: Double)
      extends Serializable {
	
	override def toString(): String = "" + index_i + " " + index_k + " " + value 
}
	  
object Multiply {
  def main(args: Array[ String ]) {
	
	val conf = new SparkConf().setAppName("Multiply")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val m_matrix = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                Matrix_M(a(0).toInt, a(1).toInt, a(2).toDouble) } )
	val n_matrix = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                Matrix_N(a(0).toInt, a(1).toInt, a(2).toDouble) } )
	var prod = m_matrix.map( m_matrix => (m_matrix.index_j,m_matrix) ).join(n_matrix.map( n_matrix => (n_matrix.index_j,n_matrix) ))
                .map { case (k,(m_matrix,n_matrix)) => ((m_matrix.index_i,n_matrix.index_k), m_matrix.value*n_matrix.value) }
		.reduceByKey((val1,val2) => val1 + val2)
		.sortByKey()
	
	//prod.sortBy { case ((idx_i, idx_j), product) => (idx_i, idx_j)}
	//prod.sortBy { case ((idx_i, idx_j), product) => (idx_i)}

	val result = prod.map { case ((idx_i, idx_j), product) => ResultMatrix(idx_i, idx_j, product)}

	result.saveAsTextFile(args(2))
    sc.stop()

  }
}
