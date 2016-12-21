import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MmulSparse {
    val on_csv = """\s*,\s*""".r

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mmul").setMaster("local")
        val sc   = new SparkContext(conf)

        val matrixA = readSparseMatrix(sc, "_test/dataA.csv")
        val matrixB = readSparseMatrix(sc, "_test/dataB.csv")
        
        matrixA.cache
        matrixB.cache
        
        val ((rowA, maxColA), valA) = matrixA.toArray.maxBy(_._1._2)
        val ((maxRowB, colB), valB) = matrixB.toArray.maxBy(_._1._1)
        
        val maxRange = math.max(maxColA, maxRowB)
                
        // TODO: calculate C = A * B

        // maxRange = max(max cols in A, max rows in B)
        val nrange = (0 : Long) to (maxRange)

        // step-1
        // reference => cell.scala from notes
        val distA = matrixA.flatMap( cell => {
            val ((ii, kk), vv) = cell
            nrange.map(jj => ((ii, jj, kk), vv))
        })

        // step-2
        val distB = matrixB.flatMap( cell => {
            val ((kk, jj), vv) = cell
            nrange.map(ii => ((ii, jj, kk), vv))
        })

        // step-3
        val distC = distA.join(distB).map( cell => {
            val ((ii, jj, kk), (aa, bb)) = cell
            ((ii, jj), aa * bb)
        })

        // step-4
        val cellC = distC.reduceByKey( (xx, yy) => xx + yy )
        
        // step-5
        // no need to sort since its a sparse matrix
        val matrixC = cellC.map(cell => {
            val ((ii, jj), vv) = cell
            (ii + ","+ jj + "," + vv)
            })
         
        val sparseCellInA = matrixA.count
        val sparseCellInB = matrixB.count    
        val distACount = distA.count
        val distBCount = distB.count
        val distCCount = distC.count    
        val cellCCount = cellC.count    
        val sparseCellInC  = matrixC.count
        
        
        println("\n\n\nAnalysis :")
        println("max cols in A :" + maxColA)
        println("max rows in B :" + maxRowB)
        println("Non-zero cells in matrix A : " + sparseCellInA)
        println("Non-zero cells in matrix B : " + sparseCellInB)
        println("dist A count : " + distACount)
        println("dist B count : " + distBCount)
        println("dist C count : " + distCCount)
        println("cell C count : " + cellCCount)
        println("Non-zero cells in matrix C : " + sparseCellInC)
        println("\n\n\n")
        
        matrixC.saveAsTextFile("output")    
    }
    
    def readSparseMatrix(sc: SparkContext, path: String) = {
        val inputMatrix = sc.textFile(path).map(csvLine => csvLine.split(","))
        			  .map (values => ((values(0).trim.toLong ,
        			                    values(1).trim.toLong),
        			                    values(2).trim.toDouble))
        inputMatrix
    }
}
