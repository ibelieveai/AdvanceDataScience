import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.rdd.RDD

/**
 * Created by Bellamkonda on 7/8/2015.
 */
object MSD_Regression {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "Midterm1")
    val data = sc.textFile("C:/Users/Bellamkonda/Documents/GitHub/Midterm/data/YearPredictionMSD.csv")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble)))
    }
    val normalizer1 = new Normalizer()
    val data1 = parsedData.map(x => (x.label, normalizer1.transform(x.features))).first()
    println("Normalized data = " + data1)
    println(parsedData.first())
    //val mat = RowMatrix.
    val PCA1 = new PCA(20).fit(parsedData.map(_.features))
    //Project vectors to the linear space spanned by the top 10 principal components, keeping the label
    val projected: RDD[LabeledPoint] = parsedData.map(p => p.copy(features = PCA1.transform(p.features)))
    val features  = projected.map(p => p.features).first()
    print("featuresCount:"+ features)
    // Normalizing the data

    // split in to training and test datasets

    val splits = projected.randomSplit(Array(0.6, 0.2, 0.2), seed = 13L)
    val training = splits(0)
    val validation = splits(1)
    val test = splits(2)

    // Building the model
    val numIterations = 10
    val alpha = 0.1 // step
    //var miniBatchFrac: Double = 1.0 // miniBatchFraction
    //val reg = 1e-1  //regParam
    //var regType = 'l2'
    //val useIntercept = 'True'  // intercept

    val model = {
      LinearRegressionWithSGD.train(training, numIterations)
    }
    val valuesAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ i => math.pow((i._1 - i._2), 2)}.reduce(_ + _)/valuesAndPreds.count
    println("training Mean Squared Error = " + MSE)

  }
}
