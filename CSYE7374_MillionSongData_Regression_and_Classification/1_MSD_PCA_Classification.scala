import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by Bellamkonda on 7/11/2015.
 */
object MSD_Classification {
  object MSD_Regression {
    def main(args: Array[String]) {
      val sc = new SparkContext("local[2]", "Midterm1")
      val data = sc.textFile("C:/Users/Bellamkonda/Documents/GitHub/Midterm/data/YearPredictionMSD.csv")
      val parsedData = data.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble)))
      }
      val normalizer1 = new Normalizer()
      val data1 = parsedData.map(x => (if x.label > 1965 1 else 0, normalizer1.transform(x.features))).first()
      println("Normalized data = " + data1)
      println(parsedData.first())

      // Run training algorithm to build the model
      val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(10)
        .run(training)

      // Compute raw scores on the test set.
      val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }

      model.clearThreshold()
      val ScoreAndLabels = test.map { case LabeledPoint(label, features) =>
        val Score = model.predict(features)
        (Score, label)
      }
      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(ScoreAndLabels)
      val auROC = metrics.areaUnderROC()

      val testErr = predictionAndLabels.filter(r => r._1 != r._2).count.toDouble / parsedData.count
      println("\nTest Error = " + testErr)
      println("\n Area under curve =" + auROC)

      val model1 = svmAlg.run(training)

      // Evaluate model on training examples and compute training error
      val labelAndPreds = test.map { point =>
        val prediction = model1.predict(point.features)
        (point.label, prediction)
      }

      model1.clearThreshold()
      val ScoreAndLabels = test.map { case LabeledPoint(label, features) =>
        val Score = model1.predict(features)
        (Score, label)
      }
      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(ScoreAndLabels)
      val auROC = metrics.areaUnderROC()


      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count

      println("\nTest Error = " + testErr)
      println("\n Area under curve =" + auROC)
}
