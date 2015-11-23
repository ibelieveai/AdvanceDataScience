package Assignmet2

import org.apache.spark.SparkContext

object DataClean_Object {


  def convertCol1toInt(s : String): AnyVal ={
    val a2 = s match {
      case " ?" =>
      case " Federal-gov" => 1
      case " Local-gov" => 2
      case " Never-worked" => 3
      case " Private" => 4
      case " Self-emp-inc" => 5
      case " Self-emp-not-inc" => 6
      case " State-gov" => 7
      case " Without-pay" => 8

    }
    a2
  }

  def convertCol4toInt(s : String): Double ={
    val a2 = s match {
      case " 10th" => 0
      case " 11th" => 1
      case " 12th" => 2
      case " 1st-4th" => 3
      case " 5th-6th" => 4
      case " 7th-8th" => 5
      case " 9th" => 6
      case " Assoc-acdm" => 7
      case " Assoc-voc" => 8
      case " Bachelors" => 9
      case " Doctorate" => 10
      case " HS-grad" => 11
      case " Masters" => 12
      case " Preschool" => 13
      case " Prof-school" => 14
      case " Some-college" => 15

    }
    a2
  }

  def convertCol6toInt(s : String): Double ={
    val a2 = s match {
      case " Divorced" => 0
      case " Married-AF-spouse" => 1
      case " Married-civ-spouse" => 2
      case " Married-spouse-absent" => 3
      case " Never-married" => 4
      case " Separated" => 5
      case " Widowed" => 6
    }
    a2
  }

  def convertCol7toInt(s : String): AnyVal ={
    val a2 = s match {
      case " ?" =>
      case " Adm-clerical" => 1
      case " Armed-Forces" => 2
      case " Craft-repair" => 3
      case " Exec-managerial" => 4
      case " Farming-fishing" => 5
      case " Handlers-cleaners" => 6
      case " Machine-op-inspct" => 7
      case " Other-service" => 8
      case " Prof-specialty" => 9
      case " Protective-serv" => 10
      case " Sales" => 11
      case " Tech-support" => 12
      case " Transport-moving" => 13
      case " Priv-house-serv" => 14

    }
    a2
  }

  def convertCol8toInt(s : String): Double ={
    val a2 = s match {
      case " Husband" => 0
      case " Not-in-family" => 1
      case " Other-relative" => 2
      case " Own-child" => 3
      case " Unmarried" => 4
      case " Wife" => 5

    }
    a2
  }


  def convertCol9toInt(s : String): Double ={
    val a2 = s match {
      case " Amer-Indian-Eskimo" => 0
      case " Asian-Pac-Islander" => 1
      case " Black" => 2
      case " Other" => 3
      case " White" => 4

    }
    a2
  }


  def convertCol10toInt(s : String): Double ={
    val a2 = s match {
      case " Female" => 0
      case " Male" => 1

    }
    a2
  }

  def convertCol14toInt(s : String): AnyVal ={
    val a2 = s match {
      case " ?" =>
      case " Cambodia" => 1
      case " Canada" => 2
      case " China" => 3
      case " Columbia" => 4
      case " Cuba" => 5
      case " Dominican-Republic" => 6
      case " Ecuador" => 7
      case " El-Salvador" => 8
      case " England" => 9
      case " France" => 10
      case " Germany" => 11
      case " Greece" => 12
      case " Guatemala" => 13
      case " Haiti" => 14
      case " Holand-Netherlands" => 15
      case " Honduras" => 16
      case " Hong" => 17
      case " Hungary" => 18
      case " India" => 19
      case " Iran" => 20
      case " Ireland" => 21
      case " Italy" => 22
      case " Jamaica" => 23
      case " Japan" => 24
      case " Laos" => 25
      case " Mexico" => 26
      case " Nicaragua" => 27
      case " Outlying-US(Guam-USVI-etc)" => 28
      case " Peru" => 29
      case " Philippines" => 30
      case " Poland" => 31
      case " Portugal" => 32
      case " Puerto-Rico" => 33
      case " Scotland" => 34
      case " South" => 35
      case " Taiwan" => 36
      case " Thailand" => 37
      case " Trinadad&Tobago" => 38
      case " United-States" => 39
      case " Vietnam" => 40
      case " Yugoslavia" => 41

    }
    a2
  }

  def convertCol15toInt(s : String): Double ={
    val a2 = s match {
      case " <=50K" => 0
      case " >50K" => 1

    }
    a2
  }


  def cleanData(i : String): String ={
    val a1 = i.split(",")
    val b1 : List[AnyVal] = List(a1(0).toDouble,convertCol1toInt(a1(1)),a1(2).toDouble,convertCol4toInt(a1(3)),a1(4).toDouble,
      convertCol6toInt(a1(5)),convertCol7toInt(a1(6)),convertCol8toInt(a1(7)),convertCol9toInt(a1(8)),convertCol10toInt(a1(9)),
      a1(10).toDouble,a1(11).toDouble,a1(12).toDouble,
     convertCol14toInt(a1(13)),convertCol15toInt(a1(14)))

    b1.mkString(",")
  }

  def main(args:Array[String]) {

    val sc = new SparkContext("local[1]", "Midterm")
    val a0 = sc.textFile("/Users/prateekgangwal/Downloads/adult.data.txt")
    val b0 = a0.map(i => cleanData(i))

    b0.saveAsTextFile("/Users/prateekgangwal/Downloads/adult_New_Data_1")
  }

}