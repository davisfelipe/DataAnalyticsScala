import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, Tokenizer,StopWordsRemover}


def load(path: String, sqlContext: SQLContext, featuresArr: String*): DataFrame = {
    var data = sqlContext.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter","\t")
    .load(path)
    .toDF(featuresArr: _*)
    return data
  }
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var train_data = load("Data/train.tsv",sqlContext,"id","nombre","estado","categoria","marca","precio","envio","descripcion").cache()
train_data=train_data.withColumn("descripcion",regexp_replace($"descripcion","[\"\\.]","")).withColumn("descripcion",lower($"descripcion"))
train_data=train_data.withColumn("tmp", split($"categoria","\\/"))
.withColumn("categoria1", $"tmp".getItem(0))
.withColumn("categoria2", $"tmp".getItem(1))
.withColumn("categoria3", $"tmp".getItem(2))
.drop("tmp")
val descripciontok = new Tokenizer()
.setInputCol("descripcion")
.setOutputCol("desctk")
val datan=descripciontok.transform(train_data).drop("tmp")
val remover = new StopWordsRemover()
.setInputCol("desctk")
.setOutputCol("contado")
train_data=remover.transform(datan).drop("desctk")
