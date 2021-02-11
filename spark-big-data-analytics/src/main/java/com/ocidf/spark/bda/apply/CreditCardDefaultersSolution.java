/****************************************************************************

                   Spark with Java

             Copyright : V2 Maestros @2016
                    
Solution for APPLY Project : Credit Card Defaulters Analysis

NOTE: This solution contains all code a part of a single class /main() function.
It has been built so for easy readability. For a proper solution though,
we recommend following OOP concepts and other Java best practices in real world.

*****************************************************************************/
package com.ocidf.spark.bda.apply;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.ocidf.spark.bda.common.ExerciseUtils;
import com.ocidf.spark.bda.common.SparkConnection;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

public class CreditCardDefaultersSolution {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		JavaSparkContext spContext = SparkConnection.getContext();
		SparkSession spSession = SparkConnection.getSession();
		
		/*--------------------------------------------------------------------------
		Carregar dados
		--------------------------------------------------------------------------*/
		Dataset<Row> ccRawDf = spSession.read()
				.option("header","true")
				.csv("data/credit-card-default-1000.csv");
		System.out.println("Raw Data : " );
		ccRawDf.show(5);
		ccRawDf.printSchema();
		
		/*--------------------------------------------------------------------------
		Limpar e transformar dados
		--------------------------------------------------------------------------*/
		
		//Remova as linhas que começam com aaaa*
		Dataset<Row> ccCleanedDf = ccRawDf.filter( not(col("CUSTID").startsWith("aaaaa") ) );
		
		//Crie o esquema para os dados a serem carregados no conjunto de dados.
		StructType ccSchema = DataTypes
				.createStructType(new StructField[] {
						DataTypes.createStructField("CustId", DataTypes.DoubleType, false),
						DataTypes.createStructField("LimitBal", DataTypes.DoubleType, false),
						DataTypes.createStructField("Sex", DataTypes.DoubleType, false),
						DataTypes.createStructField("Education", DataTypes.DoubleType, false),
						DataTypes.createStructField("Marriage", DataTypes.DoubleType, false),
						DataTypes.createStructField("Age", DataTypes.DoubleType, false),
						DataTypes.createStructField("AvgPayDur", DataTypes.DoubleType, false),
						DataTypes.createStructField("AvgBillAmt", DataTypes.DoubleType, false),
						DataTypes.createStructField("AvgPayAmt", DataTypes.DoubleType, false),
						DataTypes.createStructField("PerPaid", DataTypes.DoubleType, false),
						DataTypes.createStructField("Defaulted", DataTypes.DoubleType, false) 
					});

		//Alterar quadro de dados de volta para RDD
		JavaRDD<Row> rdd1 = ccCleanedDf.toJavaRDD().repartition(2);
		
		//Função para mapear.
		JavaRDD<Row> rdd2 = rdd1.map( new Function<Row, Row>() {

			@Override
			public Row call(Row iRow) throws Exception {
				
				//PR#06 - Rodada de idade até 10 anos
				Double age = Math.round(Double.valueOf(iRow.getString(5)) /10.0 ) * 10.0;
				
				//Normalize o sexo para 1 ou 2
				Double sex;
				switch( iRow.getString(2)) {
				case "M" : sex=1.0; break;
				case "F" : sex=2.0; break;
				default  : sex= Double.valueOf(iRow.getString(2));
				}
				
				//Encontre o valor médio faturado
				double avgBillAmt = Math.abs(( Double.valueOf(iRow.getString(12)) +
										Double.valueOf(iRow.getString(13)) +
										Double.valueOf(iRow.getString(14)) +
										Double.valueOf(iRow.getString(15)) +
										Double.valueOf(iRow.getString(16)) +
										Double.valueOf(iRow.getString(17))
											) / 6.0 );
				
				//Encontre o valor médio do pagamento
				double avgPayAmt = Math.abs(( Double.valueOf(iRow.getString(18)) +
										Double.valueOf(iRow.getString(19)) +
										Double.valueOf(iRow.getString(20)) +
										Double.valueOf(iRow.getString(21)) +
										Double.valueOf(iRow.getString(22)) +
										Double.valueOf(iRow.getString(23))
											) / 6.0 );
				
				//Encontre a duração média do pagamento
				double avgPayDuration = Math.round(( Math.abs(Double.valueOf(iRow.getString(6))) +
										Math.abs(Double.valueOf(iRow.getString(7))) +
										Math.abs(Double.valueOf(iRow.getString(8))) +
										Math.abs(Double.valueOf(iRow.getString(9))) +
										Math.abs(Double.valueOf(iRow.getString(10))) +
										Math.abs(Double.valueOf(iRow.getString(11)))
											) / 6.0);
				
				// Percentual médio pago. adicione isso como um campo adicional para ver
				// se esse campo tiver algum recurso preditivo. Isto é
				// trabalho criativo adicional que você faz para ver as possibilidades.                    
				double  perPay = Math.round((avgPayAmt/(avgBillAmt+1) * 100) / 25.0) * 25.0;

				Row retRow = RowFactory.create( Double.valueOf(iRow.getString(0)),
									Double.valueOf(iRow.getString(1)),
									sex,
									Double.valueOf(iRow.getString(3)),
									Double.valueOf(iRow.getString(4)),
									age,
									avgPayDuration,
									avgBillAmt,
									avgPayAmt,
									perPay,
									Double.valueOf(iRow.getString(24))
						);
				
				return retRow;
			}

		});
		
		//Crie o quadro de dados de volta.
		Dataset<Row> ccXformedDf1 = spSession.createDataFrame(rdd2, ccSchema);
		System.out.println("Transformed Data :");
		ccXformedDf1.show(5);
		
		//Adicione o nome do sexo para os dados necessários para PR#02
		List<Gender> gender = new ArrayList<Gender>();
		gender.add(new Gender(1.0, "Male"));
		gender.add(new Gender(2.0, "Female"));
		Dataset<Row> genderDf = spSession.createDataFrame(gender, Gender.class);
		Dataset<Row> ccXformedDf2 = ccXformedDf1
									.join(genderDf,  col("Sex").equalTo(col("sexId")))
									.drop(col("sexId"));
		
		//Adicione o nome da educação para os dados necessários para PR#03
		List<Education> education = new ArrayList<Education>();
		education.add(new Education(1.0, "Graduate"));
		education.add(new Education(2.0, "University"));
		education.add(new Education(3.0, "High School"));
		education.add(new Education(4.0, "Others"));
		Dataset<Row> educationDf = spSession.createDataFrame(education, Education.class);
		Dataset<Row> ccXformedDf3 = ccXformedDf2
									.join(educationDf,  col("Education").equalTo(col("eduId")))
									.drop(col("eduId"));
		
		//Adicione o nome do casamento para os dados necessários para PR#03
		List<Marriage> marriage = new ArrayList<Marriage>();
		marriage.add(new Marriage(1.0, "Single"));
		marriage.add(new Marriage(2.0, "Married"));
		marriage.add(new Marriage(3.0, "Others"));
		Dataset<Row> marriageDf = spSession.createDataFrame(marriage, Marriage.class);
		Dataset<Row> ccXformedDf4 = ccXformedDf3
									.join(marriageDf,  col("Marriage").equalTo(col("marriageId")))
									.drop(col("marriageId"));
		
		System.out.println("Transformed and Joined Data : ");
		ccXformedDf4.show(5);
		
		/*--------------------------------------------------------------------------
		Faça a análise conforme exigido pela declaração do problema
		--------------------------------------------------------------------------*/
		//Crie uma visualização temporária
		ccXformedDf4.createOrReplaceTempView("CCDATA");
		
		//PR#02 solução
		Dataset<Row> PR02 = spSession.sql("SELECT sexName, count(*) as Total, " + 
					                " SUM(Defaulted) as Defaults, " + 
					                " ROUND(SUM(Defaulted) * 100 / count(*)) as PerDefault " + 
					                " FROM CCDATA GROUP BY sexName" );
		System.out.println("Solution for PR#02 :");
		PR02.show();
		
		//PR#03 solução
		Dataset<Row> PR03 = spSession.sql("SELECT marriageName, eduName, count(*) as Total," +
					                " SUM(Defaulted) as Defaults, " + 
					                " ROUND(SUM(Defaulted) * 100 / count(*)) as PerDefault " + 
					                " FROM CCDATA GROUP BY marriageName, eduName " + 
					                " ORDER BY 1,2");
		System.out.println("Solution for PR#03 : ");
		PR03.show();
		
		//PR#04 solução
		Dataset<Row> PR04 = spSession.sql("SELECT AvgPayDur, count(*) as Total, " + 
					                " SUM(Defaulted) as Defaults, " + 
					                " ROUND(SUM(Defaulted) * 100 / count(*)) as PerDefault " + 
					                " FROM CCDATA GROUP BY AvgPayDur ORDER BY 1");
		System.out.println("Solution for PR#04 : ");
		PR04.show();
		
		//Faça análise de correlação
		for ( StructField field : ccSchema.fields() ) {
			if ( ! field.dataType().equals(DataTypes.StringType)) {
				System.out.println( "Correlation between Defaulted and " + field.name()
				 	+ " = " + ccXformedDf4.stat().corr("Defaulted", field.name()) );
			}
		}
		
		/*--------------------------------------------------------------------------
		Prepare-se para o Machine Learning
		--------------------------------------------------------------------------*/
		
		JavaRDD<Row> rdd3 = ccXformedDf4.toJavaRDD().repartition(2);
		
		JavaRDD<LabeledPoint> rdd4 = rdd3.map( new Function<Row, LabeledPoint>() {

			@Override
			public LabeledPoint call(Row iRow) throws Exception {
				
				Vector features =Vectors.dense( iRow.getDouble(2),
										iRow.getDouble(3),
										iRow.getDouble(4),
										iRow.getDouble(5),
										iRow.getDouble(6),
										iRow.getDouble(7),
										iRow.getDouble(8),
										iRow.getDouble(9)) ;
				
				// Usando customerID como etiqueta - um truque para adicionar o ID do cliente.
				// Adicionará o padrão posteriormente.
				LabeledPoint lp = new LabeledPoint(iRow.getDouble(0), features);
				
				return lp;
			}

		});

		Dataset<Row> ccLp = spSession.createDataFrame(rdd4, LabeledPoint.class);
		System.out.println("Labeled Point Data : ");
		ccLp.show(5);
		
		Dataset<Row> ccMap = ccXformedDf4.select(col("Custid"), col("Defaulted"));
		Dataset<Row> ccDefaultLp = ccLp.join(ccMap, col("label").equalTo(col("CustId")))
									.drop("label");

		System.out.println("Labeled Point with Defaulted and Customer ID");
		Dataset<Row> ccFinalLp = ccDefaultLp.repartition(2);
		ccFinalLp.show(5);
		
		// Faz indexação
		StringIndexer indexer = new StringIndexer()
				  .setInputCol("Defaulted")
				  .setOutputCol("indexed");
		
		StringIndexerModel siModel = indexer.fit(ccFinalLp);
		Dataset<Row> indexedCCLp = siModel.transform(ccFinalLp);

		// Divida os dados em conjuntos de treinamento e teste (30% realizados para teste).
		Dataset<Row>[] splits = indexedCCLp.randomSplit(new double[]{0.7, 0.3});
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];
		
		/*--------------------------------------------------------------------------
		Machine Learning - Classificação
		--------------------------------------------------------------------------*/
		//PR#05 Fazer previsões - para prever padrões. Use classificação múltipla
		// algoritmos para ver quais fornecem os melhores resultados

		// Avaliador de instalação
		MulticlassClassificationEvaluator evaluator 
			= new MulticlassClassificationEvaluator()
				  .setLabelCol("indexed")
				  .setPredictionCol("prediction")
				  .setMetricName("accuracy");

		// Converter rótulos indexados novamente em rótulos originais.
		IndexToString labelConverter = new IndexToString()
				  .setInputCol("indexed")
				  .setOutputCol("labelStr")
				  .setLabels(siModel.labels());
		
		IndexToString predConverter = new IndexToString()
				  .setInputCol("prediction")
				  .setOutputCol("predictionStr")
				  .setLabels(siModel.labels());		
		
		//Árvores de decisão **********
		DecisionTreeClassifier dt = new DecisionTreeClassifier()
				  .setLabelCol("indexed")
				  .setFeaturesCol("features");
		DecisionTreeClassificationModel dtModel = dt.fit(trainingData);
		//Prever em dados de teste
		Dataset<Row> dtRaw = dtModel.transform(testData);
		Dataset<Row> dtPredictions = predConverter.transform(
									labelConverter.transform(dtRaw));
		System.out.println("Decision Tree output : ");
		dtPredictions.show(5);
		double dtAccuracy = evaluator.evaluate(dtPredictions);
		System.out.println("Decision Trees Accuracy = " + Math.round( dtAccuracy * 100) + " %" );

		//Faça Random Forests **********
		RandomForestClassifier rf = new RandomForestClassifier()
				  .setLabelCol("indexed")
				  .setFeaturesCol("features");
		RandomForestClassificationModel rfModel = rf.fit(trainingData);
		//Prever em dados de teste
		Dataset<Row> rfRaw = rfModel.transform(testData);
		Dataset<Row> rfPredictions = predConverter.transform(
									labelConverter.transform(rfRaw));
		System.out.println("Random Forest output : ");
		rfPredictions.show(5);
		double rfAccuracy = evaluator.evaluate(rfPredictions);
		System.out.println("Random Forests Accuracy = " + Math.round( rfAccuracy * 100) + " %" );

		//Faça Naive Bayes **********
		NaiveBayes nb = new NaiveBayes()
				  .setLabelCol("indexed")
				  .setFeaturesCol("features");
		NaiveBayesModel nbModel = nb.fit(trainingData);
		//Prever em dados de teste
		Dataset<Row> nbRaw = nbModel.transform(testData);
		Dataset<Row> nbPredictions = predConverter.transform(
									labelConverter.transform(nbRaw));
		System.out.println("Naive Bayes output : ");
		nbPredictions.show(5);
		double nbAccuracy = evaluator.evaluate(nbPredictions);
		System.out.println("Naive Bayes Accuracy = " + Math.round( nbAccuracy * 100) + " %" );
		
		/*--------------------------------------------------------------------------
		Machine Learning - Clustering
		--------------------------------------------------------------------------*/
		//PR#06 Agrupe dados em 4 grupos com base nos parâmetros mencionados
		
		//Filtre apenas as colunas necessárias para o armazenamento em cluster
		Dataset<Row> ClustDf = ccXformedDf4.select(col("Sex"), col("Education"), 
									col("Marriage"), col("Age"), col("CustId"));
		
		System.out.println("Input Data for Clustering :");
		ClustDf.show(5);
		
		Row meanRow = ClustDf.agg(avg(ClustDf.col("Sex")), 
				avg(ClustDf.col("Education")),
				avg(ClustDf.col("Marriage")),
				avg(ClustDf.col("Age")))
		.toJavaRDD().takeOrdered(1).get(0)  ;
		
		Row stdRow = ClustDf.agg(avg(ClustDf.col("Sex")), 
						stddev(ClustDf.col("Education")),
						stddev(ClustDf.col("Marriage")),
						stddev(ClustDf.col("Age")))
				.toJavaRDD().takeOrdered(1).get(0)  ;
		
		System.out.println("Mean Values : " + meanRow);
		System.out.println("Std Dev Values : " + stdRow);
		
		Broadcast<Row> bcMeanRow = spContext.broadcast(meanRow);
		Broadcast<Row> bcStdRow = spContext.broadcast(stdRow);
		
		//Execute centro e escala e crie um vetor
		JavaRDD<Row> rdd5 = ClustDf.toJavaRDD().repartition(2);
		JavaRDD<LabeledPoint> rdd6 = rdd5.map( new Function<Row, LabeledPoint>() {
		
		@Override
		public LabeledPoint call(Row iRow) throws Exception {
			
			double sex = (bcMeanRow.value().getDouble(0) - iRow.getDouble(0))
							/ bcStdRow.value().getDouble(0);
			double education =  (bcMeanRow.value().getDouble(1) - iRow.getDouble(1))
							/ bcStdRow.value().getDouble(1);
			double marriage =  (bcMeanRow.value().getDouble(2) - iRow.getDouble(2))
							/ bcStdRow.value().getDouble(2);
			double age =  (bcMeanRow.value().getDouble(3) - iRow.getDouble(3))
							/ bcStdRow.value().getDouble(3);
			
			LabeledPoint lp = new LabeledPoint( iRow.getDouble(4),
					Vectors.dense( sex, education, marriage, age));
			
			return lp;
		}
		
		});
		
		Dataset<Row> ccVector = spSession.createDataFrame(rdd6, LabeledPoint.class );
		System.out.println("Centered and scaled vector :" + ccVector.count());
		ccVector.show(5);
		
		KMeans kmeans = new KMeans()
				.setK(4)
				.setSeed(1L);

		KMeansModel kMeansModel = kmeans.fit(ccVector);
		Dataset<Row> kMeansPredictions = kMeansModel.transform(ccVector);
		
		System.out.println("Groupings : ");
		kMeansPredictions.show(5);
		
		System.out.println("Groupings Summary : ");
		kMeansPredictions.groupBy(col("prediction")).count().show();

		
		// Mantenha o programa em execução para que possamos fazer o checkout.
		ExerciseUtils.hold();

	}

}
