package io.github.codetosurvive.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;

import io.github.codetosurvive.spark.conf.ConfigurationManager;
import io.github.codetosurvive.spark.constant.Constants;
import io.github.codetosurvive.spark.dao.ITaskDAO;
import io.github.codetosurvive.spark.dao.Task;
import io.github.codetosurvive.spark.dao.impl.DAOFactory;
import io.github.codetosurvive.spark.test.MockData;
import io.github.codetosurvive.spark.util.ParamUtils;
import scala.Tuple2;

/**
 * 用户访问session分析spark作业
 * 
 * @author lixiaojiao
 *
 */
public class UserVisitSessionAnalyzeSpark {

	public static void main(String[] args) {

		// 构建spark上下文
		SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// 模拟数据生成
		mockData(sc, sqlContext);

		// 创建task 的dao对象
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();

		// 根据task id获取任务的相关信息
		Long taskId = ParamUtils.getTaskIdFromArgs(args);
		Task task = taskDAO.findById(taskId);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

		// 首先根据日期选择符合条件的user_visit_action中的记录
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

		// 首先按照session id进行groupbykey进行分组，然后将session粒度的数据与用户信息进行join，
		// 然后就可以获取session粒度的数据了，同事还包含了session对应的user的信息

		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);
		sc.close();
	}

	/**
	 * 对行数据进行session粒度进行聚合
	 * 
	 * @param sqlContext
	 * @param actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {

		JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row t) throws Exception {
				return new Tuple2<String, Row>(t.getString(2), t);
			}
		});

		return null;
	}

	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {

		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

		String sql = "select * from user_visit_action where date>='" + startDate + "'" + " and date<='" + endDate + "'";

		DataFrame actoinDF = sqlContext.sql(sql);

		return actoinDF.javaRDD();
	}

	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			MockData.mock(sc, sqlContext);
		}
	}

	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}

}
