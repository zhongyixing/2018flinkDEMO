package com.zhongyx.bigdata.contest.demo2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class Demo2 {

	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> localLines=env.readTextFile("E:\\资料\\高新区比赛\\2018\\input\\惠民资金发放信息.csv");
		
		DataSet<Tuple2<String, Double>> yearMoney=localLines.map(new MapFunction<String,Tuple2<String,Double>>() {

			@Override
			public Tuple2<String, Double> map(String line) throws Exception {
			
				String[] columns=line.split("\t");
				
				String year=columns[4].substring(0,4);
				
				String dept=columns[1];
				
				Double sendMoney=Double.valueOf(columns[5]);
				
				return new Tuple2<String, Double>(year+"-"+dept,sendMoney);
			}
		}).
		groupBy(0).
		sum(1).
		sortPartition(0,Order.ASCENDING).setParallelism(1);
		
		
		DataSet<String> textData=yearMoney.map(new MapFunction<Tuple2<String,Double>,String>() {

			@Override
			public String map(Tuple2<String, Double> value) throws Exception {
				// TODO Auto-generated method stub
				return value.f0+"\t"+String.valueOf(value.f1);
			}
		});
		
		textData.writeAsText("E:\\资料\\高新区比赛\\2018\\flinkOutput\\demo2");
		
		env.execute();
		
	}

}
