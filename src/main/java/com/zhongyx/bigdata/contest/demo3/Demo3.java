package com.zhongyx.bigdata.contest.demo3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class Demo3 {

	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> localLines=env.readTextFile("E:\\资料\\高新区比赛\\2018\\input\\惠民资金发放信息.csv");
		
		DataSet<Tuple2<String,Integer>> userMoney=localLines.map(new MapFunction<String,Tuple2<String,Double>>() {

			@Override
			public Tuple2<String, Double> map(String line) throws Exception {
			
				String[] columns=line.split("\t");
				
				String idenNo=columns[0];
				
				Double sendMoney=Double.valueOf(columns[5]);
				
				return new Tuple2<String, Double>(idenNo,sendMoney);
			}
		}).
		groupBy(0).
		sum(1).
		map(new MapFunction<Tuple2<String,Double>,Tuple2<String,Integer>>() {

			@Override
			public Tuple2<String,Integer> map(Tuple2<String, Double> value) throws Exception {
				// TODO Auto-generated method stub

				double money=value.f1;
				String type="";
				
				if(money<5000.00D)
				{
					type="低于五千人数";
				}
				else if(money>=5000.00D && money<30000.00D)
				{
					type="五千至三万人数";
				}
				else if(money>=30000.00D && money<50000.00D)
				{
					type="三至五万人数";
				}
				else if(money>=50000.00D && money<100000.00D)
				{
					type="五至十万人数";
				}
				else if(money>=100000.00D && money<200000.00D)
				{
					type="十至二十万人数";
				}
				else if(money>=200000.00D)
				{
					type="二十万以上人数";
				}
				
				return new Tuple2(type,1);
			}	
		}).
		groupBy(0).
		sum(1);
		
		DataSet<String> textData=userMoney.map(new MapFunction<Tuple2<String,Integer>,String>() {

			@Override
			public String map(Tuple2<String,Integer> value) throws Exception {
				// TODO Auto-generated method stub
				return value.f0+"\t"+String.valueOf(value.f1);
			}
		});
		
		textData.writeAsText("E:\\资料\\高新区比赛\\2018\\flinkOutput\\demo3").setParallelism(1);
		
		env.execute();
		
	}

}
