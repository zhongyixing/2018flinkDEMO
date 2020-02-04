package com.zhongyx.bigdata.contest.demo5;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.zhongyx.bigdata.contest.pojo.BreakMan;
import com.zhongyx.bigdata.contest.pojo.DeadMan;
import com.zhongyx.bigdata.contest.pojo.MasterMan;
import com.zhongyx.bigdata.contest.pojo.RelationMan;
import com.zhongyx.bigdata.contest.pojo.SendMoney;

public class Demo5 {
	

	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//读入文本文件
		DataSet<String> breakMan=env.readTextFile("E:\\资料\\高新区比赛\\2018\\input\\残疾人信息.csv");
		
		DataSet<String> relation=env.readTextFile("E:\\资料\\高新区比赛\\2018\\input\\公职人员及家属信息.csv");
		
		DataSet<String> deadMan=env.readTextFile("E:\\资料\\高新区比赛\\2018\\input\\死亡人口信息.csv");
		
		DataSet<String> sendMoney=env.readTextFile("E:\\资料\\高新区比赛\\2018\\input\\惠民资金发放信息.csv");
			
		DataSet<BreakMan> breakManData=breakMan.map(new MapFunction<String,BreakMan>() {

			@Override
			public BreakMan map(String line) throws Exception {
				
				String[] fields = line.split("\t");
				
				// 3 缓存数据到集合
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
				return new BreakMan(fields[0], sdf.parse(fields[1]).getTime());
			}
		})
		.groupBy("idenNo")
		.reduceGroup(new GroupReduceFunction<BreakMan,BreakMan>() {

			@Override
			public void reduce(Iterable<BreakMan> values, Collector<BreakMan> out) throws Exception {
				String idenNo="";
				Long sendTime=Long.MIN_VALUE;
				
				for(BreakMan value:values)
				{
					idenNo=value.getIdenNo();
					
					//发放时间取最大
					if(value.getSendTime()>sendTime)
					{
						sendTime=value.getSendTime();
					}
				}		
				out.collect(new BreakMan(idenNo, sendTime));
				
			}
		});
		
		
		DataSet<MasterMan> masterManData=relation.filter(new FilterFunction<String>() {
			
			@Override
			public boolean filter(String line) throws Exception {
				
				String[] fields = line.split("\t");
				
				//返回true代表保留，第四个字段是户主的数据
				return (fields.length==4 && fields[3].equals("户主") );
			}
		}).
		map(new MapFunction<String,MasterMan>() {

			@Override
			public MasterMan map(String line) throws Exception {
				
				String[] fields = line.split("\t");
				
				return new MasterMan(fields[2],fields[3]);

			}
		})
		.groupBy("idenNo")
		.reduceGroup(new GroupReduceFunction<MasterMan,MasterMan>() {

			@Override
			public void reduce(Iterable<MasterMan> values, Collector<MasterMan> out) throws Exception {
				
				
				String idenNo="";
				String relationType="";
				
				for(MasterMan value:values)
				{
					idenNo=value.getIdenNo();
					relationType=value.getRelationType();
				}		
				out.collect(new MasterMan(idenNo, relationType));
			}
		});
		
		
		
		
		DataSet<RelationMan> relationManData=relation.filter(new FilterFunction<String>() {
			
			@Override
			public boolean filter(String line) throws Exception {
				String[] fields = line.split("\t");
				
				//返回true代表保留，第四个字段不是户主的数据，或一共只有3个字段的数据
				return (fields.length==4 && !fields[3].equals("户主")) || fields.length==3;
				
			}
		}).
		map(new MapFunction<String,RelationMan>() {

			@Override
			public RelationMan map(String line) throws Exception {
				// 2 切割
				String[] fields = line.split("\t");
				
				if(fields.length==4)
				{
					return new RelationMan(fields[2],fields[3]);
				}
				else
				{
					return new RelationMan(fields[2],"");
				}
			}
		})
		.groupBy("idenNo")
		.reduceGroup(new GroupReduceFunction<RelationMan,RelationMan>() {

			@Override
			public void reduce(Iterable<RelationMan> values, Collector<RelationMan> out) throws Exception {
				
				
				String idenNo="";
				String relationType="";
				
				for(RelationMan value:values)
				{
					idenNo=value.getIdenNo();
					relationType=value.getRelationType();
				}		
				out.collect(new RelationMan(idenNo, relationType));
			}
		});
		
		DataSet<DeadMan> deadManData=deadMan.map(new MapFunction<String,DeadMan>() {

			@Override
			public DeadMan map(String line) throws Exception {
				// 2 切割
				String[] fields = line.split("\t");
				
				// 3 缓存数据到集合
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
				return new DeadMan(fields[0], sdf.parse(fields[1]).getTime());
			}
		}).
		groupBy("idenNo")
		.reduceGroup(new GroupReduceFunction<DeadMan,DeadMan>() {

			@Override
			public void reduce(Iterable<DeadMan> values, Collector<DeadMan> out) throws Exception {
				String idenNo="";
				Long deadTime=Long.MAX_VALUE;
				
				for(DeadMan value:values)
				{
					idenNo=value.getIdenNo();
					
					//死亡时间取最小
					if(value.getDeadTime()<deadTime)
					{
						deadTime=value.getDeadTime();
					}
				}		
				out.collect(new DeadMan(idenNo, deadTime));
			}
		});
		
		
		DataSet<SendMoney> sendMoneyData=sendMoney.map(new MapFunction<String,SendMoney>() {

			@Override
			public SendMoney map(String line) throws Exception {
				
				String[] columns=line.split("\t");
				
				String idenNo=columns[0];
				
				String dept=columns[1];
				
				String moneyType=columns[2];
				
				Integer gender=Integer.valueOf(columns[3]);
				
				String sendDate=columns[4];
				
				Integer year=Integer.valueOf(columns[4].substring(0,4));
				
				Double sendMoney=Double.valueOf(columns[5]);
				
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
				
				Long sendTime=sdf.parse(sendDate).getTime();
				
				return new SendMoney(idenNo, dept, moneyType, year, gender, sendTime, sendMoney,"");
			}
		}).leftOuterJoin(deadManData)
			.where("idenNo")
			.equalTo("idenNo")
			.with(new JoinFunction<SendMoney, DeadMan,SendMoney>() {

				@Override
				public SendMoney join(SendMoney left, DeadMan right) throws Exception {

					String idenNo=left.getIdenNo();
					String dept=left.getDept();
					String moneyType=left.getMoneyType();
					Integer year=left.getYear();
					Integer gender=left.getGender();
					Double sendMoney=left.getSendMoney();
					Long sendTime=left.getSendTime();
					String exType=left.getExType();
					List<String> exList=left.getExList();
					
					if(right!=null && right.getDeadTime()+(1000*60*60*24*180)>sendTime)
					{
						exList.add("死亡人群");
					}

					return new SendMoney(idenNo, dept, moneyType, year, gender, sendTime, sendMoney,exType,exList);
				}
				
				
			})
		.leftOuterJoin(breakManData)
			.where("idenNo")
			.equalTo("idenNo")
			.with(new JoinFunction<SendMoney, BreakMan,SendMoney>() {

				@Override
				public SendMoney join(SendMoney left, BreakMan right) throws Exception {
					String idenNo=left.getIdenNo();
					String dept=left.getDept();
					String moneyType=left.getMoneyType();
					Integer year=left.getYear();
					Integer gender=left.getGender();
					Double sendMoney=left.getSendMoney();
					Long sendTime=left.getSendTime();
					String exType=left.getExType();
					List<String> exList=left.getExList();
					
					if(dept.equals("残联") && right==null)
					{
						exList.add("残疾人群");
					}
					
					return new SendMoney(idenNo, dept, moneyType, year, gender, sendTime, sendMoney,exType,exList);
				}
				
			})
		.leftOuterJoin(masterManData)
			.where("idenNo")
			.equalTo("idenNo")
			.with(new JoinFunction<SendMoney, MasterMan,SendMoney>() {

				@Override
				public SendMoney join(SendMoney left, MasterMan right) throws Exception {
					String idenNo=left.getIdenNo();
					String dept=left.getDept();
					String moneyType=left.getMoneyType();
					Integer year=left.getYear();
					Integer gender=left.getGender();
					Double sendMoney=left.getSendMoney();
					Long sendTime=left.getSendTime();
					String exType=left.getExType();
					List<String> exList=left.getExList();
					
					if(right!=null)
					{
						exList.add("公职人群");
					}

					return new SendMoney(idenNo, dept, moneyType, year, gender, sendTime, sendMoney,exType,exList);
					
				}
			})
		.leftOuterJoin(relationManData)
		.where("idenNo")
		.equalTo("idenNo")
		.with(new JoinFunction<SendMoney, RelationMan,SendMoney>() {

			@Override
			public SendMoney join(SendMoney left, RelationMan right) throws Exception {
				String idenNo=left.getIdenNo();
				String dept=left.getDept();
				String moneyType=left.getMoneyType();
				Integer year=left.getYear();
				Integer gender=left.getGender();
				Double sendMoney=left.getSendMoney();
				Long sendTime=left.getSendTime();
				String exType=left.getExType();
				List<String> exList=left.getExList();
				
				if(dept.equals("民政") && right!=null)
				{
					exList.add("公职亲属人群");
				}
				
				return new SendMoney(idenNo, dept, moneyType, year, gender, sendTime, sendMoney,exType,exList);
			}
		})
		.filter(new FilterFunction<SendMoney>() {
			@Override
			public boolean filter(SendMoney value) throws Exception {
				
				//返回true代表保留，异常类型大于0
				return value.getExList().size()>0;
			}
		});
					
		DataSet<Tuple2<String,Double>> userMoney=sendMoneyData.map(new MapFunction<SendMoney,Tuple2<String,Double>>() {
			@Override
			public Tuple2<String, Double> map(SendMoney value) throws Exception {
				
				String moneyType=value.getMoneyType();
				String exDesc=StringUtils.join(value.getExList().toArray(),",");
				int year=value.getYear();
				int gender=value.getGender();
				String key="[" + moneyType + "-" + exDesc + "-" + year + "-" + gender+ "]";
				
				return new Tuple2<String, Double>(key, value.getSendMoney());
			}
		}).groupBy(0)
		.sum(1);
		
		DataSet<String> textData=userMoney.map(new MapFunction<Tuple2<String,Double>,String>() {

			@Override
			public String map(Tuple2<String,Double> value) throws Exception {
				// TODO Auto-generated method stub
				return value.f0+"\t"+String.valueOf(value.f1);
			}
		});
		
		textData.writeAsText("E:\\资料\\高新区比赛\\2018\\flinkOutput\\demo5").setParallelism(1);
		
		JobExecutionResult result=env.execute();
		
	}

}
