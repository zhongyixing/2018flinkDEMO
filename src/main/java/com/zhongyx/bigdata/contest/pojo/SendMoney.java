package com.zhongyx.bigdata.contest.pojo;

import java.util.ArrayList;
import java.util.List;

public class SendMoney {
	private String idenNo;
	
	private String dept;
	
	private String moneyType;
	
	private Integer year;
	
	private Integer gender;
	
	private Long sendTime;
	
	private Double sendMoney;
	
	private String exType;
	
	private List<String> exList=new ArrayList<String>();

	public String getIdenNo() {
		return idenNo;
	}

	public void setIdenNo(String idenNo) {
		this.idenNo = idenNo;
	}

	public String getDept() {
		return dept;
	}

	public void setDept(String dept) {
		this.dept = dept;
	}

	public String getMoneyType() {
		return moneyType;
	}

	public void setMoneyType(String moneyType) {
		this.moneyType = moneyType;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getGender() {
		return gender;
	}

	public void setGender(Integer gender) {
		this.gender = gender;
	}

	public Long getSendTime() {
		return sendTime;
	}

	public void setSendTime(Long sendTime) {
		this.sendTime = sendTime;
	}

	public Double getSendMoney() {
		return sendMoney;
	}

	public void setSendMoney(Double sendMoney) {
		this.sendMoney = sendMoney;
	}
	
	public String getExType() {
		return exType;
	}

	public void setExType(String exType) {
		this.exType = exType;
	}

	public SendMoney() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	

	public List<String> getExList() {
		return exList;
	}

	public void setExList(List<String> exList) {
		this.exList = exList;
	}

	public SendMoney(String idenNo, String dept, String moneyType, Integer year, Integer gender, Long sendTime,
			Double sendMoney, String exType) {
		super();
		this.idenNo = idenNo;
		this.dept = dept;
		this.moneyType = moneyType;
		this.year = year;
		this.gender = gender;
		this.sendTime = sendTime;
		this.sendMoney = sendMoney;
		this.exType = exType;
	}

	public SendMoney(String idenNo, String dept, String moneyType, Integer year, Integer gender, Long sendTime,
			Double sendMoney, String exType, List<String> exList) {
		super();
		this.idenNo = idenNo;
		this.dept = dept;
		this.moneyType = moneyType;
		this.year = year;
		this.gender = gender;
		this.sendTime = sendTime;
		this.sendMoney = sendMoney;
		this.exType = exType;
		this.exList = exList;
	}



	
	
}
