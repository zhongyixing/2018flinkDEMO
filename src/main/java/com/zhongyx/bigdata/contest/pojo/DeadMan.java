package com.zhongyx.bigdata.contest.pojo;

public class DeadMan {
	private String idenNo;
	
	private Long deadTime;

	public String getIdenNo() {
		return idenNo;
	}

	public void setIdenNo(String idenNo) {
		this.idenNo = idenNo;
	}

	public Long getDeadTime() {
		return deadTime;
	}

	public void setDeadTime(Long deadTime) {
		this.deadTime = deadTime;
	}

	public DeadMan() {
		super();
		// TODO Auto-generated constructor stub
	}

	public DeadMan(String idenNo, Long deadTime) {
		super();
		this.idenNo = idenNo;
		this.deadTime = deadTime;
	}

	
	
}
