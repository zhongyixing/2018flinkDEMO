package com.zhongyx.bigdata.contest.pojo;

public class BreakMan {
	private String idenNo;
	
	private Long sendTime;

	public String getIdenNo() {
		return idenNo;
	}

	public void setIdenNo(String idenNo) {
		this.idenNo = idenNo;
	}

	public Long getSendTime() {
		return sendTime;
	}

	public void setSendTime(Long sendTime) {
		this.sendTime = sendTime;
	}

	public BreakMan() {
		super();
		// TODO Auto-generated constructor stub
	}

	public BreakMan(String idenNo, Long sendTime) {
		super();
		this.idenNo = idenNo;
		this.sendTime = sendTime;
	}

	
}
