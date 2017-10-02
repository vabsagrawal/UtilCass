package com.vabs.validation;

import java.util.UUID;

public class ScenarioStatus {

	UUID scenarioId;
	String creatorId;
	int itemCnt;
	int status;
	String type;
	
	public UUID getScenarioId() {
		return scenarioId;
	}
	public void setScenarioId(UUID scenarioId) {
		this.scenarioId = scenarioId;
	}
	public String getCreatorId() {
		return creatorId;
	}
	public void setCreatorId(String creatorId) {
		this.creatorId = creatorId;
	}
	public int getItemCnt() {
		return itemCnt;
	}
	public void setItemCnt(int itemCnt) {
		this.itemCnt = itemCnt;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	
}
