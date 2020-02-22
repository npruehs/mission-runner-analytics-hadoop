package de.npruehs.missionrunner.analytics.hadoop.model;

public class AnalyticsEventWithCount {
	private String eventId;
	private int count;
	
	public String getEventId() {
		return eventId;
	}
	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
}
