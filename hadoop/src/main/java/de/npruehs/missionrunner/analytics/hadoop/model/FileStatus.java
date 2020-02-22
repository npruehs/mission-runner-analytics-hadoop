package de.npruehs.missionrunner.analytics.hadoop.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FileStatus {
	private String pathSuffix;

	public String getPathSuffix() {
		return pathSuffix;
	}

	public void setPathSuffix(String pathSuffix) {
		this.pathSuffix = pathSuffix;
	}
}
