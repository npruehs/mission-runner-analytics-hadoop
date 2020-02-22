package de.npruehs.missionrunner.analytics.hadoop.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ListStatusResponse {
	private FileStatuses fileStatuses;

	public FileStatuses getFileStatuses() {
		return fileStatuses;
	}

	@JsonSetter("FileStatuses")
	public void setFileStatuses(FileStatuses fileStatuses) {
		this.fileStatuses = fileStatuses;
	}
}
