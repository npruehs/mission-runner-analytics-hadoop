package de.npruehs.missionrunner.analytics.hadoop.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FileStatuses {
	private FileStatus[] fileStatus;

	public FileStatus[] getFileStatus() {
		return fileStatus;
	}

	@JsonSetter("FileStatus")
	public void setFileStatus(FileStatus[] fileStatus) {
		this.fileStatus = fileStatus;
	}
	
}
