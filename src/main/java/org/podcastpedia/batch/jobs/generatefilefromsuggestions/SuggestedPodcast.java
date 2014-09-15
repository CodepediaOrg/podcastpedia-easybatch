package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.util.Date;

public class SuggestedPodcast {

	/** metadata containing all the necessary data required for insertion via the batch job addNewPodcast */
	String metadataLine;
	
	Date insertionDate;

	public String getMetadataLine() {
		return metadataLine;
	}

	public void setMetadataLine(String metadataLine) {
		this.metadataLine = metadataLine;
	}

	public Date getInsertionDate() {
		return insertionDate;
	}

	public void setInsertionDate(Date insertionDate) {
		this.insertionDate = insertionDate;
	}
		
}
