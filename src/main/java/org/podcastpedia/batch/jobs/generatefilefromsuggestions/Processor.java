package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.io.FileWriter;

import org.easybatch.core.api.RecordProcessor;

public class Processor implements RecordProcessor<SuggestedPodcast, SuggestedPodcast>{

	 private FileWriter fileWriter;
	 
	 public Processor(FileWriter fileWriter) {
		 this.fileWriter = fileWriter;
	 }
	
	@Override
	public SuggestedPodcast processRecord(SuggestedPodcast record) throws Exception {
		fileWriter.write(record.getMetadataLine() + "\n");
		fileWriter.flush();
		return record;
	}

}
