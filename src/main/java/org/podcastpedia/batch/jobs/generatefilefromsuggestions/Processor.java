package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.io.FileWriter;

import org.easybatch.core.api.AbstractRecordProcessor;

public class Processor extends AbstractRecordProcessor<SuggestedPodcast>{

	 private FileWriter fileWriter;
	 
	 public Processor(FileWriter fileWriter) {
		 this.fileWriter = fileWriter;
	 }
	
	@Override
	public void processRecord(SuggestedPodcast record) throws Exception {
		 fileWriter.write(record.getMetadataLine() + "\n");
		 fileWriter.flush();		
	}

}
