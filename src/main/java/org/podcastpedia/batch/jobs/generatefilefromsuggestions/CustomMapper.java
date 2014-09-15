package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.sql.ResultSet;

import io.github.benas.easybatch.core.api.Record;
import io.github.benas.easybatch.core.api.RecordMapper;
import io.github.benas.easybatch.jdbc.JdbcRecord;

public class CustomMapper implements RecordMapper<SuggestedPodcast>{

	@SuppressWarnings("rawtypes")
	@Override
	public SuggestedPodcast mapRecord(Record record) throws Exception {
        JdbcRecord jdbcRecord = (JdbcRecord) record;
        ResultSet resultSet = jdbcRecord.getRawContent();
        
        SuggestedPodcast response = new SuggestedPodcast();
        response.setMetadataLine(resultSet.getString("metadata_line"));
        
		return response;
	}

}
