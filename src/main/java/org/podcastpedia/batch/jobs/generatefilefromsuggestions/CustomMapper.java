package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.sql.ResultSet;

import org.easybatch.core.api.Record;
import org.easybatch.core.api.RecordMapper;
import org.easybatch.jdbc.JdbcRecord;

public class CustomMapper implements RecordMapper<SuggestedPodcast>{

	@SuppressWarnings("rawtypes")
	@Override
	public SuggestedPodcast mapRecord(Record record) throws Exception {
        JdbcRecord jdbcRecord = (JdbcRecord) record;
        ResultSet resultSet = jdbcRecord.getPayload();
        
        SuggestedPodcast response = new SuggestedPodcast();
        response.setMetadataLine(resultSet.getString("metadata_line"));
        
		return response;
	}

}
