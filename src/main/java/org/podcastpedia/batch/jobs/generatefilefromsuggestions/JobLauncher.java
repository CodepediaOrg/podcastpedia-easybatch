package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import io.github.benas.easybatch.core.api.EasyBatchReport;
import io.github.benas.easybatch.core.impl.EasyBatchEngine;
import io.github.benas.easybatch.core.impl.EasyBatchEngineBuilder;
import io.github.benas.easybatch.jdbc.JdbcRecordReader;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;

public class JobLauncher {

	public static void main(String[] args) throws Exception {
		
		 // create an embedded hsqldb in-memory database
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3307/pcmDB?allowMultiQueries=true", "pcm", "pcm_pw");
		 
		//output csv file
		FileWriter fileWriter = new FileWriter("c:/tmp/foo.csv");
		 
		// Build an easy batch engine
		EasyBatchEngine easyBatchEngine = new EasyBatchEngineBuilder()
		.registerRecordReader(new JdbcRecordReader(connection, "select * from ui_suggested_podcasts"))
		.registerRecordMapper(new CustomMapper())
		.registerRecordProcessor(new Processor(fileWriter))
		.build();
		 
		// Run easy batch engine
		EasyBatchReport easyBatchReport = easyBatchEngine.call();
		 
		//close file writer
		fileWriter.close();
		System.out.println(easyBatchReport);
	}

}
