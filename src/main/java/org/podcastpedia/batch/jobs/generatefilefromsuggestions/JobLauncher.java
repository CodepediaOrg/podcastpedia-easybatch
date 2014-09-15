package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.easybatch.core.api.EasyBatchReport;
import org.easybatch.core.impl.EasyBatchEngine;
import org.easybatch.core.impl.EasyBatchEngineBuilder;
import org.easybatch.jdbc.JdbcRecordReader;

public class JobLauncher {

	public static void main(String[] args) throws Exception {
		
		 // create an embedded hsqldb in-memory database
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection connection = DriverManager.getConnection(System.getProperty("db.url"), System.getProperty("db.user"), System.getProperty("db.pwd"));
		 
		FileWriter fileWriter = new FileWriter(getOutputFilePath());
		 
		// Build an easy batch engine
		EasyBatchEngine easyBatchEngine = new EasyBatchEngineBuilder()
		.registerRecordReader(new JdbcRecordReader(connection, "SELECT * FROM ui_suggested_podcasts WHERE insertion_date >= STR_TO_DATE(\'" + args[0] + "\', \'%Y-%m-%d %h:%i\')" ))
		.registerRecordMapper(new CustomMapper())
		.registerRecordProcessor(new Processor(fileWriter))
		.build();
		 
		// Run easy batch engine
		EasyBatchReport easyBatchReport = easyBatchEngine.call();
		 
		//close file writer
		fileWriter.close();
		System.out.println(easyBatchReport);
	}

	private static String getOutputFilePath() throws Exception {
		//output csv file
		Date now = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(now);
		int kalendarWoche = calendar.get(Calendar.WEEK_OF_YEAR);
		String targetDirPath = System.getProperty("output.directory.base") + String.valueOf(kalendarWoche);
		File targetDirectory = new File(targetDirPath);
		if(!targetDirectory.exists()){
			boolean created = targetDirectory.mkdir();
			if(!created){
				throw new Exception("Target directory could not be created"); 
			}
		}
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm");
		String outputFileName = "suggestedPodcasts " + dateFormat.format(now) + ".in";
		String filePath = targetDirPath + "/" + outputFileName;
		return filePath;
	}

}
