package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.easybatch.core.api.Report;
import org.easybatch.core.impl.Engine;
import org.easybatch.core.impl.EngineBuilder;
import org.easybatch.jdbc.JdbcRecordReader;

public class JobLauncher {

	private static final String OUTPUT_FILE_HEADER = "FEED_URL; IDENTIFIER_ON_PODCASTPEDIA; CATEGORIES; LANGUAGE; MEDIA_TYPE; UPDATE_FREQUENCY; KEYWORDS; FB_PAGE; TWITTER_PAGE; GPLUS_PAGE; NAME_SUBMITTER; EMAIL_SUBMITTER";

	public static void main(String[] args) throws Exception {
		
		 //connect to MySql Database 
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection connection = DriverManager.getConnection(System.getProperty("db.url"), System.getProperty("db.user"), System.getProperty("db.pwd"));
		 
		FileWriter fileWriter = new FileWriter(getOutputFilePath());
		fileWriter.write(OUTPUT_FILE_HEADER + "\n"); 
		
		// Build an easy batch engine
		Engine engine = new EngineBuilder()
			.reader(new JdbcRecordReader(connection, "SELECT * FROM ui_suggested_podcasts WHERE insertion_date >= STR_TO_DATE(\'" + args[0] + "\', \'%Y-%m-%d %H:%i\')" ))
			.mapper(new CustomMapper())
			.processor(new Processor(fileWriter))
			.build();
		 
		// Run easy batch engine
		Report report = engine.call();
		 
		//close file writer
		fileWriter.close();
		System.out.println(report);
	}

	private static String getOutputFilePath() throws Exception {
		
		//create if not existent a "weeknum" directory in the given "output.directory.base" directory
		Date now = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(now);
		int weeknum = calendar.get(Calendar.WEEK_OF_YEAR);
		String targetDirPath = System.getProperty("output.directory.base") + String.valueOf(weeknum);		
		File targetDirectory = new File(targetDirPath);
		if(!targetDirectory.exists()){
			boolean created = targetDirectory.mkdir();
			if(!created){
				throw new Exception("Target directory could not be created"); 
			}
		}
		
		//build the file name based on current time to be placed in the "weeknum" directory  
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm");
		String outputFileName = "suggestedPodcasts " + dateFormat.format(now) + ".csv";
		
		String filePath = targetDirPath + "/" + outputFileName;		
		return filePath;
	}

}
