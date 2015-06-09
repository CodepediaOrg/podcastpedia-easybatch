package org.podcastpedia.batch.jobs.generatefilefromsuggestions;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.easybatch.core.api.Report;
import org.easybatch.core.api.Status;
import org.easybatch.core.impl.Engine;
import org.easybatch.core.impl.EngineBuilder;
import org.easybatch.jdbc.JdbcRecordReader;

public class JobLauncher {

	private static final String OUTPUT_FILE_HEADER = "#FEED_URL; IDENTIFIER_ON_PODCASTPEDIA; CATEGORIES; LANGUAGE; MEDIA_TYPE; UPDATE_FREQUENCY; KEYWORDS; FB_PAGE; TWITTER_PAGE; GPLUS_PAGE; NAME_SUBMITTER; EMAIL_SUBMITTER";

	public static void main(String[] args) throws Exception {
		
		 //connect to MySql Database 
		Class.forName("com.mysql.jdbc.Driver").newInstance();

		Properties configProperties = getConfigProperties();
		Connection connection = DriverManager.getConnection(configProperties.getProperty("db.url"), configProperties.getProperty("db.user"), configProperties.getProperty("db.pwd"));


		String directoryPath = System.getProperty("output.directory.base") + "/" + System.getProperty("profile");
		File lastAddPodcastsJobRunFile = getLastAddPodcastsJobRunFile(directoryPath);
		String dateOfLatestAddPodcastsJobRun = getDateOfLatestAddPodcastsJobRun(lastAddPodcastsJobRunFile);

		FileWriter fileWriter = new FileWriter(getOutputFilePath());
		fileWriter.write(OUTPUT_FILE_HEADER + "\n");

		// Build an easy batch engine
		Engine engine = new EngineBuilder()
			.reader(new JdbcRecordReader(connection, "SELECT * FROM ui_suggested_podcasts WHERE insertion_date >= STR_TO_DATE(\'" + dateOfLatestAddPodcastsJobRun + "\', \'%Y-%m-%d %H.%i\')" ))
			.mapper(new CustomMapper())
			.processor(new Processor(fileWriter))
			.build();
		 
		// Run easy batch engine
		Report report = engine.call();
		 
		//close file writer
		fileWriter.close();

		boolean jobRunFinishedAndPreviousFileIsExistent = report.getStatus() == Status.FINISHED && lastAddPodcastsJobRunFile != null;
		if(jobRunFinishedAndPreviousFileIsExistent){
			//move previous file to archive folder
			moveOldFileToArchive(directoryPath, lastAddPodcastsJobRunFile);
		}

		System.out.println(report);
	}

	private static void moveOldFileToArchive(String directoryPath, File lastAddPodcastsJobRunFile) throws IOException {
		String oldPath = lastAddPodcastsJobRunFile.getPath();

		Files.move(lastAddPodcastsJobRunFile.toPath(), FileSystems.getDefault().getPath(directoryPath + "/archive/" + lastAddPodcastsJobRunFile.getName()));
	}

	/**
	 * Returns the file that was to add podcasts to the database the last time (there must only one) and if not existent a null.
	 *
	 * @param directoryPath
	 * @return
	 */
	private static File getLastAddPodcastsJobRunFile(String directoryPath) {

		File fl = new File(directoryPath);

		File[] files = fl.listFiles(new FileFilter() {
			@Override
			public boolean accept(File file) {
				return file.isFile();
			}
		});

		if(files.length == 0) return null;
		else if(files.length > 1)
			throw new RuntimeException("There must no more than one file present in the folder to be archived");

		return files[0];
	}

	private static Properties getConfigProperties() throws IOException {
		String profile = System.getProperty("profile");
		Properties configProperties = new Properties();
		String configPropertiesFileName = "config." + profile + ".properties";
		InputStream inputStream = JobLauncher.class.getClassLoader().getResourceAsStream(configPropertiesFileName);

		configProperties.load(inputStream);
		return configProperties;
	}

	/**
	 * Returns the date in string format of the last job run to insert submitted podcasts
	 * If no file is present (can be the case initially), a default -10 days in the past is considered.
	 *
	 * @param lastAddPodcastsJobRunFile
	 * @return
	 */
	private static String getDateOfLatestAddPodcastsJobRun(File lastAddPodcastsJobRunFile) {

		String response = null;

		String directoryPath = System.getProperty("output.directory.base") + "/" + System.getProperty("profile");
		File fl = new File(directoryPath);

		if(lastAddPodcastsJobRunFile == null) {
			//if there are no files in directory yet, go back 10 days
			long defaultDate = new Date().getTime() - 10 * 24 * 60 * 60 * 1000;
			Date date = new Date(defaultDate);
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm");

			response = dateFormat.format(date);
		} else {
			String fileName = lastAddPodcastsJobRunFile.getName();
			response = fileName.substring(0, fileName.indexOf("k"));// file name samples 2015-06-01 10.20kw23.csv
		}

		System.out.println("****************** " + response);
		return response;
	}

	/**
	 * Returns the path where the new file is to be created
	 *
	 * @return
	 * @throws Exception
	 */
	private static String getOutputFilePath() throws Exception {

		Date now = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(now);
		int weekNumber = calendar.get(Calendar.WEEK_OF_YEAR);

		String targetDirPath = System.getProperty("output.directory.base");
		File targetDirectory = new File(targetDirPath);
		if(!targetDirectory.exists()){
			boolean created = targetDirectory.mkdir();
			if(!created){
				throw new Exception("Target directory could not be created"); 
			}
		}
		
		//build the file name based on current time to be placed in the "weeknum" directory  
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm");
		String outputFileName = dateFormat.format(now);
		
		String filePath = targetDirPath + "/" + System.getProperty("profile") + "/" + outputFileName + "kw " + String.valueOf(weekNumber) + ".csv";
		return filePath;
	}

}
