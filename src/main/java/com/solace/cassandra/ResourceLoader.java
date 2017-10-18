package com.solace.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

public class ResourceLoader {

	private static Properties solProperties;
	
	public static synchronized Properties getSolaceProperties() {
		try {
			if (solProperties != null) {
				return solProperties;
			}

			InputStream inputStream = Thread.currentThread()
					.getContextClassLoader()
					.getResourceAsStream("solace.properties");
			solProperties = new Properties();
			solProperties.load(inputStream);
			inputStream.close();
		} catch (IOException ioe) {
		
			System.err.println("Failed to Solace APP Properties " +ioe.getMessage());
			
		}
		return solProperties;
			//TO LOAD WHEN RUNNING FROM JAR FILE ....
			/*solProperties = new Properties();
			System.out.println("Properties file found in path: "+getCurrentJarFilePath());
			File testEnvFile = new File(getCurrentJarFilePath()+"solace.properties");
			boolean fileExists=findFileExists(testEnvFile);
			InputStream testApplicatioInputStream = null;
			if(fileExists){
			 testApplicatioInputStream =new FileInputStream(testEnvFile);
			if(testApplicatioInputStream.available() > 0){
				solProperties.load(testApplicatioInputStream);
				testApplicatioInputStream.close();
			}
			}else{
				System.err.println("PROPERTIES FILES NOT FOUND");
				}
		} catch (IOException ioe) {
			System.err.println("Failed to APP Properties "+ ioe);
		}
		return solProperties;*/
		}
	
	
	

public static synchronized String getCurrentJarFilePath() {
String jarFilePath = "";
try {
	 URL url = ResourceLoader.class.getProtectionDomain().getCodeSource().getLocation(); //Gets the path
	  	String jarPath = null;
			try {
				jarPath = URLDecoder.decode(url.getFile(), "UTF-8"); //Should fix it to be read correctly by the system
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
	        jarFilePath = new File(jarPath).getParentFile().getPath(); //Path of the jar
	        jarFilePath = jarFilePath + File.separator;
}catch (Exception e) {
	System.err.println("Exception occurred while getting current jar file path"+ e);
}
return jarFilePath;
}

public static boolean findFileExists(File file){
boolean fileStatus=false;
try{
	if(file.exists())
	fileStatus=true;
}catch(Exception e){
	System.err.println("Exception occurred while finding file"+ e);
}
return fileStatus;

}

}
