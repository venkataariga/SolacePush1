package com.solace.cassandra;

import org.apache.log4j.Logger;

public class AppMain {
	static Logger logger = Logger.getLogger(AppMain.class);
	public static void main(String[] args) {
		
		logger.info("**************Starting the App Diagnostic Tool ******************");
		ConsumerDump app = new ConsumerDump();
		app.run(args);
	}
}
