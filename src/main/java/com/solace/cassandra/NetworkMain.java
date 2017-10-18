package com.solace.cassandra;

import org.apache.log4j.Logger;

public class NetworkMain {
	static  Logger logger = Logger.getLogger(NetworkMain.class);
	public static void main(String[] args) {
		logger.info("**************Starting the Network Diagnostic Tool ******************");
		ConsumerDump app = new ConsumerDump();
		app.run(args);
	}
}
