package com.solace.cassandra;

import org.apache.log4j.Logger;

public class DeviceMain {
	static  Logger logger = Logger.getLogger(DeviceMain.class);
	public static void main(String[] args) {
		logger.info("**************Starting the Device Diagnostic Tool ******************");
		ConsumerDump app = new ConsumerDump();
		app.run(args);
	}
}
