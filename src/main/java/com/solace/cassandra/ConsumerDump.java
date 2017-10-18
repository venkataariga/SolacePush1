package com.solace.cassandra;
//import com.datastax.driver.core.BoundStatement;
//import com.datastax.driver.core.PreparedStatement;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.sql.*;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.solacesystems.jcsmp.BytesXMLMessage;

import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageListener;

public class ConsumerDump implements XMLMessageListener {

	Logger logger = Logger.getLogger(ConsumerDump.class);

	
	Properties solProps = null;

	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	JCSMPSession session = null;

	File messagesFile = null;
	private Connection con;

	// XMLMessageListener
	public void onException(JCSMPException exception) {
		logger.error("Exception occured with XMLMessageListener " + exception);
		System.out.println("Exception occured with XMLMessageListener " + exception);
	}

	String rxdata = "";


	public void onReceive(BytesXMLMessage msg) {
		try {
			
			if (msg instanceof TextMessage) {
				rxdata = ((TextMessage) msg).getText();
			} else {
				byte[] messageBinaryPayload = msg.getAttachmentByteBuffer().array();
				rxdata = new String(messageBinaryPayload, "UTF-8");
			}
			logger.info("Data from Solace Box : " + rxdata);
			

			JSONArray array = null;
			try {
				array = new JSONArray(rxdata);
			} catch (Exception e) {
				logger.error(
						"******Message Failed due to CONVERSION rx datamsg to JSON Array :*******  and the data is  "
								+ rxdata);
		         msg.ackMessage();
				throw e;
			}
			String IMEI = "";
			logger.debug("Message Evt size : " + array.length());
			for (int i = 0; i < array.length(); i++) {

				JSONObject jsonObj = array.getJSONObject(i);
				
				String DATE="";
				String plmn="";
				String firmVersion = "";
				try {	DATE = jsonObj.getString("DT");
						plmn = jsonObj.getString("LI1");
						IMEI = jsonObj.getString("DI1");
						firmVersion = jsonObj.getString("DI5");
				    }
				catch(Exception e) {

					logger.error("******ONE OF THE FEILDS MISSING******"+ " stack Trace:" + e.getMessage()+"\n");
					if(e.getMessage().contains("DT")) {
						DATE = "NOT FOUND";
					}
					else if(e.getMessage().contains("LI1")){
						plmn = "NOT FOUND";
						
					}
					else if(e.getMessage().contains("DI1")){
						IMEI = "NOT FOUND";
						
					}
					else if(e.getMessage().contains("DI5")){
						firmVersion = "NOT FOUND";
						
					}
					
				}
				Timestamp sq = new Timestamp(System.currentTimeMillis());
				String query = " insert into dev (IMEI,DATE,plmn,firmVersion,timestamp)"+" values (?, ?, ?, ?, ?)";
				java.sql.PreparedStatement preparedStmt = con.prepareStatement(query);
			      preparedStmt.setString (1, IMEI);
			      preparedStmt.setString (2, DATE);
			      preparedStmt.setString (3, plmn);
			      preparedStmt.setString(4, firmVersion);
			      preparedStmt.setString(5, sq.toString());

			      // execute the preparedstatement
			      preparedStmt.execute();
			      
			      



			}
			 msg.ackMessage();

		} catch (UnsupportedEncodingException e) {
			 msg.ackMessage();
			System.err.println("******Message Failed while operating the parser:*******" + " and the data is  " + rxdata
					+ " stack Trace:" + e.getMessage());
		}

		catch (Exception ex) {
        	 msg.ackMessage();
			System.err.println("******Message Failed while operating the parser:*******" + " and the data is  " + rxdata
					+ " stack Trace:" + ex.getMessage());
		} 
//	
	}

	void createSession(String[] args) throws InvalidPropertiesException {

	    

				     
		logger.info("Initialzing the Solace & Mysql Connections");
	
		try {
			System.out.println("Loading solprops");
			solProps = ResourceLoader.getSolaceProperties();
			System.out.println("Loaded solprops");
			final JCSMPProperties properties = new JCSMPProperties();
 
			String uname = solProps.getProperty("mysql.uname");
			 String passwd = solProps.getProperty("mysql.passwd");
			 String url = solProps.getProperty("mysql.url");
			  System.out.println("Loaded these"+uname+passwd+url);
			  try{  
				    Class.forName("com.mysql.cj.jdbc.Driver");  
				    con=DriverManager.getConnection(url,uname,passwd);
				    System.out.println("Connected to mysql db!!");
				      
				    }
	    		catch(Exception e){ System.out.println(e.toString());}  
			
			properties.setProperty(JCSMPProperties.HOST, solProps.getProperty("solace.host"));
			properties.setProperty(JCSMPProperties.VPN_NAME, solProps.getProperty("solace.vpn"));
			properties.setProperty(JCSMPProperties.USERNAME, solProps.getProperty("solace.username"));
			properties.setProperty(JCSMPProperties.PASSWORD, solProps.getProperty("solace.password"));
			properties.setProperty(JCSMPProperties.MESSAGE_ACK_MODE, JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
			JCSMPChannelProperties cp = (JCSMPChannelProperties) properties
					.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);

			session = JCSMPFactory.onlyInstance().createSession(properties);

			logger.info("Solace Connection Established");
            String dirPath = solProps.getProperty("file.dir");
			messagesFile = new File(dirPath + df.format(new Date()) + ".csv");
//			writer = new FileWriter(messagesFile);
		} catch (Exception e) {
			logger.error("Unable to load the Solace Properties");
		}

	}

	public void run(String[] args) {
		FlowReceiver receiver = null;
		boolean blnRunFlag = true;
		try {
			// Create the Session.
			createSession(args);
			Properties solProps = ResourceLoader.getSolaceProperties();
			Queue queue = JCSMPFactory.onlyInstance().createQueue(solProps.getProperty("queue.name"));
			// Create and start the receiver.
			receiver = session.createFlow(queue, null, this);
			while (blnRunFlag) {
				receiver.start();
				// Thread.sleep(10);
			}
			// Close the receiver.
			receiver.close();
		} catch (JCSMPTransportException ex) {
			logger.error("JCSMPTransportException occured while creating the flow : " + ex.getMessage());
			if (receiver != null) {
				receiver.close();
				blnRunFlag = false;
			}

		} catch (JCSMPException ex) {
			logger.error("JCSMPException occured while creating the flow : " + ex.getMessage());
			if (receiver != null) {
				receiver.close();
				blnRunFlag = false;
			}

		} catch (Exception ex) {
			logger.error("Exception occured while creating the flow : " + ex.getMessage());
			blnRunFlag = false;

		}
	}

}
