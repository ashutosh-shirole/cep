/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.fh.his.cep.jms;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JMSConnectionUtil
{
    private static Log log = LogFactory.getLog(JMSConnectionUtil.class);
    private static Properties properties = new Properties();
    private static JMSContext jmsCtx;

    static
    {
        log.info("Loading properties");
        try {
			InputStream propertiesIS = JMSConnectionUtil.class.getClassLoader().getResourceAsStream(JMSConstants.CONFIG_FILENAME);
			if (propertiesIS == null)
			{
			    throw new RuntimeException("Properties file '" + JMSConstants.CONFIG_FILENAME + "' not found in classpath");
			}
			properties.load(propertiesIS);
		} catch (IOException e) {
			log.error(e);
			e.printStackTrace();
		}

    }
    
    public static void sendEvent(Map<String, Object> msg, String destination) {
    	 // Connect to JMS
    	log.info("Connecting to JMS server");
        try {
			String factory = properties.getProperty(JMSConstants.JMS_CONTEXT_FACTORY);
			String jmsurl = properties.getProperty(JMSConstants.JMS_PROVIDER_URL);
			String connFactoryName = properties.getProperty(JMSConstants.JMS_CONNECTION_FACTORY_NAME);
			String user = properties.getProperty(JMSConstants.JMS_USERNAME);
			String password = properties.getProperty(JMSConstants.JMS_PASSWORD);
			boolean isTopic = true;
			JMSContext jmsCtx = JMSContextFactory.createContext(factory, jmsurl, connFactoryName, user, password, destination, isTopic);
			// Get producer
	        jmsCtx.getConnection().start();
	        MessageProducer producer = jmsCtx.getSession().createProducer(jmsCtx.getDestination());
	        ObjectMessage oMsg = jmsCtx.getSession().createObjectMessage((Serializable) msg);
	        producer.send(oMsg);
	        jmsCtx.destroy();
		} catch (JMSException e) {
			e.printStackTrace();
			log.error(e);
		} catch (NamingException e) {
			e.printStackTrace();
			log.error(e);
		}
    }
    
    public static void recieveEvent(String destination, MessageListener[] listeners) {
    	try {
			String factory = properties.getProperty(JMSConstants.JMS_CONTEXT_FACTORY);
			String jmsurl = properties.getProperty(JMSConstants.JMS_PROVIDER_URL);
			String connFactoryName = properties.getProperty(JMSConstants.JMS_CONNECTION_FACTORY_NAME);
			String user = properties.getProperty(JMSConstants.JMS_USERNAME);
			String password = properties.getProperty(JMSConstants.JMS_PASSWORD);
			boolean isTopic = true;
			jmsCtx = JMSContextFactory.createContext(factory, jmsurl, connFactoryName, user, password, destination, isTopic);

	        for (MessageListener listener : listeners)
	        {
	            MessageConsumer consumer = jmsCtx.getSession().createConsumer(jmsCtx.getDestination());
	            consumer.setMessageListener(listener);
	        }
	        jmsCtx.getConnection().start();
		} catch (JMSException e) {
			log.error(e);
			e.printStackTrace();
		} catch (NamingException e) {
			log.error(e);
			e.printStackTrace();
		}
    }
    
    public static void destroyContext() {
    	try {
			jmsCtx.destroy();
		} catch (JMSException e) {
			log.error(e);
			e.printStackTrace();
		}
    }
}
