package com.fh.his.cep;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.MessageListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.fh.his.cep.jms.JMSConnectionUtil;
import com.fh.his.cep.jms.JMSMessageListener;
import com.fh.his.cep.jms.TCPCEInsertStatement;
import com.fh.his.cep.jms.TCPCEPatternStatement;

/**
 * Hello world!
 * 
 */
public class App {
	private static Log log = LogFactory.getLog(App.class);

	private boolean isShutdown;

	public App() throws Exception {
		Configuration configuration = new Configuration();

		Map<String, Object> PcapHeader = new HashMap<String, Object>();
		PcapHeader.put("timestampInNanos", long.class);
		PcapHeader.put("wirelen1", long.class);
		configuration.addEventType("PcapHeader", PcapHeader);

		Map<String, Object> DataLinkLayer = new HashMap<String, Object>();
		DataLinkLayer.put("index", long.class);
		DataLinkLayer.put("ProtocolType1", String.class);
		DataLinkLayer.put("destination", String.class);
		DataLinkLayer.put("source", String.class);
		DataLinkLayer.put("next", String.class);
		configuration.addEventType("DataLinkLayer", DataLinkLayer);

		Map<String, Object> NetworkLayer = new HashMap<String, Object>();
		NetworkLayer.put("ttl", long.class);
		NetworkLayer.put("destination", String.class);
		NetworkLayer.put("index", long.class);
		NetworkLayer.put("ProtocolType", String.class);
		NetworkLayer.put("next", long.class);
		NetworkLayer.put("tos", long.class);
		NetworkLayer.put("type", long.class);
		NetworkLayer.put("source", String.class);
		NetworkLayer.put("id", long.class);
		configuration.addEventType("NetworkLayer", NetworkLayer);

		Map<String, Object> Tcp = new HashMap<String, Object>();
		Tcp.put("index", long.class);
		Tcp.put("destination", long.class);
		Tcp.put("source", long.class);
		Tcp.put("ack", long.class);
		Tcp.put("seq", long.class);
		Tcp.put("flags", Set.class);
		configuration.addEventType("Tcp", Tcp);

		EPServiceProvider engine = EPServiceProviderManager
				.getDefaultProvider(configuration);

		Map<String, Object> conEvent = new HashMap<String, Object>();
		conEvent.put("PcapHeader", "PcapHeader");
		conEvent.put("DataLinkLayer", "DataLinkLayer");
		conEvent.put("NetworkLayer", "NetworkLayer");
		conEvent.put("Tcp", "Tcp");

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("timestamp", long.class);
		response.put("source", String.class);
		response.put("destination", String.class);
		response.put("sourcePort", long.class);
		response.put("destinationPort", long.class);

		engine.getEPAdministrator().getConfiguration()
				.addEventType("cep.tcp.connection.established", response);

		engine.getEPAdministrator().getConfiguration()
				.addEventType("sniffer.header.parsed", conEvent);

		// Initialize engine
		log.info("Creating sample statement");

		TCPCEInsertStatement.createStatement(engine.getEPAdministrator());
		TCPCEPatternStatement.createStatement(engine.getEPAdministrator());

		int numListeners = 2;
		String destination = "sniffer.header.parsed";
		log.info("Creating " + numListeners + " listeners to destination '"
				+ destination + "'");

		MessageListener listeners[] = new JMSMessageListener[numListeners];
		for (int i = 0; i < numListeners; i++) {
			listeners[i] = new JMSMessageListener(engine.getEPRuntime());
		}
		JMSConnectionUtil.recieveEvent(destination, listeners);

		reportStats(listeners);
	}

	private void reportStats(MessageListener[] listeners)
			throws InterruptedException {
		// Register shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				isShutdown = true;
			}
		});

		// Report statistics
		long startTime = System.currentTimeMillis();
		long currTime;
		double deltaSeconds;
		int lastTotalEvents = 0;
		do {
			// sleep
			Thread.sleep(1000);
			currTime = System.currentTimeMillis();
			deltaSeconds = (currTime - startTime) / 1000.0;

			// compute stats
			int totalEvents = 0;
			for (int i = 0; i < listeners.length; i++) {
				totalEvents += ((JMSMessageListener) listeners[i]).getCount();
			}

			double totalLastBatch = totalEvents - lastTotalEvents;

			log.info("total=" + totalEvents + " last=" + totalLastBatch
					+ " time=" + deltaSeconds);
			lastTotalEvents = totalEvents;
		} while (!isShutdown);

		log.info("Exiting");
		System.exit(-1);

	}

	public static void main(String[] args) {
		try {
			new App();
		} catch (Throwable t) {
			log.error("Error starting server shell : " + t.getMessage(), t);
			System.out.println(t);
			System.exit(-1);
		}
	}
}
