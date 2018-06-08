/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.instrumentation;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * -Dflume.monitoring.type=influxdb
 * -Dflume.monitoring.url=http://127.0.0.1:8086
 * -Dflume.monitoring.database=hadoop
 * -Dflume.monitoring.username=hadoop
 * -Dflume.monitoring.password=hadoop
 * -Dflume.monitoring.cluster=hadoop
 *
 */
public class InfluxDBServer implements MonitorService {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBServer.class);

    private String url;
    private String username;
    private String password;
    private String database;
    private String cluster;
    private HttpClient httpClient;
    private int pollFrequency = 60;

    private String hostname;

    public final String CONF_POLL_FREQUENCY = "pollFrequency";
    public final String INFLUXDB_URL = "url";
    public final String INFLUXDB_DATABASE = "database";
    public final String INFLUXDB_USERNAME = "username";
    public final String INFLUXDB_PASSWORD = "password";
    public final String INFLUXDB_CLUSTER = "cluster";

    protected final InfluxDBCollector collectorRunnable;

    private ScheduledExecutorService service =
            Executors.newSingleThreadScheduledExecutor();

    public InfluxDBServer() throws FlumeException {
        collectorRunnable = new InfluxDBCollector();
    }

    @Override
    public void start() {
        try {
            logger.info("Influxdb server start...");
            httpClient = new DefaultHttpClient();
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            logger.error("Could not create httpClient for metrics collection.");
            throw new FlumeException(
                    "Could not create httpClient for metrics collection.", e);
        }

        collectorRunnable.server = this;
        if (service.isShutdown() || service.isTerminated()) {
            service = Executors.newSingleThreadScheduledExecutor();
        }
        service.scheduleWithFixedDelay(collectorRunnable, 0,
                pollFrequency, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        service.shutdown();

        while (!service.isTerminated()) {
            try {
                logger.warn("Waiting for influxdb service to stop");
                service.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                logger.warn("Interrupted while waiting"
                        + " for influxdb monitor to shutdown", ex);
                service.shutdownNow();
            }
        }

    }

    @Override
    public void configure(Context context) {
        this.pollFrequency = context.getInteger(this.CONF_POLL_FREQUENCY, 60);
        if (context.getString(this.INFLUXDB_URL) == null
                || context.getString(this.INFLUXDB_URL).isEmpty()) {
            throw new ConfigurationException("influxdb url cannot be empty.");
        }
        this.url = context.getString(this.INFLUXDB_URL);
        logger.info("influxdb url : " + this.url);

        if (context.getString(this.INFLUXDB_DATABASE) == null
                || context.getString(this.INFLUXDB_DATABASE).isEmpty()) {
            throw new ConfigurationException("influxdb db cannot be empty.");
        }
        this.database = context.getString(this.INFLUXDB_DATABASE);
        logger.info("influxdb db : " + this.database);

        if (context.getString(this.INFLUXDB_USERNAME) == null
                || context.getString(this.INFLUXDB_USERNAME).isEmpty()) {
            throw new ConfigurationException("influxdb username cannot be empty.");
        }
        this.username = context.getString(this.INFLUXDB_USERNAME);
        logger.info("influxdb username : " + this.username);

        if (context.getString(this.INFLUXDB_PASSWORD) == null
                || context.getString(this.INFLUXDB_PASSWORD).isEmpty()) {
            throw new ConfigurationException("influxdb password cannot be empty.");
        }
        this.password = context.getString(this.INFLUXDB_PASSWORD);
        logger.info("influxdb password : " + this.password);

        this.cluster = context.getString(this.INFLUXDB_CLUSTER, "default");
    }

    private String sendToInfluxdb(String component, String attribute, String value) {
        logger.info("sending influxdb formatted message with "
                + component + " - " + attribute +  " - "  + value);

        try {
            Float.parseFloat(value);
        } catch (NumberFormatException ex) {
            // The param is a string, and return null
            logger.warn("The metrics value is a String, leave it. value : " + value);
            return null;
        }

        StringBuilder lines = new StringBuilder();
        // measurement
        String mName = "flume_" + attribute;
        // tag
        StringBuilder tags = new StringBuilder();
        tags.append("component=").append(component).append(",")
                .append("hostname=").append(hostname).append(",")
                .append("cluster=").append(this.cluster);
        lines.append(mName).append(",").append(tags.toString().trim())
                .append(" ").append("value=").append(value).append(" ")
                .append("\n");
        return lines.toString();
    }

    /**
     * Write metrics to influx.
     * Influx assumes a String with the following format
     * metric,tag-key=tag-value value=value timestamp
     *
     * @param lines String
     * @return return boolean if the write http return code from influx equals 204
     * @throws IOException
     */
    public boolean write(final String lines) throws IOException {
        boolean result;
        logger.info("Message of sending to influxdb is " + lines + ". ");
        String influxUrl = url + "/write" + "" +
                "?db=" + URLEncoder.encode(database, "UTF-8") +
                "&u=" + URLEncoder.encode(username, "UTF-8") +
                "&p=" + URLEncoder.encode(password, "UTF-8");
        HttpPost request = new HttpPost(influxUrl);
        request.setEntity(new ByteArrayEntity(
                lines.getBytes("UTF-8")
        ));
        HttpResponse response = httpClient.execute(request);

        // consume entity and do nothing with it because we only
        // want the connection to be dropped.
        int statusCode = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());

        if (statusCode != 204) {
            logger.error("Unable to write or parse: \n" + lines + "\n");
            throw new IOException("Error writing metrics influxdb statuscode = " + statusCode);
        } else {
            result = true;
        }

        return result;
    }

    /**
     * Worker which polls JMX for all mbeans with
     * {@link javax.management.ObjectName} within the flume namespace:
     * org.apache.flume. All attributes of such beans are sent to the all hosts
     * specified by the server that owns it's instance.
     *
     */
    protected class InfluxDBCollector implements Runnable {

        private InfluxDBServer server;

        @Override
        public void run() {
            try {
                Map<String, Map<String, String>> metricsMap =
                        JMXPollUtil.getAllMBeans();
                for (String component : metricsMap.keySet()) {
                    Map<String, String> attributeMap = metricsMap.get(component);
                    for (String attribute : attributeMap.keySet()) {
                        // log 级别
                        logger.info("component : " + component + ", attribute : " + attribute
                                + ", value : " + attributeMap.get(attribute));
                        String message = server.sendToInfluxdb(component, attribute,
                                attributeMap.get(attribute));
                        if (message != null) {
                            server.write(message);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Unexpected error", t);
            }
        }
    }

}
