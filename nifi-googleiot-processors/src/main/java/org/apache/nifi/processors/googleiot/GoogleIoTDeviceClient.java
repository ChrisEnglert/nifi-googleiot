package org.apache.nifi.processors.googleiot;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.logging.ComponentLog;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GoogleIoTDeviceClient {

    static MqttCallback mCallback;
    private final ComponentLog logger;
    private final ConcurrentLinkedQueue queue = new ConcurrentLinkedQueue();

    private GoogleIoTDeviceConfig deviceConfig;
    private GoogleIoTDeviceCredential credential;

    private MqttClient mqttClient;
    private MqttConnectOptions mqttConnectOptions;

    public GoogleIoTDeviceClient(ComponentLog logger) {
        this.logger = logger;
    }

    private MqttClient startMqtt()
            throws NoSuchAlgorithmException, IOException, MqttException, InterruptedException,
            InvalidKeySpecException {

        String mqttServerAddress = MqttClientUtils.getServerAddress();
        String mqttClientId = MqttClientUtils.getClientId(deviceConfig);

        mqttConnectOptions = MqttClientUtils.getOptions();

        credential = GoogleIoTDeviceCredential.apply(deviceConfig);

        credential.updateJWT(mqttConnectOptions);

        // Create a client, and connect to the Google MQTT bridge.
        MqttClient client = new MqttClient(mqttServerAddress, mqttClientId, new MemoryPersistence());

        tryConnect(client, mqttConnectOptions);

        attachCallback(client, deviceConfig.getDeviceId());

        return client;
    }

    private synchronized void tryConnect(MqttClient client, MqttConnectOptions connectOptions) throws MqttException, InterruptedException {

        if (client.isConnected()) {
            return;
        }

        // Both connect and publish operations may fail. If they do, allow retries but with an
        // exponential back-off time period.
        long initialConnectIntervalMillis = 500L;
        long maxConnectIntervalMillis = 6000L;
        long maxConnectRetryTimeElapsedMillis = 900000L;
        float intervalMultiplier = 1.5f;

        long retryIntervalMs = initialConnectIntervalMillis;
        long totalRetryTimeMs = 0;

        while (!client.isConnected() && totalRetryTimeMs < maxConnectRetryTimeElapsedMillis) {
            try {
                client.connect(connectOptions);
            } catch (MqttException e) {
                int reason = e.getReasonCode();

                // If the connection is lost or if the server cannot be connected, allow retries, but with
                // exponential backoff.
                logger.warn("An error occurred: " + e.getMessage());
                if (reason == MqttException.REASON_CODE_CONNECTION_LOST
                        || reason == MqttException.REASON_CODE_SERVER_CONNECT_ERROR) {
                    logger.warn("Retrying in " + retryIntervalMs / 1000.0 + " seconds.");
                    Thread.sleep(retryIntervalMs);
                    totalRetryTimeMs += retryIntervalMs;
                    retryIntervalMs *= intervalMultiplier;
                    if (retryIntervalMs > maxConnectIntervalMillis) {
                        retryIntervalMs = maxConnectIntervalMillis;
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    private void attachCallback(MqttClient client, String deviceId) throws MqttException {
        mCallback =
                new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        logger.warn("Connection lost due to {}", new Object[]{cause.getMessage()}, cause);

                        try {

                            credential.updateJWT(mqttConnectOptions);
                            tryConnect(client, mqttConnectOptions);

                        } catch (Throwable t) {
                            logger.error("Recconect failed due to {}", new Object[]{t.getMessage()}, t);
                        }
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        queue.add(new ImmutablePair<>(topic, message.getPayload()));
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        // Do nothing;
                    }
                };

        client.setCallback(mCallback);

        String commandTopic = String.format("/devices/%s/commands/#", deviceId);
        logger.info(String.format("Listening on %s", commandTopic));

        String configTopic = String.format("/devices/%s/config", deviceId);
        logger.info(String.format("Listening on %s", configTopic));

        client.subscribe(configTopic, 1);
        client.subscribe(commandTopic, 1);
    }

    public void onScheduled(GoogleIoTDeviceConfig deviceConfig)
            throws Exception {

        this.deviceConfig = deviceConfig;

        mqttClient = startMqtt();
    }

    public synchronized void onStopped() {
        try {

            if (mqttClient.isConnected()) {
                logger.info("Disconnecting client");
                mqttClient.disconnect();
            }
        } catch(MqttException me) {
            logger.error("Error disconnecting MQTT client due to {}", new Object[]{me.getMessage()}, me);
        }

        try {
            logger.info("Closing client");
            mqttClient.close();
            mqttClient = null;
        } catch (MqttException me) {
            logger.error("Error closing MQTT client due to {}", new Object[]{me.getMessage()}, me);
        }
    }

    public boolean tryPublish(byte[] content, String messageType) {
        if (!messageType.equals("events") && !messageType.equals("state")) {
            logger.error("Invalid message type, must ether be 'state' or events'");
            return false;
        }

        try {

            final String dataTopic = String.format("/devices/%s/%s", deviceConfig.getDeviceId(), messageType);

            final MqttMessage message = new MqttMessage(content);
            message.setQos(1);

            credential.updateJWT(mqttConnectOptions);

            tryConnect(mqttClient, mqttConnectOptions);

            mqttClient.publish(dataTopic, message);

        } catch (MqttException | InterruptedException me) {
            logger.error("Error closing MQTT client due to {}", new Object[]{me.getMessage()}, me);
            return false;
        }

        return true;
    }

    public Pair<String, byte[]> receive() {
        return (Pair<String, byte[]>) queue.poll();
    }
}
