package org.apache.nifi.processors.googleiot;

import org.apache.nifi.logging.ComponentLog;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class GoogleIoTDeviceClient {

    static MqttCallback mCallback;
    private final ComponentLog logger;
    private GoogleIoTDeviceConfig deviceConfig;


    public MqttClient startMqtt(
            String projectId,
            String cloudRegion,
            String registryId,
            String gatewayId,
            String privateKeyFile,
            String algorithm)
            throws NoSuchAlgorithmException, IOException, MqttException, InterruptedException,
            InvalidKeySpecException {


        String mqttServerAddress = MqttClientUtils.getServerAddress();
        String mqttClientId = MqttClientUtils.getClientId(projectId, cloudRegion, registryId, gatewayId);

        MqttConnectOptions connectOptions = MqttClientUtils.getOptions( TokenUtils.getPassword ( algorithm, projectId, privateKeyFile) );

        // Create a client, and connect to the Google MQTT bridge.
        MqttClient client = new MqttClient(mqttServerAddress, mqttClientId, new MemoryPersistence());

        tryConnect(client, connectOptions);

        attachCallback(client, gatewayId);

        return client;
    }

    void tryConnect(MqttClient client, MqttConnectOptions connectOptions) throws MqttException, InterruptedException {

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

    public static void sendDataFromDevice(
            MqttClient client, String deviceId, String messageType, String data) throws MqttException {

        if (!messageType.equals("events") && !messageType.equals("state")) {
            System.err.println("Invalid message type, must ether be 'state' or events'");
            return;
        }
        final String dataTopic = String.format("/devices/%s/%s", deviceId, messageType);
        MqttMessage message = new MqttMessage(data.getBytes());
        message.setQos(1);
        client.publish(dataTopic, message);
    }

    public void attachCallback(MqttClient client, String deviceId) throws MqttException {
        mCallback =
                new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        // Do nothing...
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) throws Exception {
                        String payload = new String(message.getPayload());
                        System.out.println("Payload : " + payload);
                        // TODO: Insert your parsing / handling of the configuration message here.
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        // Do nothing;
                    }
                };

        String commandTopic = String.format("/devices/%s/commands/#", deviceId);
        logger.info(String.format("Listening on %s", commandTopic));

        String configTopic = String.format("/devices/%s/config", deviceId);
        logger.info(String.format("Listening on %s", configTopic));

        client.subscribe(configTopic, 1);
        client.subscribe(commandTopic, 1);

        client.setCallback(mCallback);
    }


    MqttClient mqttClient;
    MqttConnectOptions mqttConnectOptions;

    public GoogleIoTDeviceClient(ComponentLog logger) {
        this.logger = logger;
    }

    public void onScheduled(GoogleIoTDeviceConfig deviceConfig)
            throws Exception {

        this.deviceConfig = deviceConfig;

        mqttClient = startMqtt(
                deviceConfig.getProjectId(),
                deviceConfig.getRegion(),
                deviceConfig.getRegistryId(),
                deviceConfig.getDeviceId(),
                deviceConfig.getPrivateKeyFile(),
                deviceConfig.getAlgorithm()
        );

    }

    public void onStopped() {
        try {
            logger.info("Disconnecting client");
            mqttClient.disconnect();
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


    public boolean isConnected(){
        return (mqttClient != null && mqttClient.isConnected());
    }

    public boolean tryPublish(MqttMessage message, String messageType) {
        if (!messageType.equals("events") && !messageType.equals("state")) {
            logger.error("Invalid message type, must ether be 'state' or events'");
            return false;
        }

        try {

            final String dataTopic = String.format("/devices/%s/%s", deviceConfig.getDeviceId(), messageType);
            message.setQos(1);
            mqttClient.publish(dataTopic, message);

        } catch (MqttException e) {
            return false;
        }

        return true;
    }
}
