package org.apache.nifi.processors.googleiot;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import java.util.Properties;

public class MqttClientUtils {

    public static MqttConnectOptions getOptions(char[] password) {
        MqttConnectOptions connectOptions = new MqttConnectOptions();

        // Note that the Google Cloud IoT Core only supports MQTT 3.1.1, and Paho requires that we
        // explictly set this. If you don't set MQTT version, the server will immediately close its
        // connection to your device.
        connectOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

        Properties sslProps = new Properties();
        sslProps.setProperty("com.ibm.ssl.protocol", "TLSv1.2");
        connectOptions.setSSLProperties(sslProps);

        // With Google Cloud IoT Core, the username field is ignored, however it must be set for the
        // Paho client library to send the password field. The password field is used to transmit a JWT
        // to authorize the device.
        connectOptions.setUserName("unused");
        connectOptions.setPassword(password);

        return connectOptions;
    }


    public static String getServerAddress() {

        final String mqttBridgeHostname = "mqtt.googleapis.com";
        final Integer mqttBridgePort = 443;

        // Build the connection string for Google's Cloud IoT Core MQTT server. Only SSL
        // connections are accepted. For server authentication, the JVM's root certificates
        // are used.
        final String mqttServerAddress = String.format("ssl://%s:%s", mqttBridgeHostname, mqttBridgePort);
        return mqttServerAddress;
    }

    public static String getClientId(final String projectId,final String cloudRegion,final String registryId,final String gatewayId) {

        // Create our MQTT client. The mqttClientId is a unique string that identifies this device. For
        // Google Cloud IoT Core, it must be in the format below.
        final String mqttClientId = String.format(
                "projects/%s/locations/%s/registries/%s/devices/%s",
                projectId, cloudRegion, registryId, gatewayId);

        return mqttClientId;
    }


    public static String getClientId(GoogleIoTDeviceConfig deviceConfig) {
        return getClientId(deviceConfig.getProjectId(), deviceConfig.getRegion(), deviceConfig.getRegistryId(), deviceConfig.getDeviceId());
    }
}
