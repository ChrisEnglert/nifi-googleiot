package org.apache.nifi.processors.googleiot;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

public class GoogleIoTDeviceCredential {

    private final String projectId;
    private final SignatureAlgorithm algorithm;
    private final Key key;

    public GoogleIoTDeviceCredential(final String projectId, final SignatureAlgorithm algorithm, final Key key) {

        this.projectId = projectId;
        this.algorithm = algorithm;
        this.key = key;
    }

    public static GoogleIoTDeviceCredential apply(GoogleIoTDeviceConfig deviceConfig)
            throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {

        final String projectId = deviceConfig.getProjectId();
        final Key key = getKey ( deviceConfig );

        return new GoogleIoTDeviceCredential(projectId, SignatureAlgorithm.RS256, key);
    }

    public String getJWT() {

        DateTime now = new DateTime();

        // Create a JWT to authenticate this device. The device will be disconnected after the token
        // expires, and will have to reconnect with a new token. The audience field should always be set
        // to the GCP project id.
        JwtBuilder jwtBuilder =
                Jwts.builder()
                        .setIssuedAt(now.toDate())
                        .setExpiration(now.plusMinutes(20).toDate())
                        .setAudience(projectId);

        return jwtBuilder.signWith(algorithm, key).compact();
    }

    public void updateJWT(MqttConnectOptions mqttConnectOptions) {
        mqttConnectOptions.setPassword( getJWT().toCharArray() );
    }



    public static Key getKey(GoogleIoTDeviceConfig deviceConfig)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {

        final String privateKeyFile = deviceConfig.getPrivateKeyFile();

        byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);

        KeyFactory kf = KeyFactory.getInstance("RSA");
        Key key = kf.generatePrivate(spec);
        return key;
    }
}
