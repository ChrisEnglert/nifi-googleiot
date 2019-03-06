package org.apache.nifi.processors.googleiot;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

public class TokenUtils {

    public static char[] getPassword(String algorithm, String projectId, String privateKeyFile)
            throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {

        byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);

        if (algorithm.equals("RS256")) {
            return createJwtRsa(projectId, spec).toCharArray();
        } else if (algorithm.equals("ES256")) {
            return createJwtEs(projectId, spec).toCharArray();
        } else {
            throw new IllegalArgumentException(
                    "Invalid algorithm " + algorithm + ". Should be one of 'RS256' or 'ES256'.");
        }
    }

    private static String createJwtRsa(String projectId, EncodedKeySpec spec)
            throws NoSuchAlgorithmException, InvalidKeySpecException {

        KeyFactory kf = KeyFactory.getInstance("RSA");
        Key key = kf.generatePrivate(spec);
        return getJWT(projectId, SignatureAlgorithm.RS256, key);
    }

    private static String createJwtEs(String projectId, EncodedKeySpec spec)
            throws NoSuchAlgorithmException, InvalidKeySpecException {

        KeyFactory kf = KeyFactory.getInstance("EC");
        Key key = kf.generatePrivate(spec);
        return getJWT(projectId, SignatureAlgorithm.ES256, key);
    }

    private static String getJWT(String projectId, SignatureAlgorithm algorithm, Key key) {

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
}
