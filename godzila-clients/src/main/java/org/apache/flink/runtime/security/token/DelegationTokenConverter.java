package org.apache.flink.runtime.security.token;


import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/** Delegation token serializer and deserializer functionality. */
public class DelegationTokenConverter {
    /** Serializes delegation tokens. */
    public static byte[] serialize(Credentials credentials) throws IOException {
        try (DataOutputBuffer dob = new DataOutputBuffer()) {
            credentials.writeTokenStorageToStream(dob);
            return dob.getData();
        }
    }

    /** Deserializes delegation tokens. */
    public static Credentials deserialize(byte[] credentialsBytes) throws IOException {
        try (DataInputStream dis =
                     new DataInputStream(new ByteArrayInputStream(credentialsBytes))) {
            Credentials credentials = new Credentials();
            credentials.readTokenStorageStream(dis);
            return credentials;
        }
    }
}
