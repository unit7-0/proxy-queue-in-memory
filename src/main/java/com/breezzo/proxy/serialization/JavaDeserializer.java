package com.breezzo.proxy.serialization;

import com.breezzo.proxy.exception.ApplicationException;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public interface JavaDeserializer {
    default <T> T deserialize(byte[] bytes) {
        try (final var bis = new ByteArrayInputStream(bytes);
             final var ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new ApplicationException(e.getMessage(), e);
        }
    }
}
