package com.breezzo.proxy.serialization;

import com.breezzo.proxy.exception.ApplicationException;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

public interface JavaSerializer {
    default <T> byte[] serialize(T obj) {
        try (final var bos = new ByteArrayOutputStream();
             final var oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new ApplicationException(e.getMessage(), e);
        }
    }
}
