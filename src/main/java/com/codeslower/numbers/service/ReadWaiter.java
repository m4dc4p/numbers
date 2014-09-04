package com.codeslower.numbers.service;

import java.io.IOException;

/**
 * Abstracts the ability to select on a channel, so
 * Reader does not need to know that it is reading
 * from a channel.
 */
public interface ReadWaiter extends AutoCloseable {
    /**
     * Blocks until whatever this object is waiting on is
     * ready to read.
     * @throws IOException
     */
    void waitToRead() throws IOException;

    /**
     * Close any associated resources.
     */
    @Override
    void close();
}
