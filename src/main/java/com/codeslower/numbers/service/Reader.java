package com.codeslower.numbers.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles reading input from the given channel.
 */
public class Reader implements Runnable {
    /**
     * The name of a system property that can be set during testing to
     * try out different line separators.
     *
     * If not set, the system default will be used.
      */
    public static final String LINE_SEP_PROPERTY = "numberReaderLineSep";
    private static Logger logger = LogManager.getLogger(Reader.class);

    private final ReadableByteChannel channel;
    private final BlockingQueue<byte[]> store;
    private final AtomicInteger receivedCount;

    public Reader(ReadableByteChannel channel, BlockingQueue<byte[]> store, AtomicInteger receivedCount) {
        this.channel = channel;
        this.store = store;
        this.receivedCount = receivedCount;
    }

    /**
     * Reads digits from a channel according to the specification given:
     *
        "* To be considered valid, the numbers must have exactly 9 digits which may include leading zeros.
         * Any data that does not conform to a valid line of input should be discarded and the client connection terminated quietly.
         * If a client writes a single line with only the word "terminate" then the server will exit immediately. "
     *
     * This class closes the associated connection if invalid data is present.
     * If the `terminate` command is given, then the class throws a
     * `TerminationException`, which will be handled in `main`.
     */
    private static class NumberReader {
        private static Logger logger = LogManager.getLogger(NumberReader.class);

        private static final byte Zero = (byte) '0';
        private static final byte Nine = (byte) '9';
        private final byte [] Newline = System.getProperty(LINE_SEP_PROPERTY, System.lineSeparator()).getBytes();
        private static final byte Tee = (byte) 't';
        private static final byte [] TERMINATE = "erminate".getBytes(Charset.forName("UTF-8"));

        private final ReadableByteChannel channel;
        private NumberReader.States state = NumberReader.States.START;
        private byte[] remaining = new byte[9];
        private int lastIdx = 0;
        private int cmdIdx = 0;
        private int nlIdx = 0;

        public NumberReader(ReadableByteChannel channel) {
            this.channel = channel;
        }

        enum States {
            START,
            TERMINATING,
            NUMBER,
            NEWLINE
        }

        /**
         * Parse 0 or more numbers from the given buffer. If invalid
         * input is encountered, the channel will be closed (however, any
         * valid numbers parsed will also be returned).
         *
         * If the client terminates the server, this method never returns.
         * @param buf
         * @return
         */
        public List<byte[]> readBuffer(ByteBuffer buf) {
            // The bytebuffer is parsed using a simple state
            // machine. Valid transitions are:
            //
            //  START -> NUMBER | TERMINATING
            //  TERMINATING -> TERMINATING
            //  NUMBER -> NUMBER | NEWLINE
            //  NEWLINE -> START
            //
            // Any invalid input closes the client connection; There is
            // no transition from TERMINATING to another statement because
            // we kill the server when "terminate" is parsed successfully.
            //
            // Note that we maintain state across calls to readBuffer, so
            // valid input may read across more than invocation.

            List<byte[]> result = new ArrayList<>(9);

      EXIT: for (int currIdx = 0; currIdx < buf.position(); currIdx++) {
                byte b = buf.get(currIdx);
                switch (state) {
                    case START:
                        remaining[lastIdx] = b;
                        lastIdx++;

                        if (b >= Zero && b <= Nine) {
                            state = NumberReader.States.NUMBER;
                        } else if (b == Tee) {
                            state = NumberReader.States.TERMINATING;
                        } else {
                            close();
                            break EXIT;
                        }

                        break;
                    case TERMINATING:
                        if (b == TERMINATE[cmdIdx]) {
                            cmdIdx++;
                            if (cmdIdx == TERMINATE.length) {
                                logger.debug("Throwing TerminateException.");
                                throw new Main.TerminateException();
                            }
                        } else {
                            close();
                            break EXIT;
                        }

                        break;
                    case NUMBER:
                        remaining[lastIdx] = b;
                        lastIdx++;

                        if (b < Zero || b > Nine) {
                            close();
                            break EXIT;
                        }

                        if (lastIdx == 9) {
                            result.add(remaining.clone());
                            state = NumberReader.States.NEWLINE;
                            lastIdx = 0;
                        }

                        break;
                    case NEWLINE:
                        if(b == Newline[nlIdx]) {
                            nlIdx++;
                            if(nlIdx == Newline.length) {
                                lastIdx = 0;
                                nlIdx = 0;
                                state = NumberReader.States.START;
                            }
                        }
                        else {
                            close();
                            break EXIT;
                        }
                        break;
                }
            }

            return result;
        }

        private void close() {
            try {
                if(logger.isDebugEnabled()) {
                    logger.debug("Closing client connection.");
                }
                channel.close();
            } catch (IOException e) {
            }
        }
    }

    @Override
    public void run() {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        buffer.order(ByteOrder.nativeOrder());
        NumberReader reader = new NumberReader(channel);
        while(true) {
            long startTime = System.currentTimeMillis();
            buffer.clear();
            try {

                int bytesRead = channel.read(buffer);
                if (bytesRead > 0) {
                    List<byte[]> result = reader.readBuffer(buffer);
                    if (result.size() > 0) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("Received %d numbers", result.size()));
                        }
                        for(byte [] r : result) {
                            store.put(r);
                        }
                        Main.RECEIVED_COUNT.mark(result.size());
                        receivedCount.addAndGet(result.size());
                    }
                }
                else if(bytesRead < 0) {
                    return;
                }

                Main.READ_TIMER.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            } catch (IOException e) {
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
