package com.codeslower.numbers.service;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class Main {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final byte [] TERMINATE = "erminate".getBytes(UTF8);
    static Logger logger = LogManager.getLogger(Main.class);

    static BlockingQueue<byte[]> toDeDupe;
    static BlockingQueue<String> numbers;

    static AtomicInteger duplicateCount = new AtomicInteger(0);
    static AtomicInteger receivedCount = new AtomicInteger(0);
    static byte Zero = (byte) '0';
    static byte Nine = (byte) '9';
    static byte NewLine = (byte) '\n';
    static byte Tee = (byte) 't';
    static Object obj = new Object();

    private static MetricRegistry registry = new MetricRegistry();
    private static Meter RECEIVED_COUNT = registry.meter(name(Main.class, "Received"));
    private static Meter DUPLICATE_COUNT = registry.meter(name(Main.class, "Duplicate"));
    private static Meter UNIQUE_COUNT = registry.meter(name(Main.class, "Unique"));

    public static class TerminateException extends RuntimeException {

        public TerminateException() {
            super();
        }

        public TerminateException(String message) {
            super(message);
        }

        public TerminateException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static void main(String [] args) throws IOException, InterruptedException, ExecutionException {
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.MINUTES)
                .build();
        reporter.start(1, TimeUnit.SECONDS);

        toDeDupe = new LinkedBlockingQueue<>(1000000);
        numbers = new LinkedBlockingQueue<>(100000);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(16);

        try(OutputStreamWriter out = new OutputStreamWriter(
                Files.newOutputStream(FileSystems.getDefault().getPath("numbers.log"),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING), UTF8)) {
            logger.info("Opened numbers.log");

            Server server = new Server(toDeDupe, executor);
            executor.submit(new Drainer(numbers, out));
            executor.submit(new Deduper(toDeDupe, numbers, duplicateCount));

            try {
                executor.submit(server).get();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch(ExecutionException e) {
                e.getCause().printStackTrace();
                e.printStackTrace();
            }

            executor.shutdown();
        }
    }

    public static class Server implements Runnable {
        static Logger logger = LogManager.getLogger(Server.class);

        private final BlockingQueue<byte[]> dest;
        private final ScheduledExecutorService executor;
        private final List<Future> clients = new ArrayList<>(5);

        public Server(BlockingQueue<byte[]> dest, ScheduledExecutorService executor) {
            this.dest = dest;
            this.executor = executor;
        }

        @Override
        public void run() {
            logger.info("Starting server.");
            try {
                Selector clientSelector = Selector.open();
                try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
                    ServerSocketChannel bind = serverSocketChannel.bind(new InetSocketAddress("localhost", 4000));
                    bind.configureBlocking(false);
                    bind.register(clientSelector, SelectionKey.OP_ACCEPT);

                    while(true) {
                        while (clientSelector.select(100) == 0) {
                            pruneClients();
                        }

                        try {
                            clientSelector.selectedKeys().clear();
                            pruneClients();

                            if (clients.size() == 5) {
                                logger.info("Ignoring client connection.");
                                continue;
                            }
                            SocketChannel client = bind.accept();
                            logger.info("Accepted client connection.");
                            clients.add(executor.submit(new Reader(client, dest, receivedCount)));
                        }
                        catch(IOException e) {  }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            catch(TerminateException e) {
                logger.info("Terminating server.");
            }
        }

        private void pruneClients() throws TerminateException {
            List<Future> toRemove = new ArrayList<>(5);
            for (Future f : clients) {
                if (f.isDone()) {
                    try {
                        f.get();
                    } catch (InterruptedException e) {

                    } catch (ExecutionException e) {
                        if(e.getCause() instanceof TerminateException) {
                            logger.info("Terminate request forwarded.");
                            throw (TerminateException) e.getCause();
                        }
                    }

                    toRemove.add(f);
                }
            }

            clients.removeAll(toRemove);
        }
    }

    public static class Reader implements Runnable {
        static Logger logger = LogManager.getLogger(Reader.class);

        private final ReadableByteChannel channel;
        private final Queue<byte[]> store;
        private final NumberReader reader;
        private final AtomicInteger receivedCount;
        private final ByteBuffer buffer;

        public Reader(ReadableByteChannel channel, Queue<byte[]> store, AtomicInteger receivedCount) {
            this.channel = channel;
            this.store = store;
            this.reader = new NumberReader(this);
            this.receivedCount = receivedCount;
            buffer = ByteBuffer.allocate(1000);
            buffer.order(ByteOrder.nativeOrder());
        }

        static class NumberReader {

            private final Reader reader;

            Expecting state = Expecting.START;
            byte[] remaining = new byte[9];
            int lastIdx = 0;
            int staticCmdIdx = 0;

            public NumberReader(Reader reader) {
                this.reader = reader;
            }

            enum Expecting {
                START,
                TERMINATING,
                NUMBER,
                NEWLINE
            }

            public List<byte[]> readBuffer(ByteBuffer buf, int startPos) {
                List<byte[]> result = new ArrayList<>(9);

                DONE:
                for (int currIdx = startPos; currIdx < buf.position(); currIdx++) {
                    byte b = buf.get(currIdx);
                    switch (state) {
                        case START:
                            remaining[lastIdx] = b;
                            lastIdx++;
                            if (b >= Zero && b <= Nine) {
                                state = Expecting.NUMBER;
                            } else if (b == Tee) {
                                staticCmdIdx = 0;
                                state = Expecting.TERMINATING;
                            } else {
                                close();
                                break DONE;
                            }
                            break;
                        case TERMINATING:
                            if (b == TERMINATE[staticCmdIdx]) {
                                staticCmdIdx += 1;
                                if (staticCmdIdx == TERMINATE.length) {
                                    logger.info("Throwing TerminateException.");
                                    throw new TerminateException();
                                }
                            } else {
                                close();
                                break DONE;
                            }
                            break;
                        case NUMBER:
                            remaining[lastIdx] = b;
                            lastIdx++;
                            if (b < Zero || b > Nine) {
                                close();
                                break DONE;
                            }

                            if (lastIdx == 9) {
                                result.add(remaining.clone());
                                state = Expecting.NEWLINE;
                                staticCmdIdx = 0;
                                lastIdx = 0;
                            }

                            break;
                        case NEWLINE:
                            if (b != NewLine) {
                                close();
                                break DONE;
                            }

                            lastIdx = 0;
                            state = Expecting.START;
                            break;
                    }
                }

                return result;
            }

            private void close() {
                try {
                    logger.info("Closing client connection.");
                    reader.channel.close();
                } catch (IOException e) {
                }
            }
        }

        @Override
        public void run() {
            while(true) {
                try {
                    buffer.clear();
                    int startPos = buffer.position();
                    int bytesRead = channel.read(buffer);
                    if (bytesRead > 0) {
                        List<byte[]> result = reader.readBuffer(buffer, startPos);
                        if (result.size() > 0) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("Received %d numbers", result.size()));
                            }
                            store.addAll(result);
                            RECEIVED_COUNT.mark(result.size());
                            receivedCount.addAndGet(result.size());
                        }
                    }
                } catch (IOException e) {
                    break;
                }
            }

        }
    }

    public static class Drainer implements Runnable {

        private final BlockingQueue<String> numbers;
        private final OutputStreamWriter outputStreamWriter;
        private List<String> result;
        private int lastCount = 0;
        private int lastDup = 0;

        public Drainer(BlockingQueue<String> numbers, OutputStreamWriter outputStreamWriter) {
            this.numbers = numbers;
            this.outputStreamWriter = outputStreamWriter;
            result = new ArrayList<>(100);
        }

        @Override
        public void run() {
            while(true) {
                if(numbers.size() > result.size())
                    result = new ArrayList<>(numbers.size());

                int dup = duplicateCount.get();
                int count = receivedCount.get();

//                logger.info(String.format("Saw %d duplicates, %d numbers since last time",
//                        dup - lastDup, count - lastCount));

                numbers.drainTo(result);
                if(logger.isDebugEnabled()) {
                    logger.debug(String.format("Drained %d elements", result.size()));
                }

                try {
                    for(String s : result) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("Writing %s", s));
                        }
                        outputStreamWriter.write(s);
                        outputStreamWriter.write("\n");
                    }

                    outputStreamWriter.flush();
                } catch (IOException e) {

                }

                result.clear();
                lastDup = dup;
                lastCount = count;
            }
        }
    }

    public static class Deduper implements Runnable {
        private final Map<String, Object> seen = new HashMap<>(100);
        private final BlockingQueue<byte[]> source;
        private final BlockingQueue<String> dest;
        private final AtomicInteger duplicateCount;

        public Deduper(BlockingQueue<byte[]> source, BlockingQueue<String> dest, AtomicInteger duplicateCount) {
            this.source = source;
            this.dest = dest;
            this.duplicateCount = duplicateCount;
        }

        @Override
        public void run() {
            while(true) {
                try {
                    byte[] result = source.poll(10, TimeUnit.MILLISECONDS);
                    if(result != null) {
                        if(logger.isDebugEnabled()) {
                            logger.debug("Got some bytes.");
                        }
                        String s = new String(result, "UTF-8");
                        if (! seen.containsKey(s)) {
                            seen.put(s, null);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Got unique element.");
                            }
                            UNIQUE_COUNT.mark();
                            dest.put(s);
                            continue;
                        }

                        DUPLICATE_COUNT.mark();
                        duplicateCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("The impossible happened!");
                }
            }
        }
    }
}
