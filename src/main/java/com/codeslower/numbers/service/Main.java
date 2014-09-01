package com.codeslower.numbers.service;

import com.codahale.metrics.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class Main {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final byte [] TERMINATE = "erminate".getBytes(UTF8);
    private static final int MAX_CLIENTS = 10;
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
    private static Meter WRITE_COUNT = registry.meter(name(Main.class, "Written"));

    private static com.codahale.metrics.Timer DEDUPE_TIMER = registry.timer(name(Main.class, "Deduper"));
    private static com.codahale.metrics.Timer DRAIN_TIMER = registry.timer(name(Main.class, "Drainer"));
    private static com.codahale.metrics.Timer READ_TIMER = registry.timer(name(Main.class, "Reader"));

    public static void main(String [] args) throws IOException, InterruptedException, ExecutionException {

        toDeDupe = new ArrayBlockingQueue<>(1_000_000);
        numbers = new ArrayBlockingQueue<>(1_000_000);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(16);

        try(PrintStream metrics = new PrintStream(Files.newOutputStream(FileSystems.getDefault().getPath("metrics.log"),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        ), true, UTF8.name())) {

            JmxReporter.forRegistry(registry).build().start();
            ConsoleReporter.forRegistry(registry)
                    .convertDurationsTo(TimeUnit.MICROSECONDS)
                    .convertRatesTo(TimeUnit.MINUTES)
                    .filter(new MetricFilter() {
                        @Override
                        public boolean matches(String s, Metric metric) {
                            return metric instanceof Meter;
                        }
                    })
                    .outputTo(metrics)
                    .build()
                    .start(10, TimeUnit.SECONDS);

            try (OutputStreamWriter out = new OutputStreamWriter(
                    Files.newOutputStream(FileSystems.getDefault().getPath("numbers.log"),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING), UTF8)) {
                logger.info("Opened numbers.log");

                Server server = new Server(toDeDupe, executor);
                executor.submit(new Drainer(numbers, out));
                executor.submit(new Deduper(toDeDupe, numbers, duplicateCount));
                executor.scheduleWithFixedDelay(new Reporter(), 10, 10, TimeUnit.SECONDS);

                try {
                    executor.submit(server).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.getCause().printStackTrace();
                    e.printStackTrace();
                }

                executor.shutdownNow();
                logger.info("Done");
            }
        }

        System.exit(0);
    }

    public static class TerminateException extends RuntimeException {
        public TerminateException() {
            super();
        }
    }

    public static class Server implements Runnable {
        static Logger logger = LogManager.getLogger(Server.class);

        private final BlockingQueue<byte[]> dest;
        private final ScheduledExecutorService executor;
        private final List<Future> clients = new ArrayList<>(MAX_CLIENTS);

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

                    while (true) {
                        while (clientSelector.select(100) == 0) {
                            pruneClients();
                        }

                        try {
                            clientSelector.selectedKeys().clear();
                            pruneClients();
                            SocketChannel client = bind.accept();

                            if (clients.size() >= MAX_CLIENTS) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Ignoring client connection.");
                                }
                                client.close();
                                continue;
                            }

                            client.configureBlocking(false);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Accepted client connection.");
                            }
                            clients.add(executor.submit(new Reader(client, dest, receivedCount)));
                        } catch (IOException e) { }
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
            List<Future> toRemove = new ArrayList<>(MAX_CLIENTS);
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
        private final BlockingQueue<byte[]> store;
        private final AtomicInteger receivedCount;

        public Reader(ReadableByteChannel channel, BlockingQueue<byte[]> store, AtomicInteger receivedCount) {
            this.channel = channel;
            this.store = store;
            this.receivedCount = receivedCount;
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

            public List<byte[]> readBuffer(ByteBuffer buf) {
                List<byte[]> result = new ArrayList<>(9);

                DONE:
                for (int currIdx = 0; currIdx < buf.position(); currIdx++) {
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
                    if(logger.isDebugEnabled()) {
                        logger.debug("Closing client connection.");
                    }
                    reader.channel.close();
                } catch (IOException e) {
                }
            }
        }

        @Override
        public void run() {
            ByteBuffer buffer = ByteBuffer.allocate(1000);
            buffer.order(ByteOrder.nativeOrder());
            NumberReader reader = new NumberReader(this);
            while(true) {
                long startTime = System.currentTimeMillis();
                try {
                    buffer.clear();
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
                            RECEIVED_COUNT.mark(result.size());
                            receivedCount.addAndGet(result.size());
                        }
                    }

                    READ_TIMER.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
                } catch (IOException e) {
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static class Reporter implements Runnable {
        private int lastCount = 0;
        private int lastDup = 0;

        @Override
        public void run() {
            int dup = duplicateCount.get();
            int count = receivedCount.get();

            System.out.println(String.format("Saw %d duplicates, %d numbers since last time",
                    dup - lastDup, count - lastCount));

            lastDup = dup;
            lastCount = count;
        }
    }

    public static class Drainer implements Runnable {

        private final BlockingQueue<String> numbers;
        private final OutputStreamWriter outputStreamWriter;
        private List<String> result;

        public Drainer(BlockingQueue<String> numbers, OutputStreamWriter outputStreamWriter) {
            this.numbers = numbers;
            this.outputStreamWriter = outputStreamWriter;
            result = new ArrayList<>(1000);
        }

        @Override
        public void run() {
            while(true) {
                long startTime = System.currentTimeMillis();
                numbers.drainTo(result, 1000);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Drained %d elements", result.size()));
                }

                try {
                    for (String s : result) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("Writing %s", s));
                        }
                        outputStreamWriter.write(s);
                        outputStreamWriter.write("\n");
                        WRITE_COUNT.mark();
                    }

                    outputStreamWriter.flush();
                } catch (IOException e) {

                }

                result.clear();
                DRAIN_TIMER.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            }
        }
    }

    public static class Deduper implements Runnable {
        private static final int MAX_NUM = 1_000_000_000;
        private static final Logger logger = LogManager.getLogger(Deduper.class);

        private final boolean[] seen = new boolean[MAX_NUM];
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
            List<byte[]> result = new ArrayList<>(1000);
            while(true) {
                long starTime = System.currentTimeMillis();
                try {
                    int cnt = source.drainTo(result, 1000);
                    if(cnt > 0) {
                        if(logger.isDebugEnabled()) {
                            logger.debug("Got some bytes.");
                        }
                        for(byte[] b : result) {
                            String str = new String(b, "UTF-8");
                            int val = Integer.parseInt(str);
                            if (! seen[val]) {
                                seen[val] = true;
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Got unique element.");
                                }
                                UNIQUE_COUNT.mark();
                                dest.put(str);
                                continue;
                            }

                            DUPLICATE_COUNT.mark();
                            duplicateCount.incrementAndGet();
                        }

                        DEDUPE_TIMER.update(System.currentTimeMillis() - starTime, TimeUnit.MILLISECONDS);
                        result.clear();
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
