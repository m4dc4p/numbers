package com.codeslower.numbers.service;

import com.codahale.metrics.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class Main {

    private static Logger logger = LogManager.getLogger(Main.class);

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final int MAX_CLIENTS = 5;

    private static final MetricRegistry registry = new MetricRegistry();
    public static final Meter RECEIVED_COUNT = registry.meter(name(Main.class, "Received"));
    public static final Meter DUPLICATE_COUNT = registry.meter(name(Main.class, "Duplicate"));
    public static final Meter UNIQUE_COUNT = registry.meter(name(Main.class, "Unique"));
    public static final Meter WRITE_COUNT = registry.meter(name(Main.class, "Written"));

    public static final Timer DEDUPE_TIMER = registry.timer(name(Main.class, "DeDuper"));
    public static final Timer DRAIN_TIMER = registry.timer(name(Main.class, "Drainer"));
    public static final Timer READ_TIMER = registry.timer(name(Main.class, "Reader"));

    private static OutputStream createLog(String filename) throws IOException {
        return Files.newOutputStream(FileSystems.getDefault().getPath(filename),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    public static void main(String [] args) throws IOException, InterruptedException, ExecutionException {

        AtomicInteger duplicateCount = new AtomicInteger(0);
        AtomicInteger receivedCount = new AtomicInteger(0);
        BlockingQueue<byte[]> toDeDupe = new ArrayBlockingQueue<>(1_000_000);
        BlockingQueue<Integer> numbers = new ArrayBlockingQueue<>(1_000_000);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(16);

        try(PrintStream counters = new PrintStream(createLog("counters.log"), false, "UTF-8");
            PrintStream timers = new PrintStream(createLog("timers.log"), false, "UTF-8");
            OutputStreamWriter out = new OutputStreamWriter(createLog("numbers.log"), UTF8)) {

            setupMetrics(counters, timers);

            executor.submit(new Drainer(numbers, out));
            executor.submit(new DeDuper(toDeDupe, numbers, duplicateCount));
            executor.scheduleWithFixedDelay(new Reporter(System.out, duplicateCount, receivedCount), 10, 10, TimeUnit.SECONDS);

            executor.submit(new Server(toDeDupe, executor, receivedCount)).get();

        }
        catch (InterruptedException|ExecutionException e) { }

        executor.shutdownNow();
        logger.info("Done");
        System.exit(0);
    }

    private static void setupMetrics(PrintStream counters, PrintStream timers) {
        JmxReporter.forRegistry(registry).build().start();
        ConsoleReporter.forRegistry(registry)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .convertRatesTo(TimeUnit.MINUTES)
                .filter(new MetricFilter() {
                    @Override
                    public boolean matches(String s, Metric metric) {
                        return metric instanceof Timer;
                    }
                })
                .outputTo(timers)
                .build()
                .start(10, TimeUnit.SECONDS);
        ConsoleReporter.forRegistry(registry)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .convertRatesTo(TimeUnit.MINUTES)
                .filter(new MetricFilter() {
                    @Override
                    public boolean matches(String s, Metric metric) {
                        return metric instanceof Meter;
                    }
                })
                .outputTo(counters)
                .build()
                .start(10, TimeUnit.SECONDS);
    }

    public static class TerminateException extends RuntimeException {
        public TerminateException() {
            super();
        }
    }

    public static class Server implements Runnable {
        private static Logger logger = LogManager.getLogger(Server.class);

        private final BlockingQueue<byte[]> dest;
        private final ScheduledExecutorService executor;
        private final List<Future> clients = new ArrayList<>(MAX_CLIENTS);
        private AtomicInteger receivedCount;

        public Server(BlockingQueue<byte[]> dest, ScheduledExecutorService executor, AtomicInteger receivedCount) {
            this.dest = dest;
            this.executor = executor;
            this.receivedCount = receivedCount;
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

    /**
     * On every invocation, prints the count of duplicate numbers seen (and numbers
     * received) since the last invocation.
     */
    public static class Reporter implements Runnable {
        private final PrintStream out;
        private final AtomicInteger duplicateCount;
        private final AtomicInteger receivedCount;
        private int lastCount = 0;
        private int lastDup = 0;

        public Reporter(PrintStream out, AtomicInteger duplicateCount, AtomicInteger receivedCount) {
            this.out = out;
            this.duplicateCount = duplicateCount;
            this.receivedCount = receivedCount;
        }

        @Override
        public void run() {
            int dup = duplicateCount.get();
            int count = receivedCount.get();

            out.println(String.format("Saw %d duplicates, %d numbers since last time",
                    dup - lastDup, count - lastCount));

            lastDup = dup;
            lastCount = count;
        }
    }

    /**
     * Converts bytes (representing ASCII digits) in an input queue into numbers; discards
     * duplicate values; puts unique values into the given destination queue.
     */
    public static class DeDuper implements Runnable {
        private static final int MAX_NUM = 1_000_000_000;
        private static final Logger logger = LogManager.getLogger(DeDuper.class);

        private final BitSet seen = new BitSet(MAX_NUM);
        private final BlockingQueue<byte[]> source;
        private final BlockingQueue<Integer> dest;
        private final AtomicInteger duplicateCount;

        public DeDuper(BlockingQueue<byte[]> source, BlockingQueue<Integer> dest, AtomicInteger duplicateCount) {
            this.source = source;
            this.dest = dest;
            this.duplicateCount = duplicateCount;
        }

        @Override
        public void run() {
            List<byte[]> result = new ArrayList<>(1000);
            while(true) {
                try {
                    byte [] first = source.take();

                    if(first != null) {
                        long starTime = System.currentTimeMillis();
                        result.add(first);
                        source.drainTo(result, 999);
                        if(logger.isDebugEnabled()) {
                            logger.debug("Got some bytes.");
                        }

                        for(byte[] b : result) {
                            Integer val = Integer.parseInt(new String(b, "UTF-8"));
                            if (! seen.get(val)) {
                                seen.set(val, true);
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Got unique element.");
                                }
                                UNIQUE_COUNT.mark();
                                dest.put(val);
                                continue;
                            }

                            DUPLICATE_COUNT.mark();
                            duplicateCount.incrementAndGet();
                        }

                        result.clear();
                        DEDUPE_TIMER.update(System.currentTimeMillis() - starTime, TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("The impossible happened!");
                }
            }
        }
    }

    /**
     * Drains numbers from the input queue and writes them to an output
     * stream.
     */
    public static class Drainer implements Runnable {
        private static final Logger logger = LogManager.getLogger(Drainer.class);

        private final BlockingQueue<Integer> numbers;
        private final OutputStreamWriter outputStreamWriter;

        public Drainer(BlockingQueue<Integer> numbers, OutputStreamWriter outputStreamWriter) {
            this.numbers = numbers;
            this.outputStreamWriter = outputStreamWriter;
        }

        @Override
        public void run() {
            List<Integer> result = new ArrayList<>(1000);
            while(true) {
                try {
                    Integer first = numbers.take();

                    if (first != null) {
                        long startTime = System.currentTimeMillis();
                        result.add(first);
                        numbers.drainTo(result, 999);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("Drained %d elements", result.size()));
                        }

                        for (Integer i : result) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("Writing %s", i));
                            }
                            outputStreamWriter.write(String.format("%09d\n", i));
                            WRITE_COUNT.mark();
                        }

                        outputStreamWriter.flush();
                        result.clear();

                        DRAIN_TIMER.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
                    }
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {

                }
            }
        }
    }
}
