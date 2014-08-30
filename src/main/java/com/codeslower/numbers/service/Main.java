package com.codeslower.numbers.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class Main {

    static BlockingQueue<byte[]> toDeDupe;
    static BlockingQueue<String> numbers;

    static AtomicInteger duplicateCount = new AtomicInteger(0);
    static AtomicInteger receivedCount = new AtomicInteger(0);
    static byte Zero = (byte) Character.getNumericValue('0');
    static byte Nine = (byte) Character.getNumericValue('9');
    static byte NewLine = (byte) Character.getNumericValue('\n');
    static Object obj = new Object();

    public static void main(String [] args) throws IOException {
        toDeDupe = new LinkedBlockingQueue<>(100);
        numbers = new LinkedBlockingQueue<>(100);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(16);

        try(BufferedWriter outputFile = Files.newBufferedWriter(Paths.get("numbers.log"),
                            Charset.forName("UTF-8"),
                            CREATE,
                            TRUNCATE_EXISTING)) {
            try {
                Server server = new Server(toDeDupe, executor);
                executor.scheduleWithFixedDelay(new Deduper(toDeDupe, numbers), 0, 0, TimeUnit.SECONDS);
                executor.scheduleWithFixedDelay(new Drainer(numbers, outputFile), 10, 10, TimeUnit.SECONDS);
                executor.submit(server).get();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Server implements Runnable {
        private final BlockingQueue<byte[]> dest;
        private final ScheduledExecutorService executor;
        private final List<Future<?>> clients = new ArrayList<>(5);

        public Server(BlockingQueue<byte[]> dest, ScheduledExecutorService executor) {
            this.dest = dest;
            this.executor = executor;
        }

        @Override
        public void run() {
            try {
                Selector clientSelector = Selector.open();
                try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
                    ServerSocketChannel bind = serverSocketChannel.bind(InetSocketAddress.createUnresolved("127.0.0.1", 4000));
                    bind.register(clientSelector, SelectionKey.OP_ACCEPT);

                    while(true) {
                        while (clientSelector.select(100) == 0) ;
                        try {
                            SocketChannel client = bind.accept();

                            if (clients.size() == 5) {
                                for (Future f : clients) {
                                    if (f.isDone()) {
                                        clients.remove(f);
                                    }
                                }
                            }

                            if (clients.size() == 5) {
                                // ignore connection
                                try {
                                    client.close();
                                } catch (IOException e) {
                                }
                            } else {
                                clients.add(executor.submit(new Reader(client, dest)));
                            }
                        }
                        catch(IOException e) { }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reader implements Runnable {
        private final ReadableByteChannel channel;
        private final Queue<byte[]> store;
        private final NumberReader reader;

        public Reader(ReadableByteChannel channel, Queue<byte[]> store) {
            this.channel = channel;
            this.store = store;
            this.reader = new NumberReader(this);
        }

        static class NumberReader {

            private static final String TERMINATE = "terminate";
            private final Reader reader;

            enum Expecting {
                NUMBER,
                NEWLINE
            }

            public NumberReader(Reader reader) {
                this.reader = reader;
            }

            Expecting state = Expecting.NUMBER;
            byte [] remaining = new byte[9];
            int lastIdx = 0;

            public List<byte[]> readBuffer(ByteBuffer buf, int startPos) {
                List<byte[]> result = new ArrayList<>(10);

                for(int startAt = startPos; startAt < buf.position(); startAt++) {
                    byte b = buf.get(startAt);
                    switch(state) {
                        case NUMBER:
                            if(b >= Zero && b <= Nine) {
                                remaining[lastIdx] = b;
                                lastIdx++;
                            }
                            else {
                                closeOrExit();
                            }

                            if(lastIdx == 9) {
                                result.add(remaining.clone());
                                state = Expecting.NEWLINE;
                                lastIdx = 0;
                            }

                            break;
                        case NEWLINE:
                            if(b != NewLine) {
                                closeOrExit();
                            }

                            state = Expecting.NUMBER;
                            break;
                    }
                }

                return result;
            }

            private void closeOrExit() {
                try {
                    switch(new String(remaining, 0, TERMINATE.length(), "UTF-8")) {
                        case "terminate":
                            System.exit(1);
                            break;
                    }
                } catch (UnsupportedEncodingException e) { }

                close();
            }

            private void close() {
                try {
                    reader.channel.close();
                } catch (IOException e) { }
            }
        }

        @Override
        public void run() {
            ByteBuffer buffer = ByteBuffer.allocate(1000);
            buffer.order(ByteOrder.nativeOrder());

            while(true) {
                try {
                    int startPos = buffer.position();
                    int bytesRead = channel.read(buffer);
                    if(bytesRead > 0) {
                        List<byte[]> result = reader.readBuffer(buffer, startPos);
                        if (result.size() > 0) {
                            store.addAll(result);
                            receivedCount.addAndGet(result.size());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if(! channel.isOpen()) {
                    return;
                }
            }
        }
    }

    public static class Drainer implements Runnable {

        private final BlockingQueue<String> numbers;
        private final BufferedWriter out;
        private List<String> result;
        private int lastCount = 0;
        private int lastDup = 0;

        public Drainer(BlockingQueue<String> numbers, BufferedWriter out) {
            this.numbers = numbers;
            this.out = out;
            result = new ArrayList<>(100);
        }

        @Override
        public void run() {
            if(numbers.size() > result.size())
                result = new ArrayList<>(numbers.size());

            int dup = duplicateCount.get();
            int count = receivedCount.get();

            numbers.drainTo(result);

            System.out.println(String.format("Saw %d duplicates, %d numbers since last time",
                    dup - lastDup, count - lastCount));

            for(String s : result) {
                try {
                    out.write(s);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }

            lastDup = dup;
            lastCount = count;
        }
    }

    public static class Deduper implements Runnable {
        private final ConcurrentHashMap<String, Object> seen = new ConcurrentHashMap<>(100);
        private final BlockingQueue<byte[]> source;
        private final BlockingQueue<String> dest;

        public Deduper(BlockingQueue<byte[]> source, BlockingQueue<String> dest) {
            this.source = source;
            this.dest = dest;
        }

        @Override
        public void run() {
            try {
                while(true) {
                    String s = new String(source.take(), "UTF-8");
                    Object v = seen.get(s);
                    if(v == null) {
                        Object o = seen.putIfAbsent(s, obj);
                        if(o == null) {
                            dest.put(s);
                            continue;
                        }
                    }

                    duplicateCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("The impossible happend!");
            }
        }
    }
}
