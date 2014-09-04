package com.codeslower.numbers.service;

import com.google.common.base.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestReader {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private AtomicInteger receivedCount = new AtomicInteger(0);
    private BlockingQueue<byte[]> store;
    private ReadWaiter noWaiter = new ReadWaiter() {
        @Override
        public void waitToRead() throws IOException {
            return;
        }

        @Override
        public void close() {
            return;
        }
    };
    private Supplier<ReadWaiter> noWaiterSupplier = new Supplier<ReadWaiter>() {
        @Override
        public ReadWaiter get() {
            return noWaiter;
        }
    };

    @Before
    public void setUp() throws Exception {
        store = new LinkedBlockingQueue<>();
        System.clearProperty(Reader.LINE_SEP_PROPERTY);
    }

    @After
    public void tearDown() {
        System.clearProperty(Reader.LINE_SEP_PROPERTY);
    }

    @Test(expected = Main.TerminateException.class)
    public void testTerminate() throws Throwable {
        ByteArrayInputStream bs = new ByteArrayInputStream("terminate".getBytes(UTF8));
        ReadableByteChannel readableByteChannel = Channels.newChannel(bs);
        Reader reader = new Reader(readableByteChannel, store, receivedCount, noWaiterSupplier);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        Future<?> readerResult = executorService.scheduleWithFixedDelay(reader, 0, 10, TimeUnit.MILLISECONDS);
        while(! readerResult.isDone()) {
            Thread.sleep(10);
        }
        executorService.shutdown();

        try {
            readerResult.get();
            fail("Should have got ExecutionException");
        }
        catch(ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = Main.TerminateException.class)
    public void testReadAndTerminate() throws Throwable {
        ReadableByteChannel readableByteChannel = Channels.newChannel(
                new ByteArrayInputStream("012345678\nterminate".getBytes(UTF8)));
        Reader reader = new Reader(readableByteChannel, store, receivedCount, noWaiterSupplier);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> readerResult = executorService.submit(reader);
        while(! readerResult.isDone()) {
            Thread.sleep(10);
        }

        try {
            readerResult.get();
            fail("Should have got ExecutionException");
        }
        catch(ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testReadAndClose() throws Throwable {
        ReadableByteChannel readableByteChannel = Channels.newChannel(
                new ByteArrayInputStream("01234\nterminate".getBytes(UTF8)));
        Reader reader = new Reader(readableByteChannel, store, receivedCount, noWaiterSupplier);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> readerResult = executorService.submit(reader);
        while(! readerResult.isDone()) {
            Thread.sleep(10);
        }

        try {
            readerResult.get();
            assertFalse("Channel should be closed", readableByteChannel.isOpen());
        }
        catch(ExecutionException e) {
            fail("Should NOT have got ExecutionException");
        }
    }

    @Test
    public void testBadInput() throws InterruptedException, IOException {
        ReadableByteChannel readableByteChannel = Channels.newChannel(
                new ByteArrayInputStream("1\n2".getBytes(UTF8)));
        verifyBadInput(readableByteChannel);
    }

    private void verifyBadInput(ReadableByteChannel readableByteChannel) throws InterruptedException {
        Reader reader = new Reader(readableByteChannel, store, receivedCount, noWaiterSupplier);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> readerResult = executorService.submit(reader);
        while(! readerResult.isDone()) {
            Thread.sleep(10);
        }

        try {
            readerResult.get();
            assertEquals("No elements should be in the destination queue.", store.size(), 0);
            assertFalse("Channel should be closed", readableByteChannel.isOpen());
        }
        catch(ExecutionException e) {
            fail("Should NOT have got ExecutionException");
        }
    }

    @Test
    public void testGoodInput() throws InterruptedException {
        ReadableByteChannel readableByteChannel = Channels.newChannel(
                new ByteArrayInputStream("012345678\n012345678\nbye".getBytes(UTF8)));
        List<String> expected = new ArrayList<>();
        expected.add("012345678");
        expected.add("012345678");

        verifyGoodInput(System.lineSeparator(), expected, readableByteChannel);
    }


    @Test
    public void testNewLines() throws InterruptedException {
        String win = "\r\n";
        String nix = "\n";
        String weird = "\t";

        ReadableByteChannel winBytes = Channels.newChannel(new ByteArrayInputStream("012345678\r\n012345678\r\nbye".getBytes(UTF8)));
        ReadableByteChannel nixBytes = Channels.newChannel(new ByteArrayInputStream("012345678\n012345678\nbye".getBytes(UTF8)));
        ReadableByteChannel weirdBytes = Channels.newChannel(new ByteArrayInputStream("012345678\t012345678\tbye".getBytes(UTF8)));

        ArrayList<String> expected = new ArrayList<>();
        expected.add("012345678");
        expected.add("012345678");

        verifyGoodInput(win, expected, winBytes);
        verifyGoodInput(nix, expected, nixBytes);
        verifyGoodInput(weird, expected, weirdBytes);

        System.setProperty(Reader.LINE_SEP_PROPERTY, win);
        verifyBadInput(nixBytes);
        verifyBadInput(weirdBytes);

        System.setProperty(Reader.LINE_SEP_PROPERTY, nix);
        verifyBadInput(winBytes);
        verifyBadInput(weirdBytes);

        System.setProperty(Reader.LINE_SEP_PROPERTY, weird);
        verifyBadInput(winBytes);
        verifyBadInput(nixBytes);

    }

    private void verifyGoodInput(String newline, List<String> expected, ReadableByteChannel bytes) throws InterruptedException {
        System.setProperty(Reader.LINE_SEP_PROPERTY, newline);
        Reader reader = new Reader(bytes, store, receivedCount, noWaiterSupplier);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> readerResult = executorService.submit(reader);
        while(! readerResult.isDone()) {
            Thread.sleep(10);
        }

        try {
            readerResult.get();
            assertFalse("Channel should be closed", bytes.isOpen());
            assertEquals("Result queue should have the same number of elements as expected list.", expected.size(), store.size());
            int i = 0;
            for(String exp : expected) {
                assertEquals(String.format("Expected element at index %s did not match.", i), exp, new String(store.remove(), UTF8));
                i+=1;
            }
        }
        catch(ExecutionException e) {
            fail("Should NOT have got ExecutionException");
        }
    }
}
