package com.codeslower.numbers.service;

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

public class ReaderTests {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private AtomicInteger receivedCount = new AtomicInteger(0);
    private Queue<byte[]> store;

    @Before
    public void setUp() throws Exception {
        store = new LinkedList<>();
    }

    @Test(expected = Main.TerminateException.class)
    public void testTerminate() throws Throwable {
        ByteArrayInputStream bs = new ByteArrayInputStream("terminate".getBytes(UTF8));
        ReadableByteChannel readableByteChannel = Channels.newChannel(bs);
        Main.Reader reader = new Main.Reader(readableByteChannel, store, receivedCount);
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
        ByteArrayInputStream bs = new ByteArrayInputStream("012345678\nterminate".getBytes(UTF8));
        ReadableByteChannel readableByteChannel = Channels.newChannel(bs);
        Main.Reader reader = new Main.Reader(readableByteChannel, store, receivedCount);
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
        ByteArrayInputStream bs = new ByteArrayInputStream("01234\nterminate".getBytes(UTF8));
        ReadableByteChannel readableByteChannel = Channels.newChannel(bs);
        Main.Reader reader = new Main.Reader(readableByteChannel, store, receivedCount);
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
        ByteArrayInputStream bs = new ByteArrayInputStream("1\n2".getBytes(UTF8));
        ReadableByteChannel readableByteChannel = Channels.newChannel(bs);
        Main.Reader reader = new Main.Reader(readableByteChannel, store, receivedCount);
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
    public void testGoodInput() throws InterruptedException {
        ByteArrayInputStream bs = new ByteArrayInputStream("012345678\n012345678\nbye".getBytes(UTF8));
        ReadableByteChannel readableByteChannel = Channels.newChannel(bs);
        Main.Reader reader = new Main.Reader(readableByteChannel, store, receivedCount);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> readerResult = executorService.submit(reader);
        while(! readerResult.isDone()) {
            Thread.sleep(10);
        }

        try {
            readerResult.get();
            assertFalse("Channel should be closed", readableByteChannel.isOpen());
            List<String> result = new ArrayList<>(2);
            result.add(new String(store.remove(), UTF8));
            result.add(new String(store.remove(), UTF8));
            List<String> expected = new ArrayList<>();
            expected.add("012345678");
            expected.add("012345678");
            assertEquals("Did not get bytes expected", expected, result);
        }
        catch(ExecutionException e) {
            fail("Should NOT have got ExecutionException");
        }

    }
}
