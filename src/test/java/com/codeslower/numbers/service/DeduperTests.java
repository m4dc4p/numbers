package com.codeslower.numbers.service;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DeduperTests {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    Main.Deduper deduper;
    BlockingQueue<byte[]> source;
    BlockingQueue<String> dest;
    AtomicInteger duplicateCounter;

    @Before
    public void setUp() {
        source = new ArrayBlockingQueue<>(10);
        dest = new ArrayBlockingQueue<>(10);
        duplicateCounter = new AtomicInteger(0);
        deduper = new Main.Deduper(source, dest, duplicateCounter);
    }

    @Test
    public void testDedupe() throws InterruptedException {
        List<byte[]> initialBytes = new ArrayList<>();
        initialBytes.add("123".getBytes(UTF8));
        initialBytes.add("456".getBytes(UTF8));
        initialBytes.add("123".getBytes(UTF8));
        initialBytes.add("1456".getBytes(UTF8));

        source.addAll(initialBytes);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        Future<?> submit = executorService.scheduleAtFixedRate(deduper, 0, 10, TimeUnit.MILLISECONDS);
        while(! source.isEmpty()) {
            Thread.sleep(10);
        }
        executorService.shutdown();

        assertEquals("Should have 1 duplicate", 1, duplicateCounter.get());
        assertEquals("Destination queue should only have 3 elements", 3, dest.size());
        List<String> result = new ArrayList<>();
        dest.drainTo(result);
        assertUnique("All list elements should be unique", result);
    }

    private <E extends Comparable<? super E>> void assertUnique(String msg, List<E> result) {
        Collections.sort(result);
        Iterator<E> iterator = result.iterator();
        if(iterator.hasNext()) {
            E lastElem = iterator.next();
            while(iterator.hasNext()) {
                E next = iterator.next();
                if(lastElem.equals(next)) {
                    fail(msg);
                }
                lastElem = next;
            }
        }
    }
}
