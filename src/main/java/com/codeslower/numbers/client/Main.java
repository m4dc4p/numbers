package com.codeslower.numbers.client;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Random;

public class Main {
    public static void main(String [] args) throws IOException {
        Socket client = new Socket("localhost", 4000);
        OutputStreamWriter out = new OutputStreamWriter(client.getOutputStream(), "UTF-8");
        Random random = new Random();
        while(true) {
            int i = random.nextInt(1_000_000_000);
            String num = String.format("%09d\n", i);
            System.out.print(".");
            out.write(num);
        }
    }
}
