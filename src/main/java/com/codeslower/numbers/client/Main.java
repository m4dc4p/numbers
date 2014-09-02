package com.codeslower.numbers.client;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.Random;

public class Main {

    private static final int MAX_NUM = 1_000_000_000;
    private static boolean quiet;

    public static void main(String [] args) throws IOException, ParseException {
        RateLimiter r = null;

        Options opts = new Options();
        opts.addOption("v","val",true, "Dependent on client.");
        opts.addOption("q","quiet", false, "Quiet output.");
        opts.addOption("h","help", false, "Display help.");
        opts.addOption("b", "bad", false, "Use bad input client. val - between 0 & 1, representing percentage of time when bad input is sent");
        opts.addOption("s", "steady", false, "Use steady client. val - rate of requests in 1000s per second.");
        opts.addOption("r", "repeat", false, "Use repeating client. val - rate of requests in 1000s per second.");

        CommandLine cmds = new BasicParser().parse(opts, args);
        quiet = cmds.hasOption("quiet");

        if(cmds.hasOption("help")) {
            printUsageAndExit(opts);
        }

        Client client;
        if(cmds.hasOption("bad")) {
            if(! cmds.hasOption("val")) {
                printUsageAndExit(opts, "'val' argument must be present.");
            }
            client = new BadInputClient(Double.parseDouble(cmds.getOptionValue("val")));
        }
        else if(cmds.hasOption("steady")) {
            if(! cmds.hasOption("val")) {
                printUsageAndExit(opts, "'val' argument must be present.");
            }
            client = new SteadyClient(Double.parseDouble(cmds.getOptionValue("val")));
        }
        else if(cmds.hasOption("repeat")) {
            if(! cmds.hasOption("val")) {
                printUsageAndExit(opts, "'val' argument must be present.");
            }
            client = new RepeatingClient(Double.parseDouble(cmds.getOptionValue("val")));
        }
        else {
            client = new SpeedClient();
        }

        client.go();
    }

    private static void printUsageAndExit(Options opts, String message) {
        System.out.println(message);
        printUsageAndExit(opts);
    }

    private static void printUsageAndExit(Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Main.class.getCanonicalName(), opts);
        System.exit(0);
    }

    public abstract static class Client {
        protected abstract int getNext();

        public void go() throws IOException {
            Socket socket = new Socket("localhost", 4000);
            OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(), "UTF-8");
            while (true) {
                try {
                    if (!quiet) {
                        System.out.print(".");
                    }
                    out.write(String.format("%09d\n", getNext()));
                } catch (SocketException e) {
                    socket = new Socket("localhost", 4000);
                    out = new OutputStreamWriter(socket.getOutputStream(), "UTF-8");
                    if(!quiet) {
                        System.out.print("\n");
                    }
                }
            }
        }
    }

    public static class SpeedClient extends Client {
        private Random random = new Random();

        @Override
        protected int getNext() {
            return random.nextInt(MAX_NUM);
        }
    }

    public static class SteadyClient extends Client {
        private final RateLimiter limiter;
        private Random random = new Random();

        /**
         *
         * @param rate In 1000 requests / second.
         */
        public SteadyClient(double rate) {
            this.limiter = RateLimiter.create(rate * 1000.0);
        }

        protected int getNext() {
            limiter.acquire();
            return random.nextInt(MAX_NUM);
        }
    }

    public static class RepeatingClient extends Client {
        private final RateLimiter limiter;

        /**
         *
         * @param rate In 1000 requests / second.
         */
        public RepeatingClient(double rate) {
            this.limiter = RateLimiter.create(rate * 1000.0);
        }

        protected int getNext() {
            limiter.acquire();
            return 4;
        }
    }

    /**
     * A client that generates bad input some percent
     * of the time.
     */
    public static class BadInputClient extends Client {

        private final double percent;
        private Random random = new Random();

        /**
         *
         * @param percent A value between 0 and 1.
         */
        public BadInputClient(double percent) {
            Preconditions.checkArgument(percent >=0 && percent <= 1);
            this.percent = percent * MAX_NUM;
        }

        @Override
        protected int getNext() {
            int n = random.nextInt(MAX_NUM);
            if(n < percent) {
                return MAX_NUM + 1;
            }

            return n;
        }
    }
}
