/*
 * Author: cbedford
 * Date: 10/28/13
 * Time: 2:20 PM
 */


import java.io.*;
import java.net.Socket;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

public class ServerAndThreadCoordinationUtils {

    public static final String SENTINEL_FILE_PATH = "/tmp/go";

    /**
     *  Sets up a process termination task that will trigger if  the given number of milliseconds
     *  elapses and the test has not finished yet.  We exit the JVM rather than just throwing an
     *  exception because exceptions might be swallowed in the reams of output that could be produced
     *  by Kafka and Storm servers that are running on threads that would not be stopped if we limited
     *  ourselves to just throwing an exception.
     */
    public static Timer setMaxTimeToRunTimer(int millisecs) {
        Date timeLimit =
                new Date(new Date().getTime() +  millisecs);
        Timer timer = new Timer();

        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                System.out.println("aborting test !  Took too long");
                System.exit(-1);
            }
        }, timeLimit);

        return timer;
    }


    /**
     * Run in a tight sleep/wake loop until sentinel file (by default '/tmp/go') comes into
     * existence.  We use this method in cases where we want to pause the flow of a test
     * but still be able to look around within zookeeper. If we were to merely pause in the
     * debugger then when we tried to connect to zookeeper to look around we would find the
     * server to be unresponsive (since the debugger pauses the whole process.)  But if we use
     * the method below the zookeeper thread will still get some CPU cycles so we can connect to
     * it and examine its structure.
     */
    public static void pauseUntil(String path) {
        if (path == null) {
            path = SENTINEL_FILE_PATH;
        }
        boolean fileExists = false;
        while (!fileExists) {
            File pauseFile = new File(path);
            if (!pauseFile.exists()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                fileExists = true;
            }
        }
    }

    public static void removePauseSentinelFile() {
        File sentinel = new File(SENTINEL_FILE_PATH);
        //noinspection ResultOfMethodCallIgnored
        sentinel.delete();
        if (sentinel.exists()) {
            throw new RuntimeException("Could not delete sentinel file");
        }

    }


    public static String send4LetterWord(String host, int port, String cmd)
            throws IOException {
        System.out.println("connecting to " + host + " " + port);
        Socket sock = new Socket(host, port);
        BufferedReader reader = null;
        try {
            OutputStream outstream = sock.getOutputStream();
            outstream.write(cmd.getBytes());
            outstream.flush();
            // this replicates NC - close the output stream before reading
            sock.shutdownOutput();

            reader =
                    new BufferedReader(
                            new InputStreamReader(sock.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        } finally {
            sock.close();
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static boolean waitForServerUp(String host, int port, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                // if there are multiple hostports, just take the first one
                String result = send4LetterWord(host, port, "stat");
                System.out.println("result of send: " + result);
                if (result.startsWith("Zookeeper version:")) {
                    return true;
                }
            } catch (IOException e) {
                // ignore as this is expected
                System.out.println("server " + host + ":" + port + " not up " + e);
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

    public static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("FATAL ERROR");
            System.exit(-1);
        }
    }


    public static void countDown(CountDownLatch latch) {
        try {
            latch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("FATAL ERROR");
            System.exit(-1);
        }
    }

}
