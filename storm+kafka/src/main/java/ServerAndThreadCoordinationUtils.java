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
import java.util.concurrent.TimeUnit;

public class ServerAndThreadCoordinationUtils {

    public static void setMaxTimeToRunTimer(int millisecs) {
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
    }

    public static void pauseUntil() {
        boolean fileExists = false;
        while (!fileExists) {
            File pauseFile = new File("/tmp/go");
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
