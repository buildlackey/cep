/*
 * Author: cbedford
 * Date: 11/10/13
 * Time: 7:57 PM
 */


import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;
import sun.misc.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class SentimentClassifier {
    String[] categories;
    LMClassifier clazz;

    public static void main(String[] args) throws IOException {
        SentimentClassifier classifier =  new SentimentClassifier();

        File tweets =   new File("/tmp/tweets");
        BufferedReader br = new BufferedReader(new FileReader(tweets));
        String line;
        while ((line = br.readLine()) != null) {
            String classification = classifier.classify(line);
            System.out.println(classification + ": | " + line);
        }
        br.close();
    }

    public SentimentClassifier() {
        try {
            File serializedClassifier =   new File("/home/chris/esper/TwitterSentiment-master/classifier.obj");
            clazz = (LMClassifier) AbstractExternalizable.readObject(serializedClassifier);
            categories = clazz.categories();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String classify(String text) {
        ConditionalClassification classification = clazz.classify(text);
        return classification.bestCategory();
    }
}
