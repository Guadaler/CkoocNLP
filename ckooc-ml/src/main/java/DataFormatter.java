import java.io.*;
import java.util.HashMap;

/**
 * Created by yhao on 2015/12/16.
 */
public class DataFormatter {

    public static void main(String[] args)throws IOException{

        HashMap<String, Integer> classMap = new HashMap<String, Integer>();
        classMap.put("agriculture", 0);
        classMap.put("automobile", 1);
        classMap.put("commuincation", 2);
        classMap.put("digital", 3);
        classMap.put("economy", 4);
        classMap.put("entertainment", 5);
        classMap.put("estates", 6);
        classMap.put("fashion", 7);
        classMap.put("game", 8);
        classMap.put("gongyi", 9);
        classMap.put("health", 10);
        classMap.put("history", 11);
        classMap.put("it", 12);
        classMap.put("law", 13);
        classMap.put("learning", 14);
        classMap.put("military", 15);
        classMap.put("news", 16);
        classMap.put("sports", 17);
        classMap.put("trans", 18);
        classMap.put("travel", 19);

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[1])));

        String line;
        DataFormatter df = new DataFormatter();
        while ((line = br.readLine()) != null) {
            String text = df.FormatDocOfJGLLDA(line, classMap);
            bw.write(text + "\n");
        }

        br.close();
        bw.close();
    }

    public String FormatDocOfJGLLDA(String text, HashMap<String, Integer> classMap) {
        String[] tokens = text.split(" ");
        String uuid = tokens[0];
        String classify = uuid.split("\\_")[0].split("\\*")[1];

        String temp = "";
        int topic;
        Double weight;
        for (int i = 1; i < tokens.length; i++) {
            String token = tokens[i];
            topic = Integer.parseInt(token.split("\\:")[0]);
            weight = Double.parseDouble(token.split("\\:")[1]);

            temp += topic + 1 + ":" + weight + " ";
        }
        return classMap.get(classify) + " " + temp.substring(0, temp.length() - 1);
    }
}
