import java.io.*;
import java.util.HashMap;

/**
 * Created by yhao on 2015/10/8.
 */
public class ProcessUUID {
    public static void main(String[] args)throws IOException {
        ProcessUUID process = new ProcessUUID();

        if (args.length == 4) {
            if (args[3].equals("true")) {
                process.preprocess(args[0], args[1], args[2]);
            } else if (args[3].equals("false")) {
                process.afterprocess(args[0], args[1], args[2]);
            }
        } else {
            System.out.println("输入参数个数错误！");
            System.exit(1);
        }

    }

    public void preprocess(String input, String outputData, String outputUUID)throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
        BufferedWriter bwData = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputData), "utf-8"));
        BufferedWriter bwUUID = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputUUID), "utf-8"));

        Long count = 0L;
        String line;
        String[] lineTokens = new String[2];
        while ((line = br.readLine()) != null) {
            if (line.contains("|") && line.split("\\|").length == 2) {
                lineTokens[0] = line.split("\\|")[0];
                lineTokens[1] = line.split("\\|")[1];
                bwUUID.write(count+ " " + lineTokens[0] + "\n");
                bwData.write(lineTokens[1] + "\n");
                if (count % 10000 == 0) {
                    System.out.println("computed: " + count);
                }
                count++;
            }
        }

        System.out.println("counts: " + count);

        br.close();
        bwUUID.close();
        bwData.close();
    }

    public void afterprocess(String inputData, String inputUUID, String output)throws IOException {
        BufferedReader brData = new BufferedReader(new InputStreamReader(new FileInputStream(inputData)));
        BufferedReader brUUID = new BufferedReader(new InputStreamReader(new FileInputStream(inputUUID)));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "utf-8"));

        HashMap<Long, String> uuidMap = new HashMap<Long, String>();
        String uuidLine;
        while ((uuidLine = brUUID.readLine()) != null) {
            uuidMap.put(Long.parseLong(uuidLine.split(" ")[0]), uuidLine.split(" ")[1]);
        }

        String dataLine;
        Long count = 0L;
        while ((dataLine = brData.readLine()) != null) {
            bw.write(uuidMap.get(count) + "|" + dataLine + "\n");
            count++;
        }

        brData.close();
        brUUID.close();
        bw.close();
    }
}
