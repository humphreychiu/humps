import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class FixLogAnalyser {

    public static final String SOH = String.valueOf('\001');

    private static final DateTimeFormatter FIX_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        String messageKeyTag = args.length > 1 ? args[1] : "11";
        Map<String, List<Map<String, String>>> keyedMessages = parse(args[0], messageKeyTag);

        printOrderAckTimes(keyedMessages);
        printCancelAckTimes(keyedMessages);
    }

    // messageKey -> {MsgType -> {tag, value}}
    private static Map<String, List<Map<String, String>>> parse(String file, String keyTag) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        Map<String, List<Map<String, String>>> keyedMsgs = new HashMap<>();

        bufferedReader.lines().forEach(line -> {
            System.out.println("line: " + line);
            Map<String, String> tvMap = new HashMap<>();
            //List<Map<String, String>> msgList = new ArrayList<>();
            Arrays.stream(line.split(SOH)).iterator().forEachRemaining(tvp -> {
                var tv = tvp.split("=");
                tvMap.put(tv[0], tv[1]);

                if (tv[0].equals(keyTag)) { //matching key
                    var msgList = keyedMsgs.computeIfAbsent(tv[1], k -> new ArrayList<>());
                    msgList.add(tvMap);
                }
            });
        });

        System.out.println("parsed " + keyedMsgs.size() + " keyed message.");

        return keyedMsgs;
    }

    private static void printOrderAckTimes(Map<String, List<Map<String, String>>> keyedMessages) {
        printSendAckTimes(
                keyedMessages,
                msg -> "D".equals(msg.get("35")),
                msg -> "8".equals(msg.get("35")) && "0".equals(msg.get("150")),
                "clordId\tsending time\tack time\tduration in millis");
/*        System.out.println("clordId\tsending time\tack time\tduration in millis");
        keyedMessages.forEach((clordId, msgs) -> {
            String sendingTime = msgs.stream()
                    .filter(message -> "D".equals(message.get("35")))
                    .findFirst()
                    .map(msg -> msg.get("52"))
                    .orElse("");
            String ackTime = msgs.stream()
                    .filter(msg -> "8".equals(msg.get("35")) && "0".equals(msg.get("150")))
                    .findFirst()
                    .map(msg -> msg.get("52"))
                    .orElse("");
            if (!"".equals(sendingTime)) {
                String timeDiff = ackTime.equals("") ? "" : "" + timeDiff(ackTime, sendingTime);
                System.out.println(clordId + "\t" + sendingTime + "\t" + ackTime + "\t" + timeDiff);
            }
        });*/
    }

    private static void printCancelAckTimes(Map<String, List<Map<String, String>>> keyedMessages) {
        printSendAckTimes(
                keyedMessages,
                msg -> "F".equals(msg.get("35")),
                msg -> "8".equals(msg.get("35")) && "4".equals(msg.get("150")),
                "clordId\tcxl time\tack time\tduration in millis");
/*        System.out.println("clordId\tcxl time\tack time\tduration in millis");
        keyedMessages.forEach((clordId, msgs) -> {
            String sendingTime = msgs.stream()
                    .filter(message -> "F".equals(message.get("35")))
                    .findFirst()
                    .map(msg -> msg.get("52"))
                    .orElse("");
            String ackTime = msgs.stream()
                    .filter(msg -> "8".equals(msg.get("35")) && "4".equals(msg.get("150")))
                    .findFirst()
                    .map(msg -> msg.get("52"))
                    .orElse("");
            if (!"".equals(sendingTime)) {
                String timeDiff = ackTime.equals("") ? "" : "" + timeDiff(ackTime, sendingTime);
                System.out.println(clordId + "\t" + sendingTime + "\t" + ackTime+ "\t" + timeDiff);
            }
        });*/
    }

    private static void printSendAckTimes(
            Map<String, List<Map<String, String>>> keyedMessages,
            Predicate<Map<String, String>> sendFilter,
            Predicate<Map<String, String>> ackFilter,
            String headers) {
        System.out.println(headers);
        keyedMessages.forEach((clordId, msgs) -> {
            String sendingTime = msgs.stream()
                    .filter(sendFilter)
                    .findFirst()
                    .map(msg -> msg.get("52"))
                    .orElse("");
            String ackTime = msgs.stream()
                    .filter(ackFilter)
                    .findFirst()
                    .map(msg -> msg.get("52"))
                    .orElse("");
            if (!"".equals(sendingTime)) {
                String timeDiff = ackTime.equals("") ? "" : "" + timeDiff(ackTime, sendingTime);
                System.out.println(clordId + "\t" + sendingTime + "\t" + ackTime + "\t" + timeDiff);
            }
        });
    }

    private static int timeDiff(String ack, String sent) {
        var sentTime = LocalDateTime.parse(sent, FIX_DATE_FORMAT);
        var ackTime = LocalDateTime.parse(ack, FIX_DATE_FORMAT);

        var duration = Duration.between(sentTime, ackTime);

        return duration.toMillisPart();
    }
}
