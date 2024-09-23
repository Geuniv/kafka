package kopo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Scanner;

public class App {

    // 토픽 이름
    private static final  String TOPIC_NAME = "quickstart-events";

    // 종료할 메시지
    private static final  String FIN_MASSAGE = "exit";

    private final static Logger Log = LoggerFactory.getLogger("App");

    public static void main( String[] args ) {

        // 카프카 환경 설정 객체
        Properties properties = new Properties();

        // 접속할 카프카 서버 정보
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka_server:9092");

        // 전달할 데이터 구조는 Map구조인 키와 값 형태의 구조로 메시지를 보냄
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 전달할 데이터 구조는 Map구조인 키의 데이터타입 : 문자열
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 데이터 전달하기 위한 KafkaProducer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Exit 메시지 입력 전까지 반복함
        while (true) {

            // 시스템으로부터 입력받기
            Scanner sc = new Scanner(System.in);
            System.out.print("Send Message >> ");
            String message = sc.nextLine(); // 전달할 메시지

            // 토픽에 전달할 객체 ProducerRecord 생성함
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, message);

            try {

                // 메시지 전달하기
                producer.send(record);

            } catch (Exception e) {
                Log.info("Error : " + e.toString());

            } finally {

                // 메시지 전달하고, 메모리 비우기
                producer.flush();
            }

            // exit 메시지가 입력되면, kafka Clients 종료
            if (message.equals(FIN_MASSAGE)) {
                producer.close();
                sc.close();
                break;
            }
        }
    }
}
