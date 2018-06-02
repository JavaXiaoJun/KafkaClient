package com.study.kafka;

import com.study.kafka.producer.KafkaProducerTool;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerTest extends Thread{

	static ExecutorService pool = Executors.newFixedThreadPool(10);
	KafkaProducerTool producer;
	/**
	 * producer for kafka 1.0
	 * @throws Exception
	 */
	@Test
	public void testProducer() throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ssecbigdata03:9092");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);//��Ϣ��������С
		props.put(ProducerConfig.LINGER_MS_CONFIG, 0);//send message without delay
		props.put(ProducerConfig.ACKS_CONFIG, "all");//��Ӧpartition��followerд�����غ�ŷ��سɹ���
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.newegg.ec.kafka.producer.partition.ItemPartitioner");
		props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("retries", 0);//������Ϣʧ�ܲ�����
		producer = KafkaProducerTool.getInstance(props, "kafka10test");
		/**
		 * �ݵ�producer ��producer.send���߼����ݵȵģ���������ͬ��Kafka��Ϣ��broker�˲����ظ�д����Ϣ��kafka��֤�ײ���־ֻ�־û�һ�Ρ�
		 *               �ݵ��Կ��Լ���ؼ�������consumerϵͳʵ����Ϣȥ�صĹ�����������������������ƣ�
		 *               1.����֤��������ݵ��� 2.����֤��Ựʵ���ݵ��ԣ�ͬһ��producer��������Ҳ����֤��
		 * ֻ�����ݵ�produce��enable.idempotence=true
		 */
		//props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

		/**
		 * �����͵�producer:����һ��Ӧ�÷�����Ϣ�����partitions����topics,transactional��֤ԭ���Ե�д�뵽�������
		 * ʹ��transaction����Ҫ��֤retries != 0 && acks = all (����follower����ȷ�Ͻ��յ�����) && enable.idempotence=true
		 */
		//props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");

		Random random = new Random();
		try {
			while(true){
				int num = random.nextInt(20)%(20-0+1) + 0;
				String key = "15-WWW-80" + num +"|USA|1003";
				byte[] value = String.valueOf(num).getBytes("UTF-8");
				pool.submit(new ProducerTask(key,value));
				sleep(2000);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	class ProducerTask implements Runnable{

		private String key;
		private byte[] value;

		public ProducerTask(String key,byte[] value){
			this.key = key;
			this.value = value;
		}

		@Override
		public void run() {
			producer.append(key, value);
			System.out.println(new Date() + " : "+  key+"");
		}
	}

}