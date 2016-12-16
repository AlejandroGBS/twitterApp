package com.master2016;

public class KafkaStart {

	KafkaTwitterProducer kafkaTwitterProducer = new KafkaTwitterProducer();

	public static void main(String[] args) {
		// TODO Auto-generated method stub


		if (args.length == 7) {

			Keys.mode = args[0];
			Keys.apiKey = args[1];
			Keys.apiSecret = args[2];
			Keys.tokenValue = args[3];
			Keys.tokenSecret = args[4];
			Keys.brokerUrl = args[5];
			Keys.fileName = args[6];
			
			for (int i = 0; i < args.length; i++) {
				System.out.println(args[i]);
			}

			KafkaStart kafkaStart = new KafkaStart();
			try {
				if(Keys.mode.compareTo("1") == 0){
					kafkaStart.kafkaTwitterProducer.fromFile();
				}else{					
					kafkaStart.kafkaTwitterProducer.fromTwitter();
				}
			} catch (Exception ex) {
				System.out.println(ex);
			}
		}




	}

}
