package com.example.demo.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
public class ProducerService {

    @Value("${rocketmq.producerGroup}")
    private String producerGroup;

    @Value("${rocketmq.namesrv}")
    private String namesrv;

    private DefaultMQProducer producer;

    @PostConstruct
    public void initDefaultMQProducer(){
        producer=new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrv);
        producer.setRetryTimesWhenSendFailed(2);

        try{
            producer.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendInSync(){
        for (int i=0;i<100;i++){
            try{
                Message message=new Message("topic","sync",("瓜田李下"+i).getBytes());
                SendResult result=producer.send(message);
                System.out.println(result);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void sendInAsync(){
        for (int i=0;i<100;i++){
            try{
                Message message=new Message("topic","async",("瓜田李下 2"+i).getBytes());
                producer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println("发送成功");
                        System.out.println(sendResult);
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        System.out.println("发送失败");
                        System.out.println(throwable.getMessage());
                    }
                });
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void sendOneWay(){
        for (int i=0;i<100;i++){
            try{
                Message message=new Message("topic","oneWay",("瓜田李下 3"+i).getBytes());
                producer.sendOneway(message);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void sendDelay(){
        try{
            Message message=new Message("topic","delay",("瓜田李下 延时消息").getBytes());
            message.setDelayTimeLevel(2);
            SendResult result=producer.send(message);
            System.out.println(result);
            System.out.println("发送时间: "+System.currentTimeMillis());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendInOrder(){
        try{
            for(int i=0;i<100;i++){
                Message message=new Message("topic-2","order",("瓜田李下 顺序消息 "+i).getBytes());
                SendResult result=producer.send(message, (list, message1, o) -> {
                    int index=Integer.parseInt(o.toString());

                    return list.get(index);
                },1);
                System.out.println(result);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void destroy(){
        if(producer!=null){
            producer.shutdown();
        }
    }
}
