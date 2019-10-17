package com.example.demo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class ConsumerService {

    @Value("${rocketmq.consumerGroup}")
    private String consumerGroup;

    @Value("${rocketmq.namesrv}")
    private String namesrv;

    @PostConstruct
    public void consumeSync(){
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrv);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try{
            Thread.sleep(4000);
            consumer.subscribe("topic","*");
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try{
                    for(MessageExt messageExt:list){
                        String body=new String(messageExt.getBody());
                        System.out.println(body);
                        System.out.println("消费时间："+System.currentTimeMillis());
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }catch (Exception e){
                    System.out.println("消费失败");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }

        try{
            consumer.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void consumeOrder(){
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("consumerGroup"+2);
        consumer.setInstanceName("consumerGroup2");
        consumer.setNamesrvAddr(namesrv);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try{
            Thread.sleep(2000);
            consumer.subscribe("topic-2","order");
            consumer.registerMessageListener((MessageListenerOrderly) (list, consumeOrderlyContext) -> {
                try{
                   for(MessageExt messageExt:list){
                       String body=new String(messageExt.getBody());
                       System.out.println(body);
                       System.out.println("消费成功");
                   }

                   return ConsumeOrderlyStatus.SUCCESS;
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("消费失败");
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }

        try{
            consumer.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void consumeTransaction(){
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("transactionConsumer");
        consumer.setInstanceName("consumerGroup3");
        consumer.setNamesrvAddr(namesrv);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try{
            Thread.sleep(2000);
            consumer.subscribe("topic-3","transaction");
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try{
                    for(MessageExt messageExt:list){
                        String body=new String(messageExt.getBody());
                        System.out.println(body);
                        System.out.println("消费成功");
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }catch (Exception e){
                    System.out.println("消费失败");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }

        try{
            consumer.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}