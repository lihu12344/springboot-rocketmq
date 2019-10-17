package com.example.demo.producer;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TransactionProducerService {

    @Value("${rocketmq.namesrv}")
    private String namesrv;

    @Autowired
    private LocalExecutor localExecutor;

    private TransactionMQProducer producer;

    @PostConstruct
    public void initTransactionMQProducer(){
        producer=new TransactionMQProducer("transactionProducer");
        producer.setNamesrvAddr(namesrv);

        try{
            producer.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendInTransaction() {
        producer.setTransactionListener(localExecutor);
        try {
            for (int i = 0; i < 10; i++) {
                Message message = new Message("topic-3", "transaction", ("瓜田李下 事务消息"+Integer.toString(i)).getBytes());
                TransactionSendResult result=producer.sendMessageInTransaction(message,i);
                System.out.println(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void destroy(){
        if(producer!=null) {
            producer.shutdown();
        }
    }
}

@Component
class LocalExecutor implements TransactionListener{

    private ConcurrentHashMap<String, LocalTransactionState> map=new ConcurrentHashMap<>();

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        map.put(message.getTransactionId(),LocalTransactionState.UNKNOW);
        Integer i=Integer.valueOf(o.toString());
        if (i.equals(2)){
            System.out.println("本地函数出错，回滚事务消息");
            map.put(message.getTransactionId(),LocalTransactionState.ROLLBACK_MESSAGE);
        }else {
            map.put(message.getTransactionId(),LocalTransactionState.COMMIT_MESSAGE);
        }
        return map.get(message.getTransactionId());
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        return map.get(messageExt.getMsgId());
    }
}