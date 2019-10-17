package com.example.demo.controller;

import com.example.demo.producer.ProducerService;
import com.example.demo.producer.TransactionProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    @Autowired
    private ProducerService producerService;

    @Autowired
    private TransactionProducerService transactionProducerService;

    @RequestMapping("/send")
    public String hello(){
        producerService.sendInSync();

        return "success";
    }

    @RequestMapping("/send2")
    public String hello2(){
        producerService.sendInAsync();

        return "success 2";
    }

    @RequestMapping("/send3")
    public String hello3(){
        producerService.sendOneWay();

        return "success 3";
    }

    @RequestMapping("/sendDelay")
    public String hello4(){
        producerService.sendDelay();

        return "success 4";
    }

    @RequestMapping("/sendInorder")
    public String hello5(){
        producerService.sendInOrder();

        return "success 5";
    }

    @RequestMapping("/sendInTransaction")
    public String hello6(){
        transactionProducerService.sendInTransaction();

        return "success 6";
    }
}
