package com.example.kafka_producer.model;

import lombok.Data;

@Data
public class Address {

    private String houseNumber;
    private String city;
    private String state;
    private String country;
    private String pinCode;
}