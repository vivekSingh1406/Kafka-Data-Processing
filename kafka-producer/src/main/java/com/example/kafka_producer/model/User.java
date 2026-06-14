package com.example.kafka_producer.model;

import lombok.Data;
import java.util.List;

@Data
public class User {

    private String firstName;
    private String lastName;
    private String email;
    private String phoneNumber;
    private String gender;
    private String department;
    private String designation;
    private String companyName;
    private Integer experience;
    private List<String> companyWork;
    private List<String> skills;
    private Address address;
}