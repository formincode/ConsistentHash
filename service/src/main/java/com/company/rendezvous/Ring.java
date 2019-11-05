package com.company.rendezvous;

import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

//Highest Random Weight Hashing

public class Ring {
    private Set<String> nodes = new HashSet<>();

    private Long ip2Long(String ip) {
        return Long.valueOf(ip.replace(".",""));
    }

    private Long weight(Long ipLong,String key) {
        int a = 1103515245;
        int b = 12345;
        Long hash = Hashing.murmur3_128().newHasher().putBytes(key.getBytes()).hash().asLong();
        return (a * ((a * ipLong + b) ^ hash) + b) % (2^31);
    }


    public void add(String node) {
        nodes.add(node);
    }

    public void remove(String node) {
        nodes.remove(node);
    }

    public Set<String> getNodes() {
        return nodes;
    }

    public String hash(String key) {
        TreeMap<Long,String> weights = new TreeMap<>();
        for (String node:nodes) {
            weights.put(weight(ip2Long(node),key),node);
        }

        return weights.lastEntry().getValue();
    }

    public static void main(String[] args) {

        List<String> randomStrings = new ArrayList<>();
        for (int i=0; i<5; i++) {
            randomStrings.add(i+"");

        }

        Ring ring = new Ring();
        ring.add("1.1.1.1");
        for (String str : randomStrings) {
            String node = ring.hash(str);
            System.out.println(node + " : " + str);
        }

        System.out.println("------Add two node---");

        ring.add("1.1.1.2");
        ring.add("1.1.1.3");
        for (String str : randomStrings) {
            String node = ring.hash(str);
            System.out.println(node + " : " + str);
        }

        System.out.println("------Remove a node---");

        ring.remove("1.1.1.3");
        for (String str : randomStrings) {
            String node = ring.hash(str);
            System.out.println(node + " : " + str);
        }
    }
}
