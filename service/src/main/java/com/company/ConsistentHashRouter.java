package com.company;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRouter {
    private final SortedMap<Long, ReplicaNode> ring = new TreeMap<>();

    public ConsistentHashRouter(Collection<Node> phyicalNodes, int replicationFactor) {
        if (phyicalNodes != null) {
            for (Node phyicalNode : phyicalNodes) {
                addNode(phyicalNode, replicationFactor);
            }
        }
    }

    public void addNode(Node phyicalNode, int replicationFactor) {
        if (replicationFactor < 0) throw new IllegalArgumentException("illegal replica node counts :" + replicationFactor);
        int existingReplicas = getExistingReplicas(phyicalNode);
        for (int i = 0; i < replicationFactor; i++) {
            ReplicaNode replicaNode = new ReplicaNode(phyicalNode, i + existingReplicas);
            ring.put(hash(replicaNode.getKey()), replicaNode);
        }
    }

    public void removeNode(Node phyicalNode) {
        Iterator<Long> it = ring.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            ReplicaNode replicaNode = ring.get(key);
            if (replicaNode.isVirtualNodeOf(phyicalNode)) {
                it.remove();
            }
        }
    }

    public Node routeNode(String objectKey) {
        if (ring.isEmpty()) {
            return null;
        }
        Long hashVal = hash(objectKey);
        //find the submap where the keys of the submap are less than the hash of the objectKey
        SortedMap<Long, ReplicaNode> tailMap = ring.tailMap(hashVal);
        //either use the first entry in the sub map or the first key thus a ring going clockwise
        Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(nodeHashVal).getPhysicalNode();
    }


    public int getExistingReplicas(Node phyicalNode) {
        int replicas = 0;
        for (ReplicaNode replicaNode : ring.values()) {
            if (replicaNode.isVirtualNodeOf(phyicalNode)) {
                replicas++;
            }
        }
        return replicas;
    }


    //we decouple the key from the # of nodes
    public long hash(String key) {
        return Math.abs(Hashing.md5().hashString(key, Charset.defaultCharset()).asLong());
    }

}
