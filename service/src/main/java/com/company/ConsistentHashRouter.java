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

    /**
     *
     * @param pNodes collections of physical nodes
     * @param vNodeCount amounts of virtual nodes
     */
    public ConsistentHashRouter(Collection<Node> pNodes, int vNodeCount) {
        if (pNodes != null) {
            for (Node pNode : pNodes) {
                addNode(pNode, vNodeCount);
            }
        }
    }

    /**
     * add physic node to the hash ring with some virtual nodes
     * @param pNode physical node needs added to hash ring
     * @param vNodeCount the number of virtual node of the physical node. Value should be greater than or equals to 0
     */
    public void addNode(Node pNode, int vNodeCount) {
        if (vNodeCount < 0) throw new IllegalArgumentException("illegal virtual node counts :" + vNodeCount);
        int existingReplicas = getExistingReplicas(pNode);
        for (int i = 0; i < vNodeCount; i++) {
            ReplicaNode vNode = new ReplicaNode(pNode, i + existingReplicas);
            ring.put(hash(vNode.getKey()), vNode);
        }
    }

    /**
     * remove the physical node from the hash ring
     * @param pNode
     */
    public void removeNode(Node pNode) {
        Iterator<Long> it = ring.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            ReplicaNode replicaNode = ring.get(key);
            if (replicaNode.isVirtualNodeOf(pNode)) {
                it.remove();
            }
        }
    }

    /**
     * with a specified key, route the nearest Node instance in the current hash ring
     * @param objectKey the object key to find a nearest Node
     * @return
     */
    public Node routeNode(String objectKey) {
        if (ring.isEmpty()) {
            return null;
        }
        Long hashVal = hash(objectKey);
        SortedMap<Long, ReplicaNode> tailMap = ring.tailMap(hashVal);
        Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(nodeHashVal).getPhysicalNode();
    }


    public int getExistingReplicas(Node pNode) {
        int replicas = 0;
        for (ReplicaNode vNode : ring.values()) {
            if (vNode.isVirtualNodeOf(pNode)) {
                replicas++;
            }
        }
        return replicas;
    }


    public long hash(String key) {
        return Hashing.md5().hashString(key, Charset.defaultCharset()).asLong();
    }

}
