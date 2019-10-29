package com.company;

public class ReplicaNode implements Node {

    private Node physicalNode;
    private int index;

    public ReplicaNode(Node physicalNode, int index) {
        this.index=index;
        this.physicalNode=physicalNode;
    }

    public String getKey() {
        return physicalNode.getKey() + "-" + index;
    }

    public boolean isVirtualNodeOf(Node node) {
        return physicalNode.getKey().equals(node.getKey());
    }

    public Node getPhysicalNode() {
        return physicalNode;
    }

}
