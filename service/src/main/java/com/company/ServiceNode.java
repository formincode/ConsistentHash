package com.company;

import java.util.Objects;

public class ServiceNode implements Node {
    private final String idc;
    private final String ip;
    private final int port;

    public ServiceNode(String idc,String ip, int port) {
        this.idc = idc;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public String getKey() {
        return idc + "-"+ip+":"+port;
    }

    @Override
    public String toString(){
        return getKey();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServiceNode that = (ServiceNode) o;
        return port == that.port &&
            Objects.equals(idc, that.idc) &&
            Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idc, ip, port);
    }
}
