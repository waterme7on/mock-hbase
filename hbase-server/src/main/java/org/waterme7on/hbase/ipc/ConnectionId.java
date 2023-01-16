package org.waterme7on.hbase.ipc;

import java.util.Objects;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.User;

/**
 * This class holds the address and the user ticket, etc. The client connections
 * to servers are
 * uniquely identified by (remoteAddress, ticket, serviceName);
 */
class ConnectionId {
    private static final int PRIME = 16777619;
    final User ticket;
    final String serviceName;
    final Address address;

    public ConnectionId(User ticket, String serviceName, Address address) {
        this.address = address;
        this.ticket = ticket;
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return this.serviceName;
    }

    public Address getAddress() {
        return address;
    }

    public User getTicket() {
        return ticket;
    }

    @Override
    public String toString() {
        return this.address.toString() + "/" + this.serviceName + "/" + this.ticket;
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ConnectionId)) {
            return false;
        }
        ConnectionId id = (ConnectionId) obj;
        return address.equals(id.address)
                && ((ticket != null && ticket.equals(id.ticket)) || (ticket == id.ticket))
                && Objects.equals(this.serviceName, id.serviceName);
    }

    @Override // simply use the default Object#hashcode() ?
    public int hashCode() {
        return hashCode(ticket, serviceName, address);
    }

    public static int hashCode(User ticket, String serviceName, Address address) {
        return (address.hashCode()
                + PRIME * (PRIME * serviceName.hashCode() ^ (ticket == null ? 0 : ticket.hashCode())));
    }
}
