package model;

public enum BrokerMessageType {

    REGISTER_SUBSCRIPTION("REGISTER_SUBSCRIPTION"),
    TRANSMIT_PUBLICATION("TRANSMIT_PUBLICATION");

    private final String messageType;

    BrokerMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getMessageType() {
        return this.messageType;
    }
}
