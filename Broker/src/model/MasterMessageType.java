package model;

public enum MasterMessageType {

    REGISTER_BROKER("REGISTER_BROKER"),
    REGISTER_SUBSCRIBER("REGISTER_SUBSCRIBER"),
    REGISTER_PUBLISHER("REGISTER_PUBLISHER"),
    NO_PARENT_PEER("NO_PARENT_PEER");

    private final String messageType;

    MasterMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getMessageType() {
        return this.messageType;
    }
}
