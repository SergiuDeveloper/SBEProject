package model;

public enum MasterMessageType {

    REGISTER_BROKER("REGISTER_BROKER"),
    REGISTER_SUBSCRIBER("REGISTER_SUBSCRIBER"),
    REGISTER_PUBLISHER("REGISTER_PUBLISHER"),
    SUBSCRIPTION_IN("SUBSCRIPTION_IN"),
    PUBLICATION_IN("PUBLICATION_IN"),
    PUBLICATION_OUT("PUBLICATION_OUT"),
    NO_PARENT_PEER("NO_PARENT_PEER");

    private final String messageType;

    MasterMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getMessageType() {
        return this.messageType;
    }
}
