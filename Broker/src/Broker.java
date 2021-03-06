import model.BrokerMessageType;
import model.MasterMessageType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("FieldCanBeLocal")
public abstract class Broker<Pub, Sub> {

    private final Class<Pub> pubClass;
    private final Class<Sub> subClass;

    private boolean isRoot;

    private final Map<Socket, Triple<BufferedReader, PrintWriter, List<Sub>>> subscribersMap;
    private final Object subscribersMapLock;

    private final String masterHost;
    private final int masterPort;

    private Socket masterClientSocket;
    private BufferedReader masterClientIn;
    private PrintWriter masterClientOut;

    private String brokerPeerHost;
    private int brokerPeerPort;
    private Socket brokerPeerClientSocket;
    private BufferedReader brokerPeerIn;
    private PrintWriter brokerPeerOut;

    private ServerSocket brokerPeersServerSocket;
    private final Map<Pair<String, Integer>, Triple<Socket, BufferedReader, PrintWriter>> brokerPeersSocketsMap;
    private final Object brokerPeersSocketsMapLock;
    private final Map<Pair<String, Integer>, List<Sub>> routingTable;
    private final Object routingTableLock;

    private ServerSocket publishersServerSocket;

    private ServerSocket subscribersServerSocket;

    public Broker(String masterHost, int masterPort, Class<Pub> pubClass, Class<Sub> subClass) {
        this.pubClass = pubClass;
        this.subClass = subClass;

        this.isRoot = false;

        this.subscribersMap = new HashMap<>();
        this.subscribersMapLock = new Object();

        this.masterHost = masterHost;
        this.masterPort = masterPort;

        this.brokerPeersSocketsMap = new HashMap<>();
        this.brokerPeersSocketsMapLock = new Object();

        this.routingTable = new HashMap<>();
        this.routingTableLock = new Object();
    }

    public void start() throws IOException {
        this.startBrokerPeersServer();
        this.startPublishersServer();
        this.startSubscribersServer();

        this.getBrokerPeerHostPort();
        if (this.isRoot) {
            return;
        }

        this.connectToBrokerPeer();
    }

    protected abstract boolean publicationMatchesSubscription(Pub publication, Sub subscription);

    private void startBrokerPeersServer() throws IOException {
        this.brokerPeersServerSocket = new ServerSocket(0);
        new Thread(() -> {
            while (true) {
                Socket brokerPeerSocket;
                try {
                    brokerPeerSocket = this.brokerPeersServerSocket.accept();
                } catch (IOException e) {
                    e.printStackTrace();
                    continue;
                }

                new Thread(() -> {
                    BufferedReader brokerPeerChildIn;
                    PrintWriter brokerPeerChildOut;
                    String brokerPeerChildHost;
                    int brokerPeerChildPort;
                    try {
                        brokerPeerChildIn = new BufferedReader(new InputStreamReader(brokerPeerSocket.getInputStream()));
                        brokerPeerChildOut = new PrintWriter(brokerPeerSocket.getOutputStream(), true);
                        brokerPeerChildHost = ((InetSocketAddress) brokerPeerSocket.getRemoteSocketAddress()).getAddress().toString().substring(1);
                        brokerPeerChildPort = Integer.parseInt(brokerPeerChildIn.readLine());
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }

                    synchronized (this.brokerPeersSocketsMapLock) {
                        this.brokerPeersSocketsMap.put(
                                new ImmutablePair<>(brokerPeerChildHost, brokerPeerChildPort),
                                new ImmutableTriple<>(brokerPeerSocket, brokerPeerChildIn, brokerPeerChildOut)
                        );
                    }

                    synchronized (this.routingTableLock) {
                        this.routingTable.put(new ImmutablePair<>(brokerPeerChildHost, brokerPeerChildPort), new ArrayList<>());
                    }

                    this.masterClientOut.println(String.format("%s %s %d", MasterMessageType.REGISTER_BROKER.getMessageType(), brokerPeerChildHost, brokerPeerChildPort));

                    this.awaitBrokerPeerMessages(
                            new ImmutablePair<>(brokerPeerChildHost, brokerPeerChildPort),
                            new ImmutableTriple<>(brokerPeerSocket, brokerPeerChildIn, brokerPeerChildOut)
                    );
                }).start();
            }
        }).start();
    }

    private void startPublishersServer() throws IOException {
        this.publishersServerSocket = new ServerSocket(0);
        new Thread(() -> {
            while (true) {
                Socket publisherSocket;
                BufferedReader publisherIn;
                try {
                    publisherSocket = this.publishersServerSocket.accept();
                    publisherIn = new BufferedReader(new InputStreamReader(publisherSocket.getInputStream()));
                } catch (IOException e) {
                    e.printStackTrace();
                    continue;
                }

                this.masterClientOut.println(MasterMessageType.REGISTER_PUBLISHER);

                new Thread(() -> {
                    while (true) {
                        String serializedPublication;
                        try {
                            serializedPublication = publisherIn.readLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                            continue;
                        }
                        this.handlePublication(null, BrokerMessageType.TRANSMIT_PUBLICATION.getMessageType(), serializedPublication);

                        this.masterClientOut.println(String.format("%s %s", MasterMessageType.PUBLICATION_IN, serializedPublication.replaceAll("\\s+","")));
                    }
                }).start();
            }
        }).start();
    }

    private void startSubscribersServer() throws IOException {
        this.subscribersServerSocket = new ServerSocket(0);
        new Thread(() -> {
            while (true) {
                Socket subscriberSocket;
                BufferedReader subscriberIn;
                PrintWriter subscriberOut;
                try {
                    subscriberSocket = this.subscribersServerSocket.accept();
                    subscriberIn = new BufferedReader(new InputStreamReader(subscriberSocket.getInputStream()));
                    subscriberOut = new PrintWriter(subscriberSocket.getOutputStream(), true);
                } catch (IOException e) {
                    e.printStackTrace();
                    continue;
                }

                synchronized (this.subscribersMapLock) {
                    this.subscribersMap.put(
                            subscriberSocket,
                            new ImmutableTriple<>(subscriberIn, subscriberOut, new ArrayList<>())
                    );
                }

                this.masterClientOut.println(MasterMessageType.REGISTER_SUBSCRIBER);

                new Thread(() -> {
                    while (true) {
                        String serializedSubscription;
                        Sub subscription;
                        try {
                            serializedSubscription = subscriberIn.readLine();
                            subscription = new ObjectMapper().readValue(serializedSubscription, this.subClass);
                        } catch (IOException e) {
                            e.printStackTrace();
                            continue;
                        }

                        synchronized (this.subscribersMapLock) {
                            this.subscribersMap.get(subscriberSocket).getRight().add(subscription);
                        }

                        List<PrintWriter> peerWriters;
                        synchronized (this.brokerPeersSocketsMapLock) {
                            peerWriters = this.brokerPeersSocketsMap.values()
                                    .stream()
                                    .map(Triple::getRight).collect(Collectors.toList());
                        }
                        if (!this.isRoot) {
                            peerWriters.add(this.brokerPeerOut);
                        }

                        for (PrintWriter peerWriter: peerWriters) {
                            peerWriter.println(String.format("%s %s", BrokerMessageType.REGISTER_SUBSCRIPTION.getMessageType(), serializedSubscription));
                        }

                        this.masterClientOut.println(String.format("%s %s", MasterMessageType.SUBSCRIPTION_IN, serializedSubscription.replaceAll("\\s+","")));
                    }
                }).start();
            }
        }).start();
    }

    private void getBrokerPeerHostPort() throws IOException {
        this.masterClientSocket = new Socket(this.masterHost, this.masterPort);
        this.masterClientIn = new BufferedReader(new InputStreamReader(this.masterClientSocket.getInputStream()));
        this.masterClientOut = new PrintWriter(this.masterClientSocket.getOutputStream(), true);

        this.masterClientOut.println(this.brokerPeersServerSocket.getLocalPort());
        this.masterClientOut.println(this.publishersServerSocket.getLocalPort());
        this.masterClientOut.println(this.subscribersServerSocket.getLocalPort());

        this.brokerPeerHost = this.masterClientIn.readLine();
        if (Objects.equals(this.brokerPeerHost, MasterMessageType.NO_PARENT_PEER.getMessageType())) {
            this.brokerPeerHost = null;
            this.brokerPeerPort = -1;
            this.isRoot = true;
            return;
        }
        this.brokerPeerPort = Integer.parseInt(this.masterClientIn.readLine());
    }

    private void connectToBrokerPeer() throws IOException {
        this.brokerPeerClientSocket = new Socket(this.brokerPeerHost, this.brokerPeerPort);
        this.brokerPeerIn = new BufferedReader(new InputStreamReader(this.brokerPeerClientSocket.getInputStream()));
        this.brokerPeerOut = new PrintWriter(this.brokerPeerClientSocket.getOutputStream(), true);

        this.brokerPeerOut.println(this.brokerPeersServerSocket.getLocalPort());

        this.routingTable.put(new ImmutablePair<>(this.brokerPeerHost, this.brokerPeerPort), new ArrayList<>());

        new Thread(() -> this.awaitBrokerPeerMessages(
                new ImmutablePair<>(this.brokerPeerHost, brokerPeerPort),
                new ImmutableTriple<>(this.brokerPeerClientSocket, this.brokerPeerIn, this.brokerPeerOut)
        )).start();
    }

    private void awaitBrokerPeerMessages(Pair<String, Integer> brokerPeerAddress, Triple<Socket, BufferedReader, PrintWriter> brokerPeerSocketData) {
        BufferedReader in = brokerPeerSocketData.getMiddle();

        //noinspection InfiniteLoopStatement
        while (true) {
            String message;
            try {
                message = in.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }

            String[] messageArgs = message.split(" ", 2);
            if (messageArgs.length < 2) {
                continue;
            }
            String messageType = messageArgs[0];
            String messageContent = messageArgs[1];

            if (Objects.equals(messageType, BrokerMessageType.REGISTER_SUBSCRIPTION.getMessageType())) {
                this.handleSubscription(brokerPeerAddress, messageType, messageContent);
            } else if (Objects.equals(messageType, BrokerMessageType.TRANSMIT_PUBLICATION.getMessageType())) {
                this.handlePublication(brokerPeerAddress, messageType, messageContent);
            }
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private void handleSubscription(Pair<String, Integer> brokerPeerAddress, String messageType, String messageContent) {
        Sub subscription;
        try {
            subscription = new ObjectMapper().readValue(messageContent, this.subClass);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        synchronized (this.routingTableLock) {
            this.routingTable.get(brokerPeerAddress).add(subscription);
        }

        // Route subscription to peers
        List<Pair<String, Integer>> eligiblePeerAddresses;
        synchronized (this.routingTableLock) {
            eligiblePeerAddresses = new ArrayList<>(this.routingTable.keySet());
        }
        List<PrintWriter> eligiblePeerWriters;
        synchronized (this.brokerPeersSocketsMapLock) {
            eligiblePeerWriters = eligiblePeerAddresses
                    .stream()
                    .filter(address -> {
                        if (brokerPeerAddress == null) {
                            return true;
                        }
                        return (!(Objects.equals(address.getLeft(), brokerPeerAddress.getLeft()) && Objects.equals(address.getRight(), brokerPeerAddress.getRight())));
                    })
                    .map(address -> {
                        if (Objects.equals(address.getLeft(), this.brokerPeerHost) && address.getRight() == this.brokerPeerPort) {
                            return this.brokerPeerOut;
                        } else {
                            return this.brokerPeersSocketsMap.get(address).getRight();
                        }
                    })
                    .collect(Collectors.toList());
        }
        for (PrintWriter eligiblePeerWriter: eligiblePeerWriters) {
            eligiblePeerWriter.println(String.format("%s %s", messageType, messageContent));
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private void handlePublication(Pair<String, Integer> brokerPeerAddress, String messageType, String messageContent) {
        Pub publication;
        try {
            publication = new ObjectMapper().readValue(messageContent, this.pubClass);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Send publication to eligible subscribers
        List<PrintWriter> eligibleSubscriberWriters;
        synchronized (this.subscribersMapLock) {
            eligibleSubscriberWriters = this.subscribersMap.values()
                    .stream()
                    .filter(subscriberData -> subscriberData.getRight()
                            .stream()
                            .anyMatch(subscription -> this.publicationMatchesSubscription(publication, subscription)))
                    .map(Triple::getMiddle)
                    .collect(Collectors.toList());
        }
        for (PrintWriter eligibleSubscriberWriter: eligibleSubscriberWriters) {
            eligibleSubscriberWriter.println(messageContent);

            this.masterClientOut.println(String.format("%s %s", MasterMessageType.PUBLICATION_OUT, messageContent.replaceAll("\\s+","")));
        }

        // Route publication to peers
        List<Pair<String, Integer>> eligiblePeerAddresses;
        synchronized (this.routingTableLock) {
            eligiblePeerAddresses = this.routingTable.keySet()
                    .stream()
                    .filter(peerAddress -> this.routingTable.get(peerAddress)
                            .stream()
                            .anyMatch(subscription -> this.publicationMatchesSubscription(publication, subscription)))
                    .collect(Collectors.toList());
        }
        List<PrintWriter> eligiblePeerWriters;
        synchronized (this.brokerPeersSocketsMapLock) {
            eligiblePeerWriters = eligiblePeerAddresses
                    .stream()
                    .filter(address -> {
                        if (brokerPeerAddress == null) {
                            return true;
                        }
                        return (!(Objects.equals(address.getLeft(), brokerPeerAddress.getLeft()) && Objects.equals(address.getRight(), brokerPeerAddress.getRight())));
                    })
                    .map(address -> {
                        if (Objects.equals(address.getLeft(), this.brokerPeerHost) && address.getRight() == this.brokerPeerPort) {
                            return this.brokerPeerOut;
                        } else {
                            return this.brokerPeersSocketsMap.get(address).getRight();
                        }
                    })
                    .collect(Collectors.toList());
        }
        for (PrintWriter eligiblePeerWriter: eligiblePeerWriters) {
            eligiblePeerWriter.println(String.format("%s %s", messageType, messageContent));
        }
    }
}
