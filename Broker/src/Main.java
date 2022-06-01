import java.io.IOException;

public class Main {

    private static final String MASTER_HOST = "127.0.0.1";
    private static final int MASTER_PORT = 8090;

    public static void main(String[] args) throws IOException {
        SpecializedBroker specializedBroker = new SpecializedBroker(MASTER_HOST, MASTER_PORT);
        specializedBroker.start();
    }
}
