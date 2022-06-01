import java.io.IOException;
import java.security.InvalidParameterException;

public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new InvalidParameterException("You must provide the master server host and port");
        }

        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);

        SpecializedBroker specializedBroker = new SpecializedBroker(masterHost, masterPort);
        specializedBroker.start();
    }
}
