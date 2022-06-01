import model.Publication;
import model.Subscription;

public class SpecializedBroker extends Broker<Publication, Subscription> {

    public SpecializedBroker(String masterHost, int masterPort) {
        super(masterHost, masterPort, Publication.class, Subscription.class);
    }

    @Override
    protected boolean publicationMatchesSubscription(Publication publication, Subscription subscription) {
        return true;
    }
}
