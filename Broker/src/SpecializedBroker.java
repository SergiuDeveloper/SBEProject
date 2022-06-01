import model.Publication;
import model.Subscription;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SpecializedBroker extends Broker<Publication, Subscription> {

    public SpecializedBroker(String masterHost, int masterPort) {
        super(masterHost, masterPort, Publication.class, Subscription.class);
    }

    @Override
    protected boolean publicationMatchesSubscription(Publication publication, Subscription subscription) {
        /*
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");

        Map<String, String> publicationValues = new HashMap<String, String>() {{
            put("company", publication.getCompany());
            put("date", simpleDateFormat.format(publication.getDate()));
            put("value", String.valueOf(publication.getValue()));
            put("drop", String.valueOf(publication.getDrop()));
            put("variation", String.valueOf(publication.getVariation()));
        }};

        for (Subscription.Condition condition: subscription.getConditions()) {
            String conditionValue = condition.getValue();
            String publicationValue = publicationValues.get(condition.getField());

            switch (condition.getOperator()) {
                case "=": if (!Objects.equals(publicationValue, conditionValue)) {
                    return false;
                }
                case "!=": if (Objects.equals(publicationValue, conditionValue)) {
                    return false;
                }
                case "<": if (publicationValue.compareTo(conditionValue) >= 0) {
                    return false;
                }
                case ">": if (publicationValue.compareTo(conditionValue) <= 0) {
                    return false;
                }
                case "<=": if (publicationValue.compareTo(conditionValue) > 0) {
                    return false;
                }
                case ">=": if (publicationValue.compareTo(conditionValue) < 0) {
                    return false;
                }
            }
        }*/
        return true;
    }
}
