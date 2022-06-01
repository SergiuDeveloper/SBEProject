package model;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@ToString
public class Subscription {

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @Getter
    @Setter
    @ToString
    public static class Condition {
        private String field;
        private String operator;
        private String value;
    }

    private List<Condition> conditions;
}
