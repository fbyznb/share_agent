package sku.schema;

import java.util.Map;

public final class CounterSchema {
    public static final int IDX_TURN = 1;
    public static final int IDX_FAV = 2;

    public static final Map<String, Integer> NAME_TO_IDX = Map.of(
            "turn", IDX_TURN,
            "fav", IDX_FAV
    );
}
