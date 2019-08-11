package com.jacobsonmt.ccrs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Taxa {
    public static enum KnownKeyTypes {
        OX,
        missing_OX,
        malformed_OX,
        invalid_OX,
        virus_OX,
    }

    private final String key;
    private final int id;

    public Taxa( int id ) {
        this.key = KnownKeyTypes.OX.name();
        this.id = id;
    }
}
