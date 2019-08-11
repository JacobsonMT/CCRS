package com.jacobsonmt.ccrs.model;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@RequiredArgsConstructor
@EqualsAndHashCode(of = {"reference"})
@ToString
public class Base {
    private final String reference;
    private final int depth;
    private final double conservation;
    private List<Double> list = new ArrayList<>();
}
