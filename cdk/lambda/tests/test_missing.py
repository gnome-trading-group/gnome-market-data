from missing import NOT_COLLECTED, missing_stretches


def covered(from_ns, to_ns):
    return {'fromNs': from_ns, 'toNs': to_ns}


def gap(from_ns, to_ns, reason):
    return {'fromNs': from_ns, 'toNs': to_ns, 'reason': reason}


def test_a_fully_covered_range_has_nothing_missing():
    assert missing_stretches([covered(0, 50), covered(50, 100)], [], None, 10, 90, 0) == ([], 0)


def test_holes_take_the_gap_reason_and_time_nothing_wanted_is_not_collected():
    stretches, untracked = missing_stretches(
        [covered(20, 40), covered(60, 80)], [gap(40, 60, 'TASK_LOST')], None, 0, 100, 0)
    assert stretches == [
        {'fromNs': 0, 'toNs': 20, 'reason': NOT_COLLECTED},
        {'fromNs': 40, 'toNs': 60, 'reason': 'TASK_LOST'},
        {'fromNs': 80, 'toNs': 100, 'reason': NOT_COLLECTED},
    ]
    assert untracked == 0


def test_a_hole_only_partly_explained_by_a_gap_splits():
    stretches, _ = missing_stretches([covered(0, 10), covered(90, 100)], [gap(30, 50, 'STALE')], None, 0, 100, 0)
    assert stretches == [
        {'fromNs': 10, 'toNs': 30, 'reason': NOT_COLLECTED},
        {'fromNs': 30, 'toNs': 50, 'reason': 'STALE'},
        {'fromNs': 50, 'toNs': 90, 'reason': NOT_COLLECTED},
    ]


def test_the_open_gap_runs_to_the_end_of_the_range():
    stretches, _ = missing_stretches([covered(0, 60)], [], {'fromNs': 60, 'reason': 'NOT_PLACED'}, 0, 100, 0)
    assert stretches == [{'fromNs': 60, 'toNs': 100, 'reason': 'NOT_PLACED'}]


def test_time_before_tracking_is_measured_not_missing():
    stretches, untracked = missing_stretches([covered(50, 70)], [], None, 0, 100, 40)
    assert untracked == 40
    assert stretches == [
        {'fromNs': 40, 'toNs': 50, 'reason': NOT_COLLECTED},
        {'fromNs': 70, 'toNs': 100, 'reason': NOT_COLLECTED},
    ]
    assert missing_stretches([], [], None, 0, 100, 200) == ([], 100)


def test_intervals_split_at_midnight_and_overlapping_rows_leave_no_hole():
    assert missing_stretches([covered(0, 50), covered(50, 100), covered(40, 60)], [], None, 0, 100, 0) == ([], 0)


def test_adjacent_stretches_with_one_reason_merge_and_a_missing_reason_is_unknown():
    stretches, _ = missing_stretches([], [gap(0, 30, None), gap(30, 50, None)], None, 0, 50, 0)
    assert stretches == [{'fromNs': 0, 'toNs': 50, 'reason': 'UNKNOWN'}]
