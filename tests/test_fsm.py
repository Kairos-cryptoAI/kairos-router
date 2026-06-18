"""The hysteresis behaviour is the whole point of Layer 2 — lock it down."""
from kairos_core.enums import RouterMode, Side
from kairos_router.fsm import RouterFSM


def test_escalates_to_high_after_threshold_conflicts():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "BTCUSD"
    for _ in range(3):
        st = fsm.update(sym, Side.SHORT, Side.LONG)
        assert st.mode is RouterMode.USE_MEDIUM  # not yet
    st = fsm.update(sym, Side.SHORT, Side.LONG)  # 4th consecutive conflict
    assert st.mode is RouterMode.USE_HIGH


def test_a_single_calm_tick_resets_conflict_streak():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "ETHUSD"
    for _ in range(3):
        fsm.update(sym, Side.SHORT, Side.LONG)
    fsm.update(sym, Side.LONG, Side.LONG)  # calm -> streak resets
    assert fsm.state(sym).conflict_streak == 0
    for _ in range(3):
        st = fsm.update(sym, Side.SHORT, Side.LONG)
    assert st.mode is RouterMode.USE_MEDIUM  # only 3 since reset


def test_requires_full_calm_window_to_fall_back():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "BTCUSD"
    for _ in range(4):
        fsm.update(sym, Side.SHORT, Side.LONG)
    assert fsm.state(sym).mode is RouterMode.USE_HIGH
    for _ in range(9):
        st = fsm.update(sym, Side.LONG, Side.LONG)
        assert st.mode is RouterMode.USE_HIGH  # still high after 9 calm ticks
    st = fsm.update(sym, Side.LONG, Side.LONG)  # 10th calm tick
    assert st.mode is RouterMode.USE_MEDIUM


def test_brief_calm_does_not_drop_high_then_resumes():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "BTCUSD"
    for _ in range(4):
        fsm.update(sym, Side.SHORT, Side.LONG)
    for _ in range(5):
        fsm.update(sym, Side.LONG, Side.LONG)  # 5 calm, not enough to fall back
    st = fsm.update(sym, Side.SHORT, Side.LONG)  # conflict resumes
    assert st.mode is RouterMode.USE_HIGH
    assert st.calm_streak == 0
