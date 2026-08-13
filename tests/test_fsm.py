"""The hysteresis behaviour is the whole point of Layer 2 — lock it down."""

from kairos_core.enums import RouterMode, Side

from kairos_router.fsm import RouterFSM


def test_escalates_to_high_after_threshold_conflicts():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "BTCUSD"
    for _ in range(3):
        st = fsm.update(sym, Side.SHORT, Side.LONG)
        assert st.mode is RouterMode.ROUTE_PRO  # not yet
    st = fsm.update(sym, Side.SHORT, Side.LONG)  # 4th consecutive conflict
    assert st.mode is RouterMode.ROUTE_GPT


def test_a_single_calm_tick_resets_conflict_streak():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "ETHUSD"
    for _ in range(3):
        fsm.update(sym, Side.SHORT, Side.LONG)
    fsm.update(sym, Side.LONG, Side.LONG)  # calm -> streak resets
    assert fsm.state(sym).conflict_streak == 0
    for _ in range(3):
        st = fsm.update(sym, Side.SHORT, Side.LONG)
    assert st.mode is RouterMode.ROUTE_PRO  # only 3 since reset


def test_requires_full_calm_window_to_fall_back():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "BTCUSD"
    for _ in range(4):
        fsm.update(sym, Side.SHORT, Side.LONG)
    assert fsm.state(sym).mode is RouterMode.ROUTE_GPT
    for _ in range(9):
        st = fsm.update(sym, Side.LONG, Side.LONG)
        assert st.mode is RouterMode.ROUTE_GPT  # still high after 9 calm ticks
    st = fsm.update(sym, Side.LONG, Side.LONG)  # 10th calm tick
    assert st.mode is RouterMode.ROUTE_PRO


def test_brief_calm_does_not_drop_high_then_resumes():
    fsm = RouterFSM(conflict_threshold=4, calm_threshold=10)
    sym = "BTCUSD"
    for _ in range(4):
        fsm.update(sym, Side.SHORT, Side.LONG)
    for _ in range(5):
        fsm.update(sym, Side.LONG, Side.LONG)  # 5 calm, not enough to fall back
    st = fsm.update(sym, Side.SHORT, Side.LONG)  # conflict resumes
    assert st.mode is RouterMode.ROUTE_GPT
    assert st.calm_streak == 0


def test_preview_is_transactional_until_committed():
    fsm = RouterFSM(conflict_threshold=1)

    preview = fsm.preview("BTCUSDT", Side.SHORT, Side.LONG)

    assert preview.mode is RouterMode.ROUTE_GPT
    assert "BTCUSDT" not in fsm._states
    fsm.commit("BTCUSDT", preview)
    assert fsm.state("BTCUSDT").mode is RouterMode.ROUTE_GPT


def test_abstention_resets_consecutive_streaks_without_deescalating():
    fsm = RouterFSM(conflict_threshold=2, calm_threshold=2)
    fsm.update("BTCUSDT", Side.SHORT, Side.LONG)
    state = fsm.update("BTCUSDT", Side.SHORT, Side.FLAT)

    assert state.mode is RouterMode.ROUTE_PRO
    assert state.conflict_streak == 0
    assert state.calm_streak == 0

    fsm.update("BTCUSDT", Side.SHORT, Side.LONG)
    fsm.update("BTCUSDT", Side.SHORT, Side.LONG)
    assert fsm.state("BTCUSDT").mode is RouterMode.ROUTE_GPT

    state = fsm.update("BTCUSDT", Side.FLAT, Side.LONG)

    assert state.mode is RouterMode.ROUTE_GPT
    assert state.conflict_streak == 0
    assert state.calm_streak == 0
