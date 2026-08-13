from kairos_core.enums import Side

from kairos_router.conflict import SignalRelation, is_conflict, sentiment_to_side, signal_relation


def test_sentiment_deadband():
    assert sentiment_to_side(0.1) is Side.FLAT
    assert sentiment_to_side(0.5) is Side.LONG
    assert sentiment_to_side(-0.5) is Side.SHORT


def test_conflict_requires_opposite_directional():
    assert is_conflict(Side.SHORT, Side.LONG) is True
    assert is_conflict(Side.LONG, Side.LONG) is False
    assert is_conflict(Side.LONG, Side.FLAT) is False


def test_signal_relation_distinguishes_agreement_from_abstention():
    assert signal_relation(Side.LONG, Side.SHORT) is SignalRelation.CONFLICT
    assert signal_relation(Side.LONG, Side.LONG) is SignalRelation.AGREEMENT
    assert signal_relation(Side.LONG, Side.FLAT) is SignalRelation.ABSTAIN
