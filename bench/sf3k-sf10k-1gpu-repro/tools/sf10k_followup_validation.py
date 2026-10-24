"""Validate a Q18 LIMIT-boundary difference against an expanded GPU result."""
from collections import Counter
from result_validation import load, validate, ORDER_KEYS


def validate_q18_boundary(left, right, expanded):
    original = validate(left, right, 18)
    lh, lt, l = load(left)
    rh, rt, r = load(right)
    eh, et, e = load(expanded)
    if not (lh == rh == eh and lt == rt == et and len(l) == len(r) == 100 and len(e) > 100):
        return dict(agree=False, detail='Expanded Q18 schema/row counts invalid')
    order = lambda row: tuple(row[i] for i in ORDER_KEYS[18])
    # The expanded result must include the entire boundary tie and extend beyond it.
    boundary = order(e[99])
    if order(e[-1]) == boundary:
        return dict(agree=False, detail='Expanded result does not extend beyond cutoff tie')
    # Check actual SQL ordering, including direction, for the expanded result.
    if any(a[4] < b[4] or (a[4] == b[4] and a[3] > b[3]) for a, b in zip(e, e[1:])):
        return dict(agree=False, detail='Expanded Q18 violates ORDER BY price DESC, date ASC')
    for rows in (l, r):
        if any(order(a) != order(b) for a, b in zip(rows, e[:100])):
            return dict(agree=False, detail='ORDER BY keys differ from expanded prefix')
    expected = Counter(tuple(row) for row in e)
    for rows in (l, r):
        if any(count > expected[row] for row, count in Counter(tuple(x) for x in rows).items()):
            return dict(agree=False, detail='Original contains values absent from expanded result')
    prefix = lambda rows: Counter(tuple(row) for row in rows if order(row) != boundary)
    if not (prefix(l) == prefix(r) == prefix(e[:100])):
        return dict(agree=False, detail='Non-boundary rows differ')
    return dict(agree=True, ordered_match=original.get('ordered_match', False),
                legal_sql_limit_tie=True, rows=100, expanded_reference_rows=len(e),
                boundary_key=[str(x) for x in boundary],
                detail='ORDER BY keys, all non-boundary rows and every selected cutoff-row value verified against expanded GPU result; no independent CPU oracle.')
