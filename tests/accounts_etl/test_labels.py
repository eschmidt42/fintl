import polars as pl

import fintl.accounts_etl.labels as fintl_labels
from fintl.accounts_etl.schemas import LabelCondition, LabelConditionOp, LabelRule

# Order matters: first matching rule wins.
LABEL_RULES: list[LabelRule] = [
    LabelRule(
        label="shopping",
        conditions=[
            LabelCondition(
                column="recipient",
                op=LabelConditionOp.contains,
                value="(?i)edeka|dm drogerie|dm-drogerie|lidl|rewe|netto|asia24",
            )
        ],
    ),
    LabelRule(
        label="income",
        conditions=[
            LabelCondition(column="source", op=LabelConditionOp.contains, value="DWS")
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="source", op=LabelConditionOp.equals, value="JANE DOE"
            ),
            LabelCondition(
                column="recipient", op=LabelConditionOp.equals, value="myself"
            ),
            LabelCondition(
                column="description", op=LabelConditionOp.not_contains, value="food"
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="source", op=LabelConditionOp.equals, value="JANE DOE"
            ),
            LabelCondition(
                column="recipient", op=LabelConditionOp.equals, value="JANE DOE"
            ),
            LabelCondition(
                column="description", op=LabelConditionOp.not_contains, value="food"
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="source", op=LabelConditionOp.equals, value="JANE DOE"
            ),
            LabelCondition(
                column="recipient",
                op=LabelConditionOp.equals,
                value="HERR JANE DOE",
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(column="source", op=LabelConditionOp.equals, value="myself"),
            LabelCondition(
                column="recipient",
                op=LabelConditionOp.equals,
                value="HERR JANE DOE",
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="source", op=LabelConditionOp.equals, value="Jane Doe"
            ),
            LabelCondition(
                column="recipient", op=LabelConditionOp.equals, value="Jane DKB"
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(column="source", op=LabelConditionOp.equals, value="myself"),
            LabelCondition(
                column="recipient", op=LabelConditionOp.equals, value="Jane Doe"
            ),
            LabelCondition(
                column="description",
                op=LabelConditionOp.not_contains,
                value="Scalable Capital Broker",
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="source", op=LabelConditionOp.equals, value="Jane Doe"
            ),
            LabelCondition(
                column="recipient", op=LabelConditionOp.equals, value="Jane Doe"
            ),
            LabelCondition(
                column="description", op=LabelConditionOp.contains, value="tagesgeld"
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="source", op=LabelConditionOp.equals, value="JANE DOE"
            ),
            LabelCondition(
                column="recipient", op=LabelConditionOp.equals, value="Jane Doe"
            ),
            LabelCondition(
                column="description", op=LabelConditionOp.contains, value="Food"
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="source", op=LabelConditionOp.equals, value="Jane Doe"
            ),
            LabelCondition(
                column="recipient", op=LabelConditionOp.equals, value="myself"
            ),
            LabelCondition(
                column="description", op=LabelConditionOp.contains, value="food"
            ),
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="description",
                op=LabelConditionOp.contains,
                value="Ausgleich Kreditkarte gem\\. Abrechnung v",
            )
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="description",
                op=LabelConditionOp.contains,
                value="from tagesgeld for festgeld",
            )
        ],
    ),
    LabelRule(
        label="income",
        conditions=[
            LabelCondition(
                column="description", op=LabelConditionOp.contains, value="Lohn/Gehalt"
            )
        ],
    ),
    LabelRule(
        label="income",
        conditions=[
            LabelCondition(
                column="description",
                op=LabelConditionOp.contains,
                value="(?i)Verguetung",
            )
        ],
    ),
    LabelRule(
        label="income",
        conditions=[
            LabelCondition(
                column="source",
                op=LabelConditionOp.contains,
                value="(?i)finanzamt|finanzaemter",
            )
        ],
    ),
    LabelRule(
        label="savings",
        conditions=[
            LabelCondition(
                column="recipient", op=LabelConditionOp.contains, value="DWS Investment"
            )
        ],
    ),
    LabelRule(
        label="savings",
        conditions=[
            LabelCondition(
                column="description",
                op=LabelConditionOp.contains,
                value="Scalable Capital Broker",
            )
        ],
    ),
    LabelRule(
        label="rent and living",
        conditions=[
            LabelCondition(
                column="recipient",
                op=LabelConditionOp.contains,
                value="(?i)CILIX|naturstrom|congstar|rundfunk",
            )
        ],
    ),
    LabelRule(
        label="rebooking",
        conditions=[
            LabelCondition(
                column="recipient", op=LabelConditionOp.contains, value="JANE DOE"
            ),
            LabelCondition(
                column="description", op=LabelConditionOp.contains, value="(?i)food"
            ),
            LabelCondition(
                column="provider", op=LabelConditionOp.contains, value="DKB"
            ),
        ],
    ),
    LabelRule(
        label="shopping",
        conditions=[
            LabelCondition(
                column="recipient",
                op=LabelConditionOp.contains,
                value="(?i)dm drogerie|dm-drogerie|lidl|rewe|netto|asia24",
            )
        ],
    ),
    LabelRule(
        label="entertainment",
        conditions=[
            LabelCondition(
                column="recipient",
                op=LabelConditionOp.contains,
                value="(?i)spotify|prime video|microsoft|netflix|disney",
            )
        ],
    ),
    LabelRule(
        label="health and wellness",
        conditions=[
            LabelCondition(
                column="recipient",
                op=LabelConditionOp.contains,
                value="(?i)some sports",
            )
        ],
    ),
]


def test_assign_labels():
    # Create a sample DataFrame for testing
    data = [
        {
            "source": "JANE DOE",
            "recipient": "EDEKA",
            "description": "Grocery shopping",
            "expected_label_root": "shopping",
            "provider": "wup",
        },
        {
            "source": "DWS",
            "recipient": "DWS",
            "description": "Salary",
            "expected_label_root": "income",
            "provider": "wup",
        },
        {
            "source": "JANE DOE",
            "recipient": "myself",
            "description": "bla",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "JANE DOE",
            "recipient": "myself",
            "description": "food",
            "expected_label_root": "unknown",
            "provider": "wup",
        },
        {
            "source": "JANE DOE",
            "recipient": "JANE DOE",
            "description": "bla",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "JANE DOE",
            "recipient": "JANE DOE",
            "description": "food",
            "expected_label_root": "unknown",
            "provider": "wup",
        },
        {
            "source": "JANE DOE",
            "recipient": "HERR JANE DOE",
            "description": "bla",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "myself",
            "recipient": "HERR JANE DOE",
            "description": "bla",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "Jane Doe",
            "recipient": "Jane DKB",
            "description": "bla",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "myself",
            "recipient": "Jane Doe",
            "description": "bla",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "employer",
            "recipient": "myself",
            "description": "Lohn/Gehalt",
            "expected_label_root": "income",
            "provider": "wup",
        },
        {
            "source": "employer",
            "recipient": "myself",
            "description": "Verguetung",
            "expected_label_root": "income",
            "provider": "wup",
        },
        {
            "source": "myself",
            "recipient": "myself",
            "description": "bla",
            "expected_label_root": "unknown",
            "provider": "wup",
        },
        {
            "source": "Jane Doe",
            "recipient": "Jane Doe",
            "description": "tagesgeld transfer",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "JANE DOE",
            "recipient": "Jane Doe",
            "description": "Food budget",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "Jane Doe",
            "recipient": "myself",
            "description": "food money",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "someone",
            "recipient": "someone",
            "description": "Ausgleich Kreditkarte gem. Abrechnung v Jan",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "someone",
            "recipient": "someone",
            "description": "from tagesgeld for festgeld",
            "expected_label_root": "rebooking",
            "provider": "wup",
        },
        {
            "source": "Finanzamt Berlin",
            "recipient": "myself",
            "description": "tax refund",
            "expected_label_root": "income",
            "provider": "wup",
        },
        {
            "source": "someone",
            "recipient": "DWS Investment",
            "description": "fund purchase",
            "expected_label_root": "savings",
            "provider": "wup",
        },
        {
            "source": "someone",
            "recipient": "someone",
            "description": "Scalable Capital Broker transfer",
            "expected_label_root": "savings",
            "provider": "wup",
        },
        {
            "source": "myself",
            "recipient": "Jane Doe",
            "description": "Scalable Capital Broker",
            "expected_label_root": "savings",
            "provider": "wup",
        },
        {
            "source": "myself",
            "recipient": "congstar",
            "description": "monthly bill",
            "expected_label_root": "rent and living",
            "provider": "wup",
        },
        {
            "source": "myself",
            "recipient": "JANE DOE",
            "description": "food reimbursement",
            "expected_label_root": "rebooking",
            "provider": "DKB",
        },
        {
            "source": "myself",
            "recipient": "netflix",
            "description": "streaming subscription",
            "expected_label_root": "entertainment",
            "provider": "wup",
        },
        {
            "source": "myself",
            "recipient": "Some Sports Club",
            "description": "membership",
            "expected_label_root": "health and wellness",
            "provider": "wup",
        },
    ]

    df = pl.from_dicts(data)

    df_labelled = fintl_labels.assign_labels(df, LABEL_RULES)

    assert (
        df_labelled["label_root"].to_list()
        == df_labelled["expected_label_root"].to_list()
    )
