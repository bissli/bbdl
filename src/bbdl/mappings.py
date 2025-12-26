"""Custom field mappings for Bloomberg Data License parsing.

This module contains field-specific configuration that customizes how
certain Bloomberg fields are parsed and formatted.
"""

BULK_FIELD_KEYS = {
    'CALL_SCHEDULE': ['Call Date', 'Call Price'],
    'PUT_SCHEDULE': ['Put Date', 'Put Price'],
    'SOFT_CALL_SCHEDULE': ['Soft Call Date', 'Soft Call Price'],
    'SOFT_CALL_SCHEDULE_EXTENDED': ['Soft Call Date', 'Soft Call Price'],
    'DDIS_AMT_OUTSTANDING_BY_YR_BNDLN': ['Year', 'Amount Outstanding - Ultimate Parent'],
    'ISSUE_UNDERWRITER': ['Role', 'Firm', 'Abbreviation', 'Code', 'Description', 'Amount', 'Order', 'Date'],
    'CONVERSION_RESET_SCHEDULE': ['Reset Date', 'Conversion Price', 'Floor'],
    'REDEMPTION_UNDERLYING': ['Ticker', 'Type'],
    'REDEMPTION_UNDERLYING_DATA': ['Ticker', 'Weight', 'Initial Value', 'Strike', 'Upper Barrier', 'Lower Barrier', 'Num Shares'],
    }
