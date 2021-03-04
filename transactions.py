"""Utilities to model money transactions."""

import pandas

from random import choices, randint
from string import ascii_letters, digits

account_chars: str = digits + ascii_letters


# Load the financial transactions file and remove apostrophes
trans_df = pandas.read_csv("FinTx.csv")
trans_df["customer"] = trans_df["customer"].str.replace('[^\w\s]','')
trans_df["age"] = trans_df["age"].str.replace('[^\w\s]','')
trans_df["gender"] = trans_df["gender"].str.replace('[^\w\s]','')
trans_df["zipcodeOri"] = trans_df["zipcodeOri"].str.replace('[^\w\s]','')
trans_df["merchant"] = trans_df["merchant"].str.replace('[^\w\s]','')
trans_df["zipMerchant"] = trans_df["zipMerchant"].str.replace('[^\w\s]','')
trans_df["category"] = trans_df["category"].str.replace('[^\w\s]','')



def _random_account_id() -> str:
    """Return a random account number made of 12 characters."""
    return ''.join(choices(account_chars, k=12))


def _random_amount() -> float:
    """Return a random amount between 1.00 and 1000.00."""
    return randint(100, 100000) / 100


def create_random_transaction() -> dict:
    """Create a fake, randomised transaction."""
    rand_cust = trans_df.sample()

    return {
        #'source'.to_string(): rand_cust["customer"],
        'source': _random_account_id(),
        'target': _random_account_id(),
        'amount': _random_amount(),
        #'amount': rand_cust["amount"],
        # Keep it simple: it's all euros
        'currency': 'EUR',
    }
