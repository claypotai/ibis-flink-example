from dataclasses import dataclass
import ibis.expr.datatypes as dt

@dataclass
class CreditCardTransaction:
    trans_num: dt.str # primary key
    user_id: dt.int64 # user_id
    cc_num: dt.int64 # creadit card number
    amt: dt.float64 # credit card transaction amount
    merchant: dt.str
    category: dt.str     
    is_fraud: dt.int32 # Fraud Label
    first: dt.str # first name
    last: dt.str # last name
    dob: dt.str # date of birth
    zipcode: dt.str
    trans_date_trans_time: dt.timestamp(scale=3)