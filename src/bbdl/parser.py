import csv
import logging
import re

from bbdl.assets import get_fields
from date import Date, DateTime, Time
from libb import OrderedSet, attrdict, cachedstaticproperty, parse_number

logger = logging.getLogger(__name__)

__all__ = ['Field', 'Ticker']


def to_date(x, fmt=None) -> Date:
    return Date.parse(x, fmt=fmt, raise_err=True)


def to_datetime(x, fmt=None) -> DateTime:
    return DateTime.parse(x, fmt=fmt, raise_err=True)


def to_time(x, fmt=None) -> Time:
    return Time.parse(x, fmt=fmt, raise_err=True)


class Field:

    @staticmethod
    def to_type(field):
        """Field type lookup (for converting to container object)"""
        try:
            ftype = Field.all_fields[field.upper()]['Field Type']
        except KeyError:
            return object
        if ftype == 'Boolean':        return bool
        if ftype == 'Bulk Format':    return list
        if ftype == 'Character':      return str
        if ftype == 'Date':           return Date
        if ftype == 'Date or Time':   return DateTime
        if ftype == 'Integer':        return int
        if ftype == 'Integer/Real':   return float
        if ftype == 'Long Character': return str
        if ftype == 'Month/Year':     return Date
        if ftype == 'Price':          return float
        if ftype == 'Real':           return float
        if ftype == 'Time':           return Time

        raise ValueError(f'Unknown type: {ftype}, for mnemonic: {field}')

    @staticmethod
    def to_python(field, value):
        """Convert field (including compound type) to python value

        >>> Field.to_python('PX_LAST', '123')
        123
        >>> Field.to_python('MATURITY', '12/1/24')
        Date(2024, 12, 1)

        One dimension List
        >>> l = ';1;2;5;11/01/2007;5;11/01/2008;'
        >>> Field.to_python('DDIS_AMT_OUTSTANDING_BY_YR_BNDLN', l)
        [Date(2007, 11, 1), Date(2008, 11, 1)]

        Two dimension List
        >>> l = ';2;2;2;5;11/01/2007;3;100.5;5;11/01/2008;3;101;'
        >>> Field.to_python('DDIS_AMT_OUTSTANDING_BY_YR_BNDLN', l)
        [(Date(2007, 11, 1), 100.5), (Date(2008, 11, 1), 101)]
        """
        if field in Field._exception_convertrs:
            return Field._exception_convertrs[field](value)
        try:
            ftype = Field.all_fields[field.upper()]['Field Type']
        except KeyError:
            raise ValueError(f'Unknown field: {field}')

        if ftype == 'Boolean':        return Field._to_bool(value)
        if ftype == 'Bulk Format':    return Field._to_list(value)
        if ftype == 'Character':      return Field._to_str(value)
        if ftype == 'Date':           return to_date(value)
        if ftype == 'Date or Time':   return to_datetime(value)
        if ftype == 'Integer':        return Field._to_number(value)
        if ftype == 'Integer/Real':   return Field._to_number(value)
        if ftype == 'Long Character': return Field._to_str(value)
        if ftype == 'Month/Year':     return to_date(value, fmt='%m/%y')
        if ftype == 'Price':          return Field._to_number(value)
        if ftype == 'Real':           return Field._to_number(value)
        if ftype == 'Time':           return to_time(value)

        raise ValueError(f'Unknown type: {ftype}, for mnemonic: {field}')

    @cachedstaticproperty
    def all_fields():
        fields = {}
        with get_fields() as f:
            reader = csv.reader(f)
            header = next(reader)
            for line in reader:
                row = dict(zip(header, [c.strip() for c in line]))
                fields[row['Field Mnemonic']] = row
        return fields

    @staticmethod
    def from_categories(categories: list, invert: bool = False) -> OrderedSet:
        """Return all fields in a category ex metadata fields

        >>> from pprint import pprint
        >>> cat = list(Field.from_categories(['Fundamentals']))
        >>> pprint(cat)
        ['3YR_AVG_NET_MARGIN',
         ...
        'ZAKAT_EXPENSES_OTHER_RESRV_CHARG']

        >>> Field.to_categories(cat).count
        {'Fundamentals': 10428}

        >>> len(Field.from_categories(['Fundamentals'], invert=False))
        10428
        >>> len(Field.from_categories(['Fundamentals'], invert=True))
        33322
        """
        filterfn = lambda a, b: a in b if not invert else a not in b
        return OrderedSet(_['Field Mnemonic']
                          for _ in Field.all_fields.values()
                          if filterfn(_['Data License Category'], categories)
                          and _['Field Mnemonic'][:3] not in {'BH_', 'LU_'})

    @staticmethod
    def to_categories(fields: list):
        """Map fields to categories. Useful for initial classification.

        >>> from pprint import pprint
        >>> pprint(Field.to_categories(Field.all_fields).count)
        {'ATM Volatility': 2,
        'ATMOTM Swaption': 3,
        'BCurve': 1,
        'Basic Tax': 8,
        'BenchmarkRegulation': 59,
        'Bram Fair Value Hierarchy Leveling Tool': 4,
        'CAPFloor Volatility': 3,
        'Central Bank Eligibility': 4,
        'Climate Risk Assessment': 2,
        'Collateral Tagging': 37,
        'Corporate Actions': 852,
        'Covered Funds': 6,
        'Credit Benchmark & Consensus Rating': 31,
        'Credit Risk Capital Structure': 129,
        'Credit Risk Corporate Structure': 222,
        'Credit Risk Regulatory Compliance': 43,
        'Default Risk': 106,
        'Derived - End of Day': 1101,
        'Derived - Intraday': 1590,
        'ESG Book': 341,
        'ESG Climate': 316,
        'ESG Regulation': 104,
        'ESG Reported': 2763,
        'ESGScores': 897,
        'Estimates': 1742,
        'Expected Credit Loss': 15,
        'FRTB SA Bucketing': 162,
        'FRTBRFETData': 28,
        'Fund Analytics': 24,
        'Fund ESG Analytics': 93,
        'Fundamentals': 10428,
        'FundamentalsIndustrySpecific': 8675,
        'FundamentalsSegmentation': 208,
        'High Quality Liquid Assets': 26,
        'Historical Time Series': 93,
        'Holiday Pricing': 5,
        'IFRS 9 SPPI': 3,
        'Implied Dividend': 4,
        'Investor Protection': 108,
        'Kestrel': 17,
        'Liquidity Assessment': 78,
        'MSCI ESG': 992,
        'MiFIR': 131,
        'Not Downloadable': 2198,
        'Open Source': 1121,
        'Packaged': 145,
        'Portfolio Holdings': 2,
        'Premium BRAM Transparency': 77,
        'Pricing - End of Day': 1581,
        'Pricing - Intraday': 862,
        'Primary Market': 88,
        'Reg SSFA': 20,
        'Regulatory & Risk Group 2 Misc': 55,
        'Regulatory & Risk Group 3 Misc': 28,
        'Regulatory & Risk Group 4 Misc': 61,
        'Sanctions': 140,
        'Securities Financing Transactions Regulation': 9,
        'Security Master': 6431,
        'SecurityOwnership': 5,
        'SupplyChain': 21,
        'Sustainalytics ESG': 286,
        'UK MIFI': 63,
        'US Withholding Tax': 38,
        'User Entered Info.': 133,
        'Volatility Surface': 3}

        >>> Field.to_categories(['ID_BB_UNIQUE', 'PX_ASK', 'PX_BID']).detail
        {'Open Source': ['ID_BB_UNIQUE'], 'Pricing - Intraday': ['PX_ASK', 'PX_BID']}
        """
        count, count_detail = attrdict(), attrdict()
        for field in fields:
            row = Field.all_fields.get(field.upper()) or {}
            category = row.get('Data License Category')
            if not category:
                continue
            count[category] = count.get(category, 0)+1
            count_detail[category] = count_detail.get(category, [])+[field]
        res = attrdict(count=count, detail=count_detail)
        return res

    @cachedstaticproperty
    def open_fields() -> OrderedSet:
        """Open fields do not incur a charge.

        >>> gratis = Field.open_fields()
        >>> type(gratis)
        <class ...OrderedSet'>
        >>> len(gratis)
        1240
        >>> 'PARSEKYABLE_DES' in gratis
        True
        """
        all_categories = Field.to_categories(Field.all_fields).detail
        return OrderedSet(
            all_categories['Open Source'] +
            all_categories['User Entered Info.'])

    @staticmethod
    def _to_number(value):
        try:
            return parse_number(str(value))
        except:
            try:
                return float(value)
            except:
                pass

    @staticmethod
    def _to_str(value):
        if not value:
            return
        value = value.strip()
        if value in {'', 'N.A.', 'N.D.', 'N.S.'}:
            return
        return value

    @staticmethod
    def _to_bool(value):
        value = Field._to_str(value)
        return bool(value and value.upper()[0] in {'1', 'T', 'Y'})

    @staticmethod
    def _to_list(s):
        """Parse a bulk field to a list of values. Values could be scalars
        if the bulk field has one dimension, or tuples, if it has two
        dimensions. More than two dimensions are not supported.

        Bulk data looks like this:
        ;1;11;5;11/01/2007;5;11/01/2008;... (one dimension)
        ;2;11;2;5;11/01/2007;3;100.5;5;11/01/2008;3;101.0;... (two dimensions)

        That is, the first example has one dimension with 11 date values. The
        values themselves are pairs of type and value.

        The second example has two dimensions with 11 rows and each row having
        a pair of values (date and price). Each row is comprised of 4 fields:
        type;value;type;value.
        """
        s = Field._to_str(s)
        if not s or not s.startswith(';'):
            return
        items = []
        try:
            # first char is the delimiter. Use it to split the string
            # but also skip the beginning and ending delimiters.
            bits = s[1:-1].split(s[0])
            # norigbits = len(bits)
            dims = int(bits.pop(0))
            if not 1 <= dims <= 2:
                raise ValueError('Bulk field dimension not supported: ' + dims)
            rows = int(bits.pop(0))
            cols = 1
            if dims > 1:
                cols = int(bits.pop(0))
            # There are row*cols values and each has a type and value
            # so rows*cols*2 bits. Then there are three extra bits at
            # the front for dims, rows and cols.
            # assert 3 + rows * cols * 2 == norigbits
            for i in range(rows):
                item = []
                for j in range(cols):
                    ftype, value = int(bits.pop(0)), bits.pop(0)
                    item.append(Field._convert_bulk_field(ftype, value))
                if cols == 1:
                    items.append(item[0])
                else:
                    items.append(tuple(item))
        except:
            logging.exception('Error parsing bulk field, skipping remainder')
        return items

    @staticmethod
    def _convert_bulk_field(ftype, s):
        if ftype in {1, 4, 11}: return Field._to_str(s)
        if ftype in {2, 3, 13}: return Field._to_number(s)
        if ftype == 5:          return to_date(s)
        if ftype == 6:          return to_time(s)
        if ftype == 7:          return to_datetime(s)
        if ftype == 8:          return Field._to_list(s)
        if ftype == 9:          return to_date(s, fmt='%m/%y')
        if ftype == 10:         return Field._to_bool(s)
        if ftype == 12:         return Field._to_number(s)
        raise ValueError(f'Unexpected field type: {ftype}, value: {s}')

    @cachedstaticproperty
    def _exception_convertrs():
        return {
            'IDENTIFIER': Field._to_str,
            'RETCODE': Field._to_number,
            'NFIELDS': Field._to_number,
            'DATE': to_date,
            'CNTRY_OF_DOMICILE': lambda x: re.sub(r'[^A-Z]', r'', x),  # remove noise
            'CPN': Field._to_number,
        }


YELLOW_KEYS = ('Comdty', 'Equity', 'Muni', 'Pfd', 'M-Mkt',
               'Govt', 'Corp', 'Index', 'Curncy', 'Mtge')


class Ticker:

    @staticmethod
    def fix_case(ticker):
        """Fix case of BB ticker to <upper> <upper> ... <capitalized>
        so '01234abc89 Us EQUITY' goes to '01234ABC89 US Equity'
        """
        if not ticker:
            return
        if ' ' not in ticker:
            return ticker.upper()
        bits = ticker.split(' ')
        return ' '.join([_.upper() for _ in bits[:-1]] + [bits[-1].capitalize()])

    @staticmethod
    def is_bb_ticker(ticker):
        """Determine if ticker is a valid Bloomberg ticker. Not perfect.
        Must be of form <ticker> [extra] <type>.
        """
        if not ticker:
            return False
        bits = ticker.split(' ')
        if len(bits) < 2:
            return False
        return not bits[-1] not in YELLOW_KEYS


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
