import logging
import sys

import config
import numpy as np
import pandas as pd

import log
from bbdl import SFTPClient

np.set_printoptions(threshold=sys.maxsize)


log.configure_logging('cmd')
logger = logging.getLogger('cmd')


def upsert_from_dataframe(df, table, where="""WHERE date = current_date"""):
    """Will take a dataframe and update each specified row in the SQL table
    with the DF values -- DF columns MUST match SQL columns
    WHERE statement should be triple-quoted string
    Will not update any columns contained in the WHERE statement
    """
    update_string = f'UPDATE {table} SET \n'
    for idx, row in df.iterrows():
        upstr = update_string
        for col in list(df.columns):
            if col not in where:
                if col != df.columns[-1]:
                    if type(row[col] == str):
                        upstr += f"""{col} = '{row[col]}', \n"""
                    else:
                        upstr += f"""{col} = {row[col]}, \n"""
                elif type(row[col] == str):
                    upstr += f"""{col} = '{row[col]}' \n"""
                else:
                    upstr += f"""{col} = {row[col]} \n"""
        upstr += where
        print(upstr)
        # cursor.execute(upstr)
        # cursor.commit()`


def limit_fields_to_categories(reqfields, categories):
    """Filter fields to avoid expensive mistakes
    """
    allfields = Field.from_categories(categories) | Field.open_fields
    fields = sorted([f.upper() for f in reqfields if (f.upper() in allfields)])
    logger.info(f'Filtered {len(reqfields)} request fields to {len(fields)} match fields.')
    return fields


if __name__ == '__main__':

    fields = [
        'id_bb_unique',
        'id_cusip',
        'id_isin',
        'id_sedol1',
        'parsekyable_des',
        'security_des',
        'real_bloomberg_dept_des',
        'issue_dt',
        'cpn',
        'day_cnt_des',
        'cpn_freq',
        'fixed',
        'maturity',
        'is_perpetual',
        'crncy',
        'amt_issued',
        'cds_spread_ticker_5y',
        'call_days_notice',
        'soft_call_trigger',
        'soft_call_days',
        'soft_call_out_of',
        'soft_call_end',
        'call_schedule',
        'put_schedule',
        'soft_call_schedule',
        'expected_report_dt',
        'eqy_split_dt',
        'dvd_pay_dt',
        'dvd_ex_dt',
        'dvd_freq',
        'amt_outstanding',
        'eqy_sh_out',
        'trail_12m_dvd_per_sh',
        'px_last',
        'px_close_dt',
        'eqy_beta',
        'volatility_60d',
        'volatility_260d',
        'volatility_360d',
        'volume_avg_30d',
        'volume_avg_3m',
        'high_52week',
        'low_52week',
        'short_int',
        'fivey_mid_cds_spread',
        'thirtyday_impvol_100pt0pctmny_df',
        'curr_entp_val',
        'ebitda',
        'net_debt',
        'est_pe_cur_yr',
        'cur_mkt_cap',
        'is_int_expense',
        'market_status',
        'ult_parent_ticker_exchange',
        'id_bb_company',
        'issuer',
        'cntry_of_domicile',
        'country_iso',
        'gics_sub_industry',
        'earnings_conf_call_dt',
        'earnings_conf_call_time',
        'dvd_sh_last',
        'low_dt_52week',
        'high_dt_52week',
        'ddis_amt_outstanding_by_yr_bndln',
        'id_bb_global',
        'trail_12m_ebitda',
        'short_and_long_term_debt',
        'earnings_conf_call_phone_num',
        'earnings_conf_call_pin',
        'central_index_key_number',
        'eqy_weighted_avg_px',
        'volatility_30d',
        'eqy_fund_ticker',
        'rtg_moody',
        'rtg_fitch',
        'rtg_sp',
        'bdvd_next_est_ex_dt',
        'bdvd_next_est_pay_dt',
        'eqy_split_adjustment_factor',
        'bs_tot_non_cur_asset',
        'eqy_po_dt',
        'eqy_fund_crncy',
        'cf_free_cash_flow',
        'cash_and_marketable_securities',
        'hostname',
        'industry_sector',
        'first_settle_dt',
        'long_comp_name',
        'initial_conversion_premium',
        'primary_exchange_name',
        'projected_conversion_ratio',
        'conversion_ratio_at_issue',
        'cv_cnvs_ratio',
        'nxt_put_dt',
        'nxt_put_px',
        'px_ask',
        'px_bid',
        'fut_cnv_risk_ctd',
        'mty_years',
        'fut_ctd_ticker',
        'mand_maximum_conversion_ratio',
        'par_amt',
        'cv_parity',
        'hyper_conv_incr_share_factor',
        'sales_result_date',
        'sales_result_time',
        'all_holders_public_filings',
        'created_on',
        'cv_common_ticker_exch',
        'nxt_cpn_dt',
        'prev_cpn_dt',
        'security_typ2',
        'rtg_mdy_lt_corp_family',
        'rtg_sp_lt_lc_issuer_credit',
        'tot_debt_to_tot_cap',
        'fut_cnvs_factor',
        'yas_risk',
    ]

    theids = [
        #  'CDX IG CDSI S10 5Y Corp',
        #  'CDX IG INDU CDSI S2 5Y Curncy',
        #  'CDX HY CDSI S10 5Y PRV8 Curncy',
        'BBG00P2DPCT8 Corp',
        #  'TUH4 Comdty',
        #  'FVH4 Comdty',
        #  'TYH4 Comdty',
    ]

    with SFTPClient('bbg.data.ftp', config) as ftpdl:
        result = ftpdl.request(theids, fields, categories=['Security Master'])
        colmap = dict(result.columns)
        df = pd.DataFrame(result.data)
        print()
        # keep = lambda x: x is not None and '#N/A' not in str(x)
        # for col in df.columns:
            # df[col] = df[col].apply(lambda x: x if keep(x) else None)
        # df = df.where((pd.notnull(df)), None)
        # # for rows where all data except date is NA
        # df = df.dropna(thresh=2)
        # for _, row in df.iterrows():
            # row['date'] = pd.to_datetime(row['date']).date()
        # df[['date', 'security_des', 'amt_outstanding', 'eqy_sh_out', 'px_last', 'id_bb_unique']].sort_values(by='date')
        # engine = create_engine(
            # 'postgresql://jmilton:porTGRupH@tc-pg.cluster-cu31mkcxg3jz.us-east-1.rds.amazonaws.com:5432/Tenor'
        # )
        # df.to_sql(
            # 'bbdata', engine, if_exists='append', index=False, chunksize=200
        # )  # do NOT select 'replace'!

    # EXAMPLES:

    """
    fields = [f for f in get_fieldmap().keys() if f.startswith('FUT_CTD')]
    lookup(['TUZ0 Comdty', 'FVZ0 Comdty', 'TYZ0 Comdty'], fields, 'futctd')
    """
    tickers = [
    'CDX IG CDSI S10 5Y Corp',
    'CDX IG INDU CDSI S2 5Y Curncy',
    'CDX HY CDSI S10 5Y PRV8 Curncy'
    ]
    # data, errors = lookup(tickers, fields, 'test', headers=['USERNUMBER=4851089'])
    print('*** data')
    print('\n\n*** errors')

    # data, errors = lookup(tickers, fields, 'test_hist', datetime.date(2012, 1, 1), datetime.date(2012, 1, 31), headers=['USERNUMBER=4851089'])
    # import pprint
    # print '*** data'
    # pprint.pprint(data)
    # print '\n\n*** errors'
    # pprint.pprint(errors)
