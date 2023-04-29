# ------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       04_create_order_view.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
# ------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Snowpark DataFrame API
# SNOWFLAKE ADVANTAGE: Streams for incremental processing (CDC)
# SNOWFLAKE ADVANTAGE: Streams on views


from snowflake.snowpark import Session
# import snowflake.snowpark.types as T
import snowflake.snowpark.functions as SF


def create_pos_view(session):
    '''
    Create a view that flattens the POS data into a single table.
    We can do this one of two ways: either select before the join so
    it is more explicit, or just join on the full tables.
    The end result is the same, it's mostly a readibility question.
    '''
    session.use_schema('HARMONIZED')
    order_detail = session.table('HOL_DB.RAW_POS.ORDER_DETAIL').select(
        SF.col('ORDER_DETAIL_ID'),
        SF.col('LINE_NUMBER'),
        SF.col('MENU_ITEM_ID'),
        SF.col('QUANTITY'),
        SF.col('UNIT_PRICE'),
        SF.col('PRICE'),
        SF.col('ORDER_ID')
    )

    order_header = session.table('HOL_DB.RAW_POS.ORDER_HEADER').select(
        SF.col('ORDER_ID'),
        SF.col('TRUCK_ID'),
        SF.col('ORDER_TS'),
        SF.to_date(SF.col('ORDER_TS')).as_('ORDER_TS_DATE'),
        SF.col('ORDER_AMOUNT'),
        SF.col('ORDER_TAX_AMOUNT'),
        SF.col('ORDER_DISCOUNT_AMOUNT'),
        SF.col('LOCATION_ID'),
        SF.col('ORDER_TOTAL')
    )

    truck = session.table('HOL_DB.RAW_POS.TRUCK').select(
        SF.col('TRUCK_ID'),
        SF.col('PRIMARY_CITY'),
        SF.col('REGION'),
        SF.col('COUNTRY'),
        SF.col('FRANCHISE_FLAG'),
        SF.col('FRANCHISE_ID')
    )

    menu = session.table('HOL_DB.RAW_POS.MENU').select(
        SF.col('MENU_ITEM_ID'),
        SF.col('TRUCK_BRAND_NAME'),
        SF.col('MENU_TYPE'),
        SF.col('MENU_ITEM_NAME')
    )

    franchise = session.table('HOL_DB.RAW_POS.FRANCHISE').select(
        SF.col('FRANCHISE_ID'),
        SF.col('FIRST_NAME').as_('FRANCHISEE_FIRST_NAME'),
        SF.col('LAST_NAME').as_('FRANCHISEE_LAST_NAME')
    )

    location = session.table('HOL_DB.RAW_POS.LOCATION').select(
        SF.col('LOCATION_ID')
    )

    # order_detail = session.table("RAW_POS.ORDER_DETAIL")
    # order_header = session.table("RAW_POS.ORDER_HEADER")
    # truck = session.table("RAW_POS.TRUCK")
    # menu = session.table("RAW_POS.MENU")
    # franchise = session.table("RAW_POS.FRANCHISE")
    # location = session.table("RAW_POS.LOCATION")

    truck_on_franchise = truck.join(franchise, ["FRANCHISE_ID"], rsuffix='_f')

    # t_with_f = truck.join(franchise, truck['FRANCHISE_ID'] == franchise['FRANCHISE_ID'], rsuffix='_f')

    orderheader_on_truck_on_location = (
        order_header.join(truck_on_franchise, ["TRUCK_ID"], rsuffix='_t')
                    .join(location, ["LOCATION_ID"], rsuffix='_l'))

    # oh_w_t_and_l = order_header.join(t_with_f, order_header['TRUCK_ID'] == t_with_f['TRUCK_ID'], rsuffix='_t') \
    #                            .join(location, order_header['LOCATION_ID'] == location['LOCATION_ID'], rsuffix='_l')

    final_df = (
        order_detail.join(orderheader_on_truck_on_location, ["ORDER_ID"], rsuffix='_oh')
                    .join(menu, ["MENU_ITEM_ID"], rsuffix='_m'))

    # final_df = order_detail.join(oh_w_t_and_l, order_detail['ORDER_ID'] == oh_w_t_and_l['ORDER_ID'], rsuffix='_oh') \
    #                        .join(menu, order_detail['MENU_ITEM_ID'] == menu['MENU_ITEM_ID'], rsuffix='_m')

    final_df = final_df.select(
        SF.col("ORDER_ID"),
        SF.col("TRUCK_ID"),
        SF.col("ORDER_TS"),
        SF.col('ORDER_TS_DATE'),
        SF.col("ORDER_DETAIL_ID"),
        SF.col("LINE_NUMBER"),
        SF.col("TRUCK_BRAND_NAME"),
        SF.col("MENU_TYPE"),
        SF.col("PRIMARY_CITY"),
        SF.col("REGION"),
        SF.col("COUNTRY"),
        SF.col("FRANCHISE_FLAG"),
        SF.col("FRANCHISE_ID"),
        SF.col("FRANCHISEE_FIRST_NAME"),
        SF.col("FRANCHISEE_LAST_NAME"),
        SF.col("LOCATION_ID"),
        SF.col("MENU_ITEM_ID"),
        SF.col("MENU_ITEM_NAME"),
        SF.col("QUANTITY"),
        SF.col("UNIT_PRICE"),
        SF.col("PRICE"),
        SF.col("ORDER_AMOUNT"),
        SF.col("ORDER_TAX_AMOUNT"),
        SF.col("ORDER_DISCOUNT_AMOUNT"),
        SF.col("ORDER_TOTAL"))

    final_df.create_or_replace_view('POS_FLATTENED_V')


def create_pos_view_stream(session):
    session.use_schema('HARMONIZED')
    _ = session.sql(
        '''
        CREATE OR REPLACE STREAM POS_FLATTENED_V_STREAM
        ON VIEW POS_FLATTENED_V
        SHOW_INITIAL_ROWS = TRUE
        '''
    ).collect()


def test_pos_view(session):
    session.use_schema('HARMONIZED')
    tv = session.table('POS_FLATTENED_V')
    tv.limit(15).show()


# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os
    import sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

#    create_pos_view(session)
#    create_pos_view_stream(session)
#    test_pos_view(session)

    session.close()
