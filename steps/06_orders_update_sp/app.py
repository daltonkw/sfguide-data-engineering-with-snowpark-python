# ------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       06_orders_process_sp/app.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
# ------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Python Stored Procedures

# import time
from snowflake.snowpark import Session
# import snowflake.snowpark.types as ST
import snowflake.snowpark.functions as SF


def table_exists(session: Session, schema='', name=''):
    '''
    Check if a table exists in a schema.
    '''
    exists = session.sql(
        '''
        SELECT EXISTS
        (
        SELECT *
        FROM INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_SCHEMA = '{}' AND
            TABLE_NAME = '{}'
        )
        AS TABLE_EXISTS
        '''.format(schema, name)
    ).collect()[0]['TABLE_EXISTS']
    return exists


def create_orders_table(session: Session):
    '''
    Create the ORDERS table and add a metadata column.
    '''
    _ = session.sql(
        '''
        CREATE TABLE HARMONIZED.ORDERS
        LIKE HARMONIZED.POS_FLATTENED_V
        '''
    ).collect()
    _ = session.sql(
        '''
        ALTER TABLE HARMONIZED.ORDERS
        ADD COLUMN META_UPDATED_AT TIMESTAMP
        '''
    ).collect()


def create_orders_stream(session: Session):
    '''
    Create the ORDERS_STREAM stream.
    This only tracks changes to the ORDERS table, not the whole table.
    Change Data Capture Metadata Fields:
    Metadata$Action
    Metadata$IsUpdate
    Metadata$RowId
    '''
    _ = session.sql(
        '''
        CREATE STREAM HARMONIZED.ORDERS_STREAM
        ON TABLE HARMONIZED.ORDERS
        '''
    ).collect()


def merge_order_updates(session: Session):
    _ = session.sql(
        '''
        ALTER WAREHOUSE HOL_WH
        SET WAREHOUSE_SIZE = XLARGE
        WAIT_FOR_COMPLETION = TRUE
        '''
    ).collect()

    source = session.table('HARMONIZED.POS_FLATTENED_V_STREAM')
    target = session.table('HARMONIZED.ORDERS')

    columns_to_update = {
        c: source[c] for c in source.schema.names if 'METADATA' not in c
    }
    metadata_columns_to_update = {
        'META_UPDATED_AT': SF.current_timestamp()
    }
    updates = {**columns_to_update, **metadata_columns_to_update}

    target.merge(
        source,
        target['ORDER_DETAIL_ID'] == source['ORDER_DETAIL_ID'],
        # clauses – A list of matched or not-matched clauses specifying
        # the actions to perform when the values from this Table and
        # source match or not match on join_expr. These actions can
        # only be instances of WhenMatchedClause and
        # WhenNotMatchedClause, and will be performed sequentially
        # in this list.
        [SF.when_matched().update(updates),
         SF.when_not_matched().insert(updates)]
    )

    _ = session.sql(
        'ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL'
    ).collect()

# def table_exists(session, schema='', name=''):
#    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
#    return exists

# def create_orders_table(session):
#    _ = session.sql("CREATE TABLE HARMONIZED.ORDERS LIKE HARMONIZED.POS_FLATTENED_V").collect()
#    _ = session.sql("ALTER TABLE HARMONIZED.ORDERS ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

# def create_orders_stream(session):
#    _ = session.sql("CREATE STREAM HARMONIZED.ORDERS_STREAM ON TABLE HARMONIZED.ORDERS").collect()

# def merge_order_updates(session):
#    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

#    source = session.table('HARMONIZED.POS_FLATTENED_V_STREAM')
#    target = session.table('HARMONIZED.ORDERS')

    # TODO: Is the if clause supposed to be based on "META_UPDATED_AT"?
#    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
#    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
#   updates = {**cols_to_update, **metadata_col_to_update}

    # merge into DIM_CUSTOMER
#    target.merge(source, target['ORDER_DETAIL_ID'] == source['ORDER_DETAIL_ID'], \
#                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

#    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()


def main(session: Session, *args) -> str:
    # Create ORDERS table and ORDERS_STREAM if they don't exist
    if not table_exists(session, schema='HARMONIZED', name='ORDERS'):
        create_orders_table(session)
        create_orders_stream(session)

    # The process data
    merge_order_updates(session)

    return print('Successfully processed ORDERS')

    # return session.table('HARMONIZED.ORDERS').limit(5).show()

# def main(session: Session) -> str:
    # Create the ORDERS table and ORDERS_STREAM stream if they don't exist
#    if not table_exists(session, schema='HARMONIZED', name='ORDERS'):
#        create_orders_table(session)
#        create_orders_stream(session)

    # Process data incrementally
#    merge_order_updates(session)
#    session.table('HARMONIZED.ORDERS').limit(5).show()

#    return f"Successfully processed ORDERS"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os
    import sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
