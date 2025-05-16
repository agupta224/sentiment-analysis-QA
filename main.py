from os import getenv
import snowflake.connector
import logging
import os
from dotenv import load_dotenv
import pandas as pd
import warnings
import sys
from pprint import pformat
warnings.filterwarnings('ignore')

load_dotenv()



try:
    conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    authenticator="externalbrowser",
    account='fbhs_gpg.east-us-2.azure',
    warehouse='WH_DATAREAD_PROD',
    role='DATAREAD_PROD',
    telemetry_enabled=False
)
    def switch_db_schema(cursor, database: str, schema: str):
        cursor.execute(f'USE DATABASE {database}')
        cursor.execute(f'USE SCHEMA {schema}')

    cursor = conn.cursor()

    try:
        switch_db_schema(cursor, 'INFOMART_PROD', 'consumer_feedback')
        query = "SELECT * FROM rep_sentiment_input_v2"
        df_input = pd.read_sql(query, conn)

        switch_db_schema(cursor, 'EDP_BRONZE_PROD', 'sentiment')
        query_subcategory = "SELECT * FROM sentiment_subcategory_mapping" 
        df_subcategory = pd.read_sql(query_subcategory, conn)

        switch_db_schema(cursor, 'EDP_BRONZE_PROD', 'sentiment')
        query_category = "SELECT * FROM sentiment_category_mapping" 
        df_category = pd.read_sql(query_category, conn)

        switch_db_schema(cursor, 'EDP_BRONZE_PROD', 'sentiment')
        query_tag = "SELECT * FROM sentiment_tag_mapping" 
        df_tag = pd.read_sql(query_tag, conn)

        switch_db_schema(cursor, 'EDP_BRONZE_PROD', 'sentiment')
        query_output = "SELECT * FROM sentiment_output" 
        df_output = pd.read_sql(query_output, conn)

        def print_data_quality_report(report):
            for table_name, tests in report.items():
                print(f"Table: {table_name}")
                for test_name, test_details in tests.items():
                    print(f"     Test: {test_name}")
                    for i, t in enumerate(test_details, start=1):
                        case_label = f"Case-{i}" if test_name == 'Uniqueness' else "Case-"
                        print(f"           {case_label}: {t.get('type', 'N/A')}")
                        print(f"           - Description: {t.get('description', 'N/A')}")
                        print(f"           - Columns Used: {t.get('columns_used', [])}")
                        print(f"           - Outcome: {t.get('outcome', 'N/A')}")

                        if 'examples' in t and t['examples']:
                            has_empty_key = any("" in ex for ex in t['examples'])
                            if has_empty_key:
                                line_values = " ".join(f"{v}" for ex in t['examples'] for k, v in ex.items() if k == "")
                                print(f"           - Example instances: {line_values}")
                                for ex in t['examples']:
                                    for k, v in ex.items():
                                        if k != "":
                                            print(f"                   - {k}: {v}")
                                #print("\n")
                            else:
                                print(f"           - Example instances:")
                                for ex in t['examples']:
                                    for k, v in ex.items():
                                        print(f"                 - {k}: {v}")
                        print("\n")
                #print("\n")


        
        
        df_input.columns = [col.upper() for col in df_input.columns]
        columns = df_input.columns.tolist()
        df_subcategory.columns = [col.upper() for col in df_subcategory.columns]
        df_category.columns = [col.upper() for col in df_category.columns]
        df_tag.columns = [col.upper() for col in df_tag.columns]

        
        created_at_col = 'CREATED_AT'
        review_id_col = 'REVIEW_ID'
        product_id_col = 'PRODUCT_ID'
        review_title_col = 'REVIEW_TITLE'
        subcategory_col = 'SUBCATEGORY_LABEL'
        category_col = 'CATEGORY_LABEL'
        subcategory_sentence_col = 'SUBCATEGORY_SENTENCE'
        tag_col = 'TAG'

        #print_heading("INPUT TABLE CHECKS")
        # 1. Check for duplicate rows excluding CREATED_AT 
        created_at_col = created_at_col.upper()
        cols_for_dup_check = [col for col in columns if col != created_at_col]
        
        duplicate_mask = df_input.duplicated(subset=cols_for_dup_check, keep='first')
        dup_row_count = duplicate_mask.sum()
        
        duplicate_input_review_ids = set(df_input.loc[duplicate_mask, 'REVIEW_ID'])


        # 2. Check duplicate REVIEW_IDs within PRODUCT_ID 
        product_id_col = product_id_col.upper()

        if review_id_col in columns and product_id_col in columns:
            # Identify duplicated rows
            duplicated_rows = df_input[df_input.duplicated(subset=[product_id_col, review_id_col], keep=False)]
            # Count unique duplicate reviews
            unique_dup_reviews = duplicated_rows[review_id_col].unique()
            dup_review_count = duplicated_rows[review_id_col].nunique()
        else:
            dup_review_count = None
        
        #print_heading("SUBCATEGORY TABLE CHECKS")
        duplicates_sub = df_subcategory[df_subcategory.duplicated(
        subset=['PRODUCT_ID', 'BRAND_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT'],
        keep='first')]
        
        # Check if any duplicates exist
        if duplicates_sub.empty:
            dup_subcat=[]
        else:
            dup_subcat=duplicates_sub['REVIEW_ID'].unique()
            
            
        # With CREATED_AT
        duplicates_sub_with_created_at = df_subcategory[df_subcategory.duplicated(
            subset=['PRODUCT_ID', 'BRAND_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT', 'CREATED_AT'],
            keep='first')]

        if duplicates_sub_with_created_at.empty:
            dup_subcat_with_created_at=[]
        else:
            dup_subcat_with_created_at = duplicates_sub_with_created_at['REVIEW_ID'].unique()
            
        Date_variations = df_subcategory.groupby(['PRODUCT_ID', 'REVIEW_ID'])['DATE'].nunique()
        inconsistent_Date = Date_variations[Date_variations > 1]
        dup_subcat_without_date_product_title = inconsistent_Date.index.get_level_values('REVIEW_ID').unique()

        duplicates_sub_sentences = df_subcategory[df_subcategory.duplicated(
            subset=['PRODUCT_ID','REVIEW_ID', 'SUBCATEGORY_SENTENCE'],
            keep='first')]

        if duplicates_sub_sentences.empty:
            dup_subcat_sentences=[]
        else:
            dup_subcat_sentences = duplicates_sub_sentences['REVIEW_ID'].unique()
        
        timestamp_variations = df_subcategory.groupby(['PRODUCT_ID', 'REVIEW_ID'])['CREATED_AT'].nunique()
        inconsistent_timestamps = timestamp_variations[timestamp_variations > 1]
        dup_subcat_timestamp = inconsistent_timestamps.index.get_level_values('REVIEW_ID').unique()


        Product_variations = df_subcategory.groupby(['PRODUCT_ID', 'REVIEW_ID'])['PRODUCT_TITLE'].nunique()
        inconsistent_Product = Product_variations[Product_variations > 1]
        dup_subcat_1 = inconsistent_Product.index.get_level_values('REVIEW_ID').unique()
# 2. Ensure there are no blank values in any columns except ProductTitle and ReviewTitle
        null_empty_columns_subcat = []

        columns_to_check = [col for col in df_subcategory.columns if col not in ['PRODUCT_TITLE', 'REVIEW_TITLE']]
        empty_sub=[]
        for col in columns_to_check:
            empty_rows = df_subcategory[df_subcategory[col].astype(str).str.strip() == '']
            if not empty_rows.empty:
                null_empty_columns_subcat.append(col)
                empty_sub = empty_rows['REVIEW_ID'].unique()


        null_empty_columns_sub = []

        columns_to_check1 = [col for col in df_subcategory.columns]
        null_sub=[]
        for col in columns_to_check1:
            null_rows = df_subcategory[df_subcategory[col].isnull()]
            if not null_rows.empty:
                null_empty_columns_sub.append(col)
                null_sub = null_rows['REVIEW_ID'].unique()



        #print_heading("CATEGORY TABLE CHECKS")
        # 1. Check for duplicate rows in Category Table
        duplicate_subset = ['PRODUCT_ID', 'CATEGORY_LABEL', 'SUBCATEGORY_LABEL']

        if df_category.duplicated(subset=duplicate_subset).sum() == 0:
            dup_category=[]
        else:
            duplicates = df_category[df_category.duplicated(subset=duplicate_subset, keep=False)]
            dup_category=duplicates[duplicate_subset].drop_duplicates()
            

        product_subcategory_pairs = df_category[[product_id_col, subcategory_col]].drop_duplicates()
        subcategory_pairs = df_subcategory[[product_id_col, subcategory_col]].drop_duplicates()
        missing_pairs = product_subcategory_pairs.merge(subcategory_pairs, on=[product_id_col, subcategory_col], how='left', indicator=True)
        missing_pairs_count = missing_pairs[missing_pairs['_merge'] == 'left_only'].shape[0]
       

        # 3. Ensure the number of categories for each product is less than or equal to 25
        unique_categories_per_product = df_category.groupby(product_id_col)[category_col].nunique()
        categories_check = (unique_categories_per_product <= 25)

        # Products that failed the check
        failed_products = unique_categories_per_product[~categories_check]

        subcategory_category_mapping = df_category.groupby(
            [product_id_col, subcategory_col]
        )[category_col].nunique().reset_index(name='unique_category_count')
        one_to_one_mapping_check = (subcategory_category_mapping['unique_category_count'] == 1).all()

        # Step 2: Filter to where the mapping is not 1-to-1
        invalid_category_mappings = subcategory_category_mapping[
            subcategory_category_mapping['unique_category_count'] > 1
        ]


        # Step 3: Join back to original DataFrame to get the full details
        invalid_category_rows = df_category.merge(
            invalid_category_mappings[[product_id_col, subcategory_col]],
            on=[product_id_col, subcategory_col],
            how='inner'
        )
        mapping = invalid_category_rows[[product_id_col, subcategory_col, category_col]].drop_duplicates()

        
        null_columns_cat = []

        for col in df_category.columns:
            null_count = df_category[col].isnull().sum()
            if null_count > 0:
                null_columns_cat.append(col)

        empty_columns_cat = []

        for col in df_category.columns:
            empty_count = (df_category[col].astype(str).str.strip() == '').sum()
            if empty_count > 0:
                empty_columns_cat.append(col)




        #print_heading("TAG TABLE CHECKS")
        # 1. Check for duplicate rows in Tag Table
        duplicate_subset_tag = ['PRODUCT_ID', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'TAG']

        # Check for duplicates
        if df_tag.duplicated(subset=duplicate_subset_tag).sum() == 0:
            #print("All rows are unique without CREATED_AT for tag table.")
            dup_tag=[]
        else:
            duplicates = df_tag[df_tag.duplicated(subset=duplicate_subset_tag, keep=False)]
            dup_tag=duplicates[duplicate_subset_tag].drop_duplicates()
            

        product_subcategory_sentence_pairs = df_tag[[product_id_col, subcategory_col, subcategory_sentence_col]].drop_duplicates()
        subcategory_sentence_pairs = df_subcategory[[product_id_col, subcategory_col, subcategory_sentence_col]].drop_duplicates()
        missing_pairs = product_subcategory_sentence_pairs.merge(subcategory_sentence_pairs, on=[product_id_col, subcategory_col, subcategory_sentence_col], how='left', indicator=True)
        missing_pairs_count_1 = missing_pairs[missing_pairs['_merge'] == 'left_only'].shape[0]

        # 3. Ensure each subcategory_sentence has a 1-1 mapping with a tag within a product
        # Step 1: Count unique tags per (product_id, subcategory_sentence)
        subcategory_sentence_tag_mapping = df_tag.groupby(
            [product_id_col, subcategory_sentence_col,subcategory_col]
        )[tag_col].nunique().reset_index(name='unique_tag_count')
        one_to_one_mapping_check_tag = (subcategory_sentence_tag_mapping['unique_tag_count'] == 1).all()

        # Step 2: Filter to where the mapping is not 1-to-1
        invalid_mappings_tag = subcategory_sentence_tag_mapping[
            subcategory_sentence_tag_mapping['unique_tag_count'] > 1
        ]

        # Step 3: Join back to original DataFrame to get the relevant rows
        invalid_rows_tag = df_tag.merge(
            invalid_mappings_tag[[product_id_col, subcategory_sentence_col]],
            on=[product_id_col, subcategory_sentence_col],
            how='inner'
        )
        invalid = invalid_rows_tag[[product_id_col, subcategory_sentence_col, tag_col]].drop_duplicates()

        
        null_columns_tag = []

        for col in df_tag.columns:
            null_count_tag = df_tag[col].isnull().sum()
            if null_count_tag > 0:
                null_columns_tag.append(col)


        examples_null_tag = []
        for col in null_columns_tag:
            null_rows_tag = df_tag[col].isnull()
            for i, row in null_rows_tag.iterrows():
                example = {
                "ProductID": row.get("PRODUCT_ID"),
                "subcategory_sentence": row.get("SUBCATEGORY_SENTENCE"),
                "subcategory_label": row.get("SUBCATEGORY_LABEL"),
                "tag_Label": row.get("TAG")
                }
                examples_null_tag.append({f"Example-{len(examples_null_tag)+1}": example})
                
                if len(examples_null_tag) >= 2:
                    break
            if len(examples_null_tag) >= 2:
                    break

        empty_columns_tag = []

        for col in df_tag.columns:
            empty_count_tag = (df_tag[col].astype(str).str.strip() == '').sum()
            if empty_count_tag > 0:
                empty_columns_tag.append(col)

        
        examples = []
        for col in empty_columns_tag:
            empty_rows = df_tag[df_tag[col].astype(str).str.strip() == '']
            for i, row in empty_rows.iterrows():
                example = {
                "ProductID": row.get("PRODUCT_ID"),
                "subcategory_sentence": row.get("SUBCATEGORY_SENTENCE"),
                "subcategory_label": row.get("SUBCATEGORY_LABEL"),
                "tag_Label": row.get("TAG")
                }
                examples.append({f"Example-{len(examples)+1}": example})
                
                if len(examples) >= 2:
                    break
            if len(examples) >= 2:
                    break




        #print_heading("OUTPUT TABLE CHECKS")
        duplicates = df_output[df_output.duplicated(
    subset=['PRODUCT_ID', 'BRAND_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT', 'CATEGORY_LABEL', 'TAG'],
    keep='first')]

        # Check and print results
        if duplicates.empty:
            dup=[]
        else:
            dup=duplicates['REVIEW_ID'].unique()

        duplicates_1 = df_output[df_output.duplicated(
        subset=['PRODUCT_ID', 'BRAND_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT', 'CATEGORY_LABEL', 'TAG', 'CREATED_AT'],
        keep='first')]

        # Check and print results
        if duplicates_1.empty:
            dup1=[]
        else:
            dup1=duplicates_1['REVIEW_ID'].unique()
            

        timestamp_variations_output = df_output.groupby(['PRODUCT_ID', 'REVIEW_ID'])['CREATED_AT'].nunique()
        inconsistent_timestamps_output = timestamp_variations_output[timestamp_variations_output > 1]
        dup_output_timestamp = inconsistent_timestamps_output.index.get_level_values('REVIEW_ID').unique()

        duplicates_sentences = df_output[df_output.duplicated(
            subset=['PRODUCT_ID','REVIEW_ID', 'SUBCATEGORY_SENTENCE'],
            keep='first')]

        if duplicates_sentences.empty:
            dup_sentences=[]
        else:
            dup_sentences = duplicates_sentences['REVIEW_ID'].unique()
            
        
        Date_variations_output = df_output.groupby(['PRODUCT_ID', 'REVIEW_ID'])['DATE'].nunique()
        inconsistent_Date_output = Date_variations_output[Date_variations_output > 1]
        dup_with_created_at_output = inconsistent_Date_output.index.get_level_values('REVIEW_ID').unique()


        Product_variations_output = df_output.groupby(['PRODUCT_ID', 'REVIEW_ID'])['PRODUCT_TITLE'].nunique()
        inconsistent_Product_output = Product_variations_output[Product_variations_output > 1]
        dup_Product_output = inconsistent_Product_output.index.get_level_values('REVIEW_ID').unique()

        # Group by and count distinct TAGs
        grouped = (
            df_output
            .groupby(['PRODUCT_ID', 'REVIEW_ID', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE'])
            .agg(distinct_tag_count=('TAG', pd.Series.nunique))
            .reset_index()
        )

        # Filter for rows with more than one distinct TAG
        df_result_tag_output = grouped[grouped['distinct_tag_count'] > 1]

        # Boolean result: True if such rows exist, False otherwise
        has_multiple_tags = not df_result_tag_output.empty
        #print(has_multiple_tags)
        # Define the columns to compare
        compare_cols_output = ['PRODUCT_ID', 'REVIEW_ID', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT']

        # Get distinct rows from both DataFrames
        distinct_output = df_output[compare_cols_output].drop_duplicates()
        distinct_subcategory = df_subcategory[compare_cols_output].drop_duplicates()

        # Perform anti-join to find rows in df_output not in df_subcategory
        missing_rows = distinct_output.merge(distinct_subcategory, on=compare_cols_output, how='left', indicator=True)
        df_missing = missing_rows[missing_rows['_merge'] == 'left_only'].drop(columns=['_merge'])

        # Boolean flag indicating if any such rows exist
        has_missing_subcategories = not df_missing.empty
       # print(has_missing_subcategories)

        # Define the columns to compare
        compare_cols_cat = ['PRODUCT_ID', 'SUBCATEGORY_LABEL', 'CATEGORY_LABEL']

        # Get distinct rows from both DataFrames
        distinct_output_cat = df_output[compare_cols_cat].drop_duplicates()
        distinct_category = df_category[compare_cols_cat].drop_duplicates()

        # Perform anti-join to find rows in df_output not in df_category
        missing_rows = distinct_output_cat.merge(distinct_category, on=compare_cols_cat, how='left', indicator=True)
        df_missing_category = missing_rows[missing_rows['_merge'] == 'left_only'].drop(columns=['_merge'])

        # Boolean flag indicating if any such rows exist
        has_missing_categories = not df_missing_category.empty
        #print(has_missing_categories)

        # Define the columns to compare
        compare_cols_tag = ['PRODUCT_ID', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'TAG']

        # Get distinct rows from both DataFrames
        distinct_output_tag = df_output[compare_cols_tag].drop_duplicates()
        distinct_tag = df_tag[compare_cols_tag].drop_duplicates()

        # Perform anti-join to find rows in df_output not in df_tag
        missing_rows = distinct_output_tag.merge(distinct_tag, on=compare_cols_tag, how='left', indicator=True)
        df_missing_tags = missing_rows[missing_rows['_merge'] == 'left_only'].drop(columns=['_merge'])

        # Boolean flag indicating if any such rows exist
        has_missing_tags = not df_missing_tags.empty
        #print(has_missing_tags)





#checking for null and empty values in output table
        columns_to_check_output = [col for col in df_output.columns if col not in ['PRODUCT_TITLE', 'REVIEW_TITLE']]

        null_columns_output = {}
        review_ids_null_output = []  # Move this outside the loop to collect all review IDs

        for col in columns_to_check_output:
            null_check = df_output[df_output[col].isnull()]
            if not null_check.empty:
                review_ids = null_check['REVIEW_ID'].dropna().unique().tolist()
                count_null = null_check.shape[0]
                null_columns_output[col] = {
                    'count': count_null,
                    'review_ids': review_ids[:5]
                }
                review_ids_null_output.extend(review_ids)  

        empty_columns_output = {}
        review_ids_empty_output = []  # Move this outside the loop to collect all review IDs

        for col in columns_to_check_output:
            empty_check = df_output[df_output[col].astype(str).str.strip() == '']
            if not empty_check.empty:
                review_ids = empty_check['REVIEW_ID'].dropna().unique().tolist()
                count_empty = empty_check.shape[0]
                empty_columns_output[col] = {
                    'count': count_empty,
                    'review_ids': review_ids[:5]
                }
                review_ids_empty_output.extend(review_ids)  


        switch_db_schema(cursor, 'EDP_BRONZE_PROD', 'sentiment')
        query = """
        WITH PROD_SCAT AS (
            SELECT DISTINCT BRAND_ID, PRODUCT_ID, REVIEW_SOURCE_ID, REVIEW_ID, REVIEW_TITLE, REVIEW_TEXT, DATE, PRODUCT_TITLE, SUBCATEGORY_LABEL,
            SUBCATEGORY_SENTENCE, SENTIMENT 
            FROM sentiment_subcategory_mapping
        ),
        PROD_CAT AS (
            SELECT DISTINCT PRODUCT_ID, CATEGORY_LABEL, SUBCATEGORY_LABEL 
            FROM sentiment_category_mapping
        ),
        PROD_TAG AS (
            SELECT DISTINCT PRODUCT_ID, SUBCATEGORY_LABEL, SUBCATEGORY_SENTENCE, 
            LOWER(FIRST_VALUE(TAG) OVER(PARTITION BY PRODUCT_ID, SUBCATEGORY_LABEL, SUBCATEGORY_SENTENCE ORDER BY CREATED_AT)) AS TAG 
            FROM sentiment_tag_mapping
        ),
        JOIN_TAB AS (
            SELECT DISTINCT S.BRAND_ID, S.PRODUCT_ID, S.REVIEW_SOURCE_ID, S.REVIEW_ID, S.REVIEW_TITLE, S.REVIEW_TEXT, S.DATE, S.PRODUCT_TITLE, 
            C.CATEGORY_LABEL, S.SUBCATEGORY_LABEL, S.SUBCATEGORY_SENTENCE, S.SENTIMENT, T.TAG
            FROM PROD_SCAT S 
            JOIN PROD_CAT C ON S.SUBCATEGORY_LABEL = C.SUBCATEGORY_LABEL and s.product_id=c.product_id 
            JOIN PROD_TAG T ON S.SUBCATEGORY_LABEL = T.SUBCATEGORY_LABEL AND S.SUBCATEGORY_SENTENCE = T.SUBCATEGORY_SENTENCE and s.product_id=t.product_id 
        ) 
        SELECT * FROM JOIN_TAB
        """
        df_merged = pd.read_sql(query, conn)
        switch_db_schema(cursor, 'EDP_BRONZE_PROD', 'sentiment')
        df_output_1=pd.read_sql("SELECT BRAND_ID, PRODUCT_ID, REVIEW_SOURCE_ID, REVIEW_ID, REVIEW_TITLE, REVIEW_TEXT, DATE, PRODUCT_TITLE, CATEGORY_LABEL, SUBCATEGORY_LABEL, SUBCATEGORY_SENTENCE, SENTIMENT, TAG FROM sentiment_output" ,conn)

        comparison = df_output_1.merge(df_merged, how='left', indicator=True)
        is_subset = comparison['_merge'].eq('both').all()


        result = {
            "REP_SENTIMENT_INPUT_V2 ": {
                "Uniqueness": [
                    {
                        "type": "Duplicate rows [All cols excluding CREATED_AT]",
                        "description": "Checking if an entire row is duplicated or not",
                        "columns_used": ["UNIQUE_ID","BRAND_ID","REVIEW_SOURCE_ID","PRODUCT_ID", "REVIEW_ID","REVIEW_TITLE","REVIEW_TEXT","DATE","PRODUCT_TITLE","UPDATE_AT"],
                        "outcome": f"Found {dup_row_count} duplicated REVIEW IDs",
                        "examples": [{"ReviewID": review_id} for review_id in list(duplicate_input_review_ids)[:2]] if duplicate_input_review_ids else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate REVIEW_ID within PRODUCT",
                        "description": "Checking number of duplicate review ids within each product",
                        "columns_used": ["PRODUCT_ID", "REVIEW_ID"],
                        "outcome": f"Found {dup_review_count} duplicate rows",
                        "examples": [{"ReviewID": f"{unique_dup_reviews[0]}"}, {"ReviewID": f"{unique_dup_reviews[1]}"}]
                    }
                ],
            },
            "SENTIMENT_SUBCATEGORY_MAPPING ": {
                "Uniqueness": [
                    {
                        "type": "Duplicate rows [All cols excluding CREATED_AT]",
                        "description": "Checking if the whole row is duplicated, excluding CREATED_AT since it's just the timestamp when the record was added.",
                        "columns_used": ['PRODUCT_ID', 'BRAND_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT'],
                        "outcome": f"Found {len(dup_subcat)} duplicated REVIEW IDs",
                        "examples": [{"ReviewID": f"{dup_subcat[i]}"} for i in range(min(2, len(dup_subcat)))] if len(dup_subcat) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate rows [All cols including CREATED_AT]",
                        "description": "Checking if any row in the data has been completely duplicated — including the CREATED_AT timestamp",
                        "columns_used": ['PRODUCT_ID', 'BRAND_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT', 'CREATED_AT'],
                        "outcome": f"Found {len(dup_subcat_with_created_at)} duplicate rows",
                        "examples": [{"ReviewID": f"{dup_subcat_with_created_at[i]}"} for i in range(min(2, len(dup_subcat_with_created_at)))] if len(dup_subcat_with_created_at) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate REVIEW_ID but with different Dates",
                        "description": "Checking if a REVIEW_ID got processed with different DATE",
                        "columns_used": ['PRODUCT_ID', 'REVIEW_ID', 'DATE'],
                        "outcome": f"Found {len(dup_subcat_without_date_product_title)} duplicate rows",
                        "examples": [{"ReviewID": f"{dup_subcat_without_date_product_title[i]}"} for i in range(min(2, len(dup_subcat_without_date_product_title)))] if len(dup_subcat_without_date_product_title) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate Subcateogry sentence",
                        "description": "Checking if a subcategory sentence got duplicated within a REVIEW_ID for a product",
                        "columns_used": ['PRODUCT_ID','REVIEW_ID', 'SUBCATEGORY_SENTENCE'],
                        "outcome": f"Found {len(dup_subcat_sentences)} duplicate rows",
                        "examples": [{"ReviewID": f"{dup_subcat_sentences[i]}"} for i in range(min(2, len(dup_subcat_sentences)))] if len(dup_subcat_sentences) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate REVIEW_ID on timestamp",
                        "description": "Checking if a REVIEW_ID under a PRODUCT_ID appears with different timestamps, which indicates the review was processed multiple times",
                        "columns_used": ['PRODUCT_ID','REVIEW_ID', 'CREATED_AT'],
                        "outcome": f"Found {len(dup_subcat_timestamp)} duplicate rows",
                        "examples": [{"ReviewID": f"{dup_subcat_timestamp[i]}"} for i in range(min(2, len(dup_subcat_timestamp)))] if len(dup_subcat_timestamp) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate REVIEW_ID but with different Product Titles",
                        "description": "Checking if a REVIEW_ID got processed with different Product Title",
                        "columns_used": ['PRODUCT_ID', 'REVIEW_ID','PRODUCT_TITLE'],
                        "outcome": f"Found {len(dup_subcat_1)} duplicate rows",
                        "examples": [{"ReviewID": f"{dup_subcat_1[i]}"} for i in range(min(2, len(dup_subcat_1)))] if len(dup_subcat_1) > 0 else [{"ReviewID": "None"}]
                    },
                ],
                "NULL CHECK ANALYSIS":[
                    {
                        "type": "Null checks",
                        "description": "Checking Null values on all columns",
                        "columns_used": ['PRODUCT_ID','BRAND_ID','REVIEW_SOUCRE_ID', 'REVIEW_ID', 'REVIEW_TEXT','REVIEW_TITLE','PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT','CREATED_AT'],
                        "outcome": [{"COLUMN": f"{null_empty_columns_sub}"}for i in range(0,len(null_empty_columns_sub))] if len(null_empty_columns_sub)>0 else [{"All columns are": "N0N NULL"}],
                        "examples": [{"ReviewID": f"{null_sub[i]}"} for i in range(min(2, len(null_sub)))] if len(null_sub) > 0 else [{"ReviewID": "None"}]
                    }
                ],
                "EMPTY VALUE CHECK ANALYSIS":[
                    {
                        "type": "Empty value checks",
                        "description": "Checking Empty values on all columns excluding PRODUCT_TITLE and REVIEW_TITLE",
                        "columns_used": ['PRODUCT_ID','BRAND_ID','REVIEW_SOUCRE_ID', 'REVIEW_ID', 'REVIEW_TEXT', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT','CREATED_AT'],
                        "outcome": [{"COLUMN": f"{null_empty_columns_subcat}"}for i in range(0,len(null_empty_columns_subcat))] if len(null_empty_columns_subcat)>0 else [{"All columns are": "N0N EMPTY"}],
                        "examples": [{"ReviewID": f"{empty_sub[i]}"} for i in range(min(2, len(empty_sub)))] if len(empty_sub) > 0 else [{"ReviewID": "None"}]
                    }
                ],
            },
            "SENTIMENT_CATEGORY_MAPPING ":{
                "Uniqueness": [
                    {
                        "type": "Duplicate rows [All cols excluding CREATED_AT]",
                        "description": "Checking duplicate rows excluding CREATED_AT",
                        "columns_used": ['PRODUCT_ID', 'CATEGORY_LABEL', 'SUBCATEGORY_LABEL'],
                        "outcome": f"Found {len(dup_category)} duplicated rows",
                        "examples": [{"ProductID": f"{dup_category[i][0]}","Category_label":f"{dup_category[i][1]}","Subcategory_Label":f"{dup_category[i][2]}"} for i in range(min(2, len(dup_category)))] if len(dup_category) > 0 else [{"": "None"}]
                    },
                ],
                "Product and Subcategory Pair Validation":[
                    {   "type":"Product and subcategory pair check",
                        "description": "Checking that each PRODUCT_ID, subcategory pair exists in the Subcategory Table",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_LABEL'],
                        "outcome": f"There are {int(missing_pairs_count)} product-subcategory pairs in the category table that are missing in the subcategory table.",
                    },
                ],
                "Category Count Validation per Product":[
                    {
                        "type":"Category Limit Check per Product (≤ 25)",
                        "description": "Checking that the number of categories for each product is less than or equal to 25",
                        "columns_used": ['PRODUCT_ID', 'CATEGORY_LABEL'],
                        "outcome": ["All products have 25 or fewer categories."] if failed_products.empty else["Few Products are more than 25 categories"] ,
                        "examples": [{"Products": f"{failed_products[i]}"} for i in range(min(2, len(failed_products)))] if len(failed_products) > 0 else [{"Failed_Products": "None"}]
                    },
                ],
                "1:1 Subcategory to Category Mapping within Product":[
                    {
                        "type": "Validating One-to-One Mapping of Subcategory and Category",
                        "description": "Each subcategory within a product should map to only one unique category",
                        "columns_used": ['PRODUCT_ID', 'CATEGORY_LABEL', 'SUBCATEGORY_LABEL'],
                        "outcome": ["All subcategories within each product have a unique category."] if one_to_one_mapping_check else["Some subcategories within a product are linked to multiple categories."] ,
                        "examples": [{f"Example-{i+1}": {"ProductID": mapping.iloc[i][product_id_col], "Subcategory_label": mapping.iloc[i][subcategory_col], "Category_Label": mapping.iloc[i][category_col]}} for i in range(min(2, len(mapping)))] if len(mapping) > 0 else [{"": "None"}]
                    },
                ],
                "NULL CHECK ANALYSIS":[
                    {
                        "type": "Null checks",
                        "description": "Checking Null values on all columns",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_LABEL', 'CATERGORY_LABEL','CREATED_AT'],
                        "outcome": [{"COLUMN": f"{null_columns_cat}"}for i in range(0,len(null_columns_cat))] if len(null_columns_cat)>0 else [{"All columns are": "N0N NULL"}],
                        "examples": [{"COLUMN_NAME": f"{null_columns_cat[i]}"} for i in range(min(2, len(null_columns_cat)))] if len(null_columns_cat) > 0 else [{"": "None"}]
                    }
                ],
                "EMPTY VALUE CHECK ANALYSIS":[
                    {
                        "type": "Empty value checks",
                        "description": "Checking Empty values on all columns",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_LABEL', 'CATERGORY_LABEL','CREATED_AT'],
                        "outcome": [{"COLUMN": f"{empty_columns_cat}"}for i in range(0,len(empty_columns_cat))] if len(empty_columns_cat)>0 else [{"All columns are": "N0N EMPTY"}],
                        "examples": [{"COLUMN_NAME": f"{empty_columns_cat[i]}"} for i in range(min(2, len(empty_columns_cat)))] if len(empty_columns_cat) > 0 else [{"": "None"}]
                    }
                ],
            },
             "SENTIMENT_TAG_MAPPING ":{
                "Uniqueness": [
                    {
                        "type": "Duplicate Rows excluding CREATED_AT",
                        "description": "Checking duplicates based on all columns excluding CREATED_AT",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_SENTENCE', 'SUBCATEGORY_LABEL','TAG'],
                        "outcome": f"Found {len(dup_tag)} duplicated rows",
                        "examples": [{"ProductID": f"{dup_tag[i][0]}","SubCategory_label":f"{dup_tag[i][1]}","Subcategory_sentence":f"{dup_tag[i][2]}","TAG":f"{dup_tag[i][3]}"} for i in range(min(2, len(dup_tag)))] if len(dup_tag) > 0 else [{"": "None"}]
                    },
                ],
                "Check for Valid Subcategory Combinations":[
                    {   "type":"Validating Product-Subcategory-Sentence Mapping",
                        "description": "The combination of product_id, subcategory, and subcategory_sentence should exist in the subcategory table.",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_LABEL','SUBCATEGORY_SENTENCE'],
                        "outcome": f"Count of missing product subcategory sentence pairs in the tag table: {int(missing_pairs_count_1)}",
                    },
                ],
                "Subcategory & Sentence Pair to Tag 1:1 Mapping Check":[
                    {
                        "type": "Validating Unique Tag per Subcategory Sentence within Product",
                        "description": "Each subcategory_sentence within a product should be uniquely mapped to one tag, ensuring a one-to-one relationship.",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_LABEL','SUBCATEGORY_SENTENCE','TAG'],
                        "outcome": ["All subcategory sentences within each product have a unique tag."] if one_to_one_mapping_check_tag else["Some subcategory sentences within a product are linked to multiple tags."] ,
                        "examples": [{f"Example-{i+1}": {"ProductID": invalid.iloc[i][product_id_col], "subcategory_sentence": invalid.iloc[i][subcategory_sentence_col], "tag_Label": invalid.iloc[i][tag_col]}} for i in range(min(2, len(invalid)))] if len(invalid) > 0 else [{"": "None"}]
                    },
                ],
                "NULL CHECK ANALYSIS":[
                    {
                        "type": "Null checks",
                        "description": "Checking Null values on all columns",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_SENTENCE', 'SUBCATEGORY_LABEL','TAG','CREATED_AT'],
                        "outcome": [{"COLUMN": f"{null_columns_tag}"}for i in range(0,len(null_columns_tag))] if len(null_columns_tag)>0 else [{"All columns are": "N0N NULL"}],
                        "examples": examples_null_tag if examples_null_tag else [{"": "None"}]
                    }
                ],
                "EMPTY VALUE CHECK ANALYSIS":[
                    {
                        "type": "Empty value checks",
                        "description": "Checking Empty values on all columns",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_SENTENCE', 'SUBCATEGORY_LABEL','TAG','CREATED_AT'],
                        "outcome": [{"COLUMN": f"{empty_columns_tag}"}for i in range(0,len(empty_columns_tag))] if len(empty_columns_tag)>0 else [{"All columns are": "N0N EMPTY"}],
                        "examples": examples
                    }
                ],
            },
            "SENTIMENT_OUTPUT":{
                "Uniqueness": [
                    {
                        "type": "Duplicate rows [All cols excluding CREATED_AT]",
                        "description": "Checking if an entire row is duplicated or not excluding CREATED_AT",
                        "columns_used": ['PRODUCT_ID', 'BRAND_DI', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT', 'CATEGORY_LABEL', 'TAG'],
                        "outcome": f"Found {len(dup)} duplicated REVIEW IDs",
                        "examples": [{"ReviewID": f"{dup[i]}"} for i in range(min(2, len(dup)))] if len(dup) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate rows [All cols including CREATED_AT]",
                        "description": "Checking if a complete row was duplicated including CREATED_AT",
                        "columns_used": ['PRODUCT_ID', 'BRAND_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT', 'CATEGORY_LABEL', 'TAG', 'CREATED_AT'],
                        "outcome": f"Found {len(dup1)} duplicated REVIEW IDs",
                        "examples": [{"ReviewID": f"{dup1[i]}"} for i in range(min(2, len(dup1)))] if len(dup1) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate REVIEW_ID on timestamp",
                        "description": "Checking if a REVIEW_ID within a PRODUCT_ID has different timestamps, which indicates the review was processed multiple times.",
                        "columns_used": ['PRODUCT_ID','REVIEW_ID', 'CREATED_AT'],
                        "outcome": f"Found {len(dup_output_timestamp)} duplicate rows",
                        "examples": [{"ReviewID": f"{dup_output_timestamp[i]}"} for i in range(min(2, len(dup_output_timestamp)))] if len(dup_output_timestamp) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate Subcateogry sentences",
                        "description": "Checking if a subcategory sentence got duplicated within a REVIEW_ID for a product",
                        "columns_used": ['PRODUCT_ID','REVIEW_ID', 'SUBCATEGORY_SENTENCE'],
                        "outcome": f"Found {len(dup_sentences)} duplicated REVIEW IDs",
                        "examples": [{"ReviewID": f"{dup_sentences[i]}"} for i in range(min(2, len(dup_sentences)))] if len(dup_sentences) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate REVIEW_ID but with different Dates",
                        "description": "Checking if a REVIEW_ID got processed with different DATE",
                        "columns_used": ['PRODUCT_ID', 'REVIEW_ID','DATE'],
                        "outcome": f"Found {len(dup_with_created_at_output)} duplicated REVIEW IDs",
                        "examples": [{"ReviewID": f"{dup_with_created_at_output[i]}"} for i in range(min(2, len(dup_with_created_at_output)))] if len(dup_with_created_at_output) > 0 else [{"ReviewID": "None"}]
                    },
                    {
                        "type": "Duplicate REVIEW_ID but with different Product Titles",
                        "description": "Checking if a REVIEW_ID got processed with different Product Title",
                        "columns_used": ['PRODUCT_ID', 'REVIEW_ID','PRODUCT_TITLE'],
                        "outcome": f"Found {len(dup_Product_output)} duplicated REVIEW IDs",
                        "examples": [{"ReviewID": f"{dup_Product_output[i]}"} for i in range(min(2, len(dup_Product_output)))] if len(dup_Product_output) > 0 else [{"ReviewID": "None"}]
                    },
                ],
                "Subcategory-Tag Consistency Check":[
                    {
                        "type": "Validation of Unique Tag per Subcategory",
                        "description": "Checking if Subcategory and sentence linked to multiple tag within a product",
                        "columns_used": ['PRODUCT_ID', 'REVIEW_ID','SUBCATEGORY_SENTENCE', 'SUBCATEGORY_LABEL','TAG'],
                        "outcome": ["Some subcategory pairs are associated with multiple TAGs."] if has_multiple_tags else["Each subcategory pair within a product should have a distinct TAG."] ,
                        "examples": [{"REVIEW_ID": rid} for rid in df_result_tag_output['REVIEW_ID'].head(2)] if has_multiple_tags else [{"": "None"}]
                    },
                ],
                
                "Subcategory Element Validation Against Subcategory Table":[
                    {
                        "type": "Validation of Subcategory Elements in Subcategory Table",
                        "description": "Checking if the combination of SUBCATEGORY_LABEL, SENTENCE, SENTIMENT, PRODUCT_ID, and REVIEW_ID exists in the SUBCATEGORY table",
                        "columns_used": ['PRODUCT_ID', 'REVIEW_ID','SUBCATEGORY_SENTENCE', 'SUBCATEGORY_LABEL','SENTIMENT'],
                        "outcome": ["Some subcategories pairs are not present in Subcategory table."] if has_missing_subcategories else["All subcategories pairs present in Subcategory table"] ,
                    },
                ],
                "Subcategory and Category Presence in CATEGORY Table":[
                    {
                        "type": "Validation of Subcategory and Category in CATEGORY Table",
                        "description": "Checking if the subcategory_label and category_label in combination with product_id is present in category table as well",
                        "columns_used": ['PRODUCT_ID', 'CATEGORY_LABEL', 'SUBCATEGORY_LABEL'],
                        "outcome": ["Some subcategory and category pairs are missing from the CATEGORY table."] if has_missing_categories else["All subcategory and category pairs exist in the CATEGORY table."] ,
                    },
                ],
                "Subcategory and Tag Presence in TAGS Table":[
                    {
                        "type": "Validation of Subcategory-Tag Mapping in TAGS Table",
                        "description": "Checking if the subcategory_label, sentence and tag in combination with product_id is present in tags table as well",
                        "columns_used": ['PRODUCT_ID', 'SUBCATEGORY_SENTENCE', 'SUBCATEGORY_LABEL','TAG'],
                        "outcome": ["Some subcategory and tag pairs are missing from the TAG table."] if has_missing_tags else["All subcategory and tag pairs exist in the TAG table."] ,
                    },
                ],
                "NULL CHECK ANALYSIS":[
                    {
                        "type": "Null checks",
                        "description": "Checking Null values on all columns excluding REVIEW_TITLE and PRODUCT_TITLE",
                        "columns_used": ['UNIQUE_ID','BRAND_ID','REVIEW_SOURCE_ID','REVIEW_ID','REVIEW_TEXT','DATE','SUBCATEGORY_SENTENCE','PRODUCT_ID', 'SUBCATEGORY_LABEL', 'CATERGORY_LABEL','SENTIMENT','TAG','CREATED_AT'],
                        "outcome": [{"COLUMN": col} for col in null_columns_output] if null_columns_output else [{"All columns are": "NON NULL"}],
                        "examples": [{"ReviewID": rid} for rid in review_ids_null_output[:2]] if review_ids_null_output else [{"ReviewID": "None"}]

                    }
                ],
                "EMPTY VALUE CHECK ANALYSIS":[
                    {
                        "type": "Empty value checks",
                        "description": "Checking Empty values on all columns excluding REVIEW_TITLE and PRODUCT_TITLE",
                        "columns_used": ['UNIQUE_ID','BRAND_ID','REVIEW_SOURCE_ID','REVIEW_ID','REVIEW_TEXT','DATE','SUBCATEGORY_SENTENCE','PRODUCT_ID', 'SUBCATEGORY_LABEL', 'CATERGORY_LABEL','SENTIMENT','TAG','CREATED_AT'],
                        "outcome": [{"COLUMN": col} for col in empty_columns_output] if empty_columns_output else [{"All columns are": "NON EMPTY"}],
                        "examples": [{"ReviewID": rid} for rid in review_ids_empty_output[:2]] if review_ids_empty_output else [{"ReviewID": "None"}]
                    }
                ],
                "Output Data Subset of Joined Tables Check":[
                    {
                        "type": "Validating Output as Subset of Subcategory-Category-Tag Join",
                        "description": "The data in the output table should be a subset of the records obtained by joining subcategory, category, and tag tables.",
                        "columns_used": ['BRAND_ID', 'PRODUCT_ID', 'REVIEW_SOURCE_ID', 'REVIEW_ID', 'REVIEW_TITLE', 'REVIEW_TEXT', 'DATE', 'PRODUCT_TITLE', 'CATEGORY_LABEL', 'SUBCATEGORY_LABEL', 'SUBCATEGORY_SENTENCE', 'SENTIMENT', 'TAG'],
                        "outcome": ["Is the output table a subset of the join of the subcategory, category, and tag tables :-" f"{is_subset}"] ,
                    }
                ]
            }
            
        }
        
    finally:
        conn.close()
    print_data_quality_report(result)
    
    

except Exception as e:
    logging.error(f"Error: {e}")

