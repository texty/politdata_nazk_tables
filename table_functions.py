import pandas as pd, os
from main_functions import subset_table, list_to_rows, save_as_excel, check_edrpou_for_party, clean_bank_account, org_name_clean
from data.data_for_downloader.renamers import renamer_1, renamer_2, renamer_3, renamer_4, renamer_5, renamer_6, renamer_7, renamer_8, renamer_9, renamer_10, renamer_11, renamer_12, renamer_13, renamer_14, renamer_15, renamer_16, renamer_17, renamer_18, renamer_19, renamer_20


### Таблиця 0.1: Перевірка на наявність дублів
def table_0_1(r_df, full_update):
    # create report about duplicates
    to_subset = ['period','year','partyName','partyCode','report_id']
    duplicates = r_df[r_df.duplicated(subset=to_subset,keep=False)]

    if len(duplicates) > 0:
        save_as_excel(duplicates, '0_report_duplcates', full_update)



### Таблиця 1: загальна інформація про партію/осередок

def table_1(r_df, full_update):
    filename = '1_legal_entity_report_info'

    cols_to_select = [x for x in renamer_1.keys() if x in r_df.columns]
    table = r_df[cols_to_select].rename(renamer_1, axis=1)

    # add None if empty column
    for v in renamer_1.values():
        if v not in table.columns:
            table[v] = None

    table = table[list(renamer_1.values())]

    save_as_excel(table, filename, full_update)


### Таблиця 0.2: Інформація про те, за який період кожна партія подала звіти
def table_0_2():
    
    table = pd.read_excel('data/excel_tables/1_legal_entity_report_info.xlsx')

    table_subset = table[['legal_entity_name','legal_entity_edrpou','officeType','party_main_name','party_main_EDRPOU','report_id','report_period','report_year']]
    table_subset['reported_period'] = table_subset['report_year'].copy().astype(str) + ', ' + table_subset['report_period'].copy()
    table_subset = table_subset.drop(['report_period','report_year'], axis=1).drop_duplicates()

    # якщо для 1 періоду є декілька report_id - об'єднати через ;  
    table_subset = table_subset.groupby(['legal_entity_name','legal_entity_edrpou','officeType','party_main_name','party_main_EDRPOU','reported_period'], as_index=False).agg({'report_id': lambda x: '; '.join(x)})

    table_subset = table_subset.pivot(index=['legal_entity_name','legal_entity_edrpou','officeType','party_main_name','party_main_EDRPOU'], 
                                    values='report_id',columns='reported_period',).reset_index().sort_values('party_main_name').fillna('')
    
    table_subset.to_excel('data/excel_tables/0_reports_per_period_per_party.xlsx', index=False, engine='xlsxwriter')




### Таблиця 2: інформація про осередки політичної партії  

def table_2_1(r_df, full_update):
    main_var = 'tablets1'

    cols_to_select = ['partyLocationRegion','period', 'year', 'party_main_name','party_main_EDRPOU','report_id', 'officeType']

    filename = '2.1_local_orgs_info'
    table = subset_table(r_df, main_var, cols_to_select)

    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_2)
        t = t[list(renamer_2.values())+['party_main_name','party_main_EDRPOU','report_id','officeType']]

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'
        # і потім видалити 'officeType'
        t = t.drop('officeType', axis=1)

        save_as_excel(t, filename, full_update)




### Таблиця 3: інформація про інші підприємства, установи та організації створені партією політичної партії

def table_2_2(r_df, full_update):
    cols_to_select = ['period', 'year', 'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'tablets2'

    filename = '2.2_other_party_orgs_info'
    table = subset_table(r_df, main_var, cols_to_select)

    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_3)
        t = t[list(renamer_3.values())+['party_main_name','party_main_EDRPOU','report_id']]
        
        # delete empty values
        t = t[(t.other_party_org_name != '0')|(t.other_party_org_EDRPOU != '00000000')]

        save_as_excel(t, filename, full_update)





### Таблиця 4: зведена інформація про нерухоме майно політичної партії (propertyObjects)
def table_3_1(r_df, full_update):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode','partyLocationRegion', 
                  'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'propertyObjects'

    filename = '3.1_property_objects'
    table = subset_table(r_df, main_var, cols_to_select)

    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_4)
        t = t[list(renamer_4.values())+['party_main_name','party_main_EDRPOU','report_id']]

        # delete empty
        t = t[t.object_type.notna()]

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # clean t['object_owner_name']

        if len(t.object_owner_name.unique()) > 1:

            t['object_owner_name'] = t['object_owner_name'].str.upper()
            t['object_owner_name'] = t['object_owner_name'].str.replace('\s+',' ', regex=True).str.strip()

            # уніфікувати ФОП, ТОВ і ПАТ
            org_name_clean(t, 'object_owner_name')

            # уніфікувати апострофи в іменах
            t['object_owner_name'] = t['object_owner_name'].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

            # replace ocassional english letters
            t.loc[t['object_owner_name'].str.contains('[А-Я]', na=False),'object_owner_name'] = t.loc[t['object_owner_name'].str.contains('[А-Я]', na=False),'object_owner_name'].str.replace('C','С').str.replace('I','І')

            save_as_excel(t, filename, full_update)





### Таблиця 5: зведена інформація про рухоме майно політичної партії (propertyMovables)
def table_3_2(r_df, full_update):

    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion', 
                    'party_main_name','party_main_EDRPOU','report_id']
    main_var = 'propertyMovables'
    filename = '3.2_movable_property'
    table = subset_table(r_df, main_var, cols_to_select)

    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_5)
        
        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # sort columns
        sorter = [x for x in list(renamer_5.values())+['party_main_name','party_main_EDRPOU','report_id']  if x in t.columns]
        t = t[sorter]

        # clean t['object_owner_name']
        var_to_clean = 'object_owner'

        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)




### Таблиця 6: зведена інформація про автотранспорт політичної партії (propertyTransport)

def table_3_3(r_df, full_update, party_list, party_region_list):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion', 
                    'party_main_name','party_main_EDRPOU','report_id']
    main_var = 'propertyTransport'
    filename = '3.3_vehicles'
    table = subset_table(r_df, main_var, cols_to_select)

    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_6)

        # sort columns
        sorter = [x for x in list(renamer_6.values())+['party_main_name','party_main_EDRPOU','report_id']  if x in t.columns]
        t = t[sorter]

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # delete empty
        t = t[(t.object_type.notna())&(t.object_brand.notna())]

        # Якщо object_owner_edrpou це код партії чи осередку то у object_owner_type ставити “Партійний осередок”
        t = check_edrpou_for_party(t, 'object_owner_edrpou', 'object_owner_type', party_list, party_region_list)

        # clean t['object_owner']
        var_to_clean = 'object_owner'
        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)




### Таблиця 7: зведена інформація про цінні папери політичної партії (propertyPapers)
def table_3_4(r_df, full_update):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion',
                  'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'propertyPapers'

    filename = '3.4_securities'
    table = subset_table(r_df, main_var, cols_to_select)

    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_7)

        if len(t) > 0:
            # sort columns
            sorter = [x for x in list(renamer_7.values())+['party_main_name','party_main_EDRPOU','report_id']  if x in t.columns]
            t = t[sorter]

            # Для центральних офісів політичних партій legal_entity_region –> “Україна”
            t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

            # save
            save_as_excel(t, filename, full_update)


        elif len(t) == 0:
            # create columns if t is empty
            t = pd.DataFrame(columns=list(renamer_7.values())+['party_main_name','party_main_EDRPOU','report_id'])
            save_as_excel(t, filename, full_update)

    else:
        # create columns if table is empty
        t = pd.DataFrame(columns=list(renamer_7.values())+['party_main_name','party_main_EDRPOU','report_id'])
        save_as_excel(t, filename, full_update)
        
    
    
### Таблиця 8: зведена інформація про інше майно та нематеріальні активи політичної партії (propertyNoMoney)
def table_3_5(r_df, full_update):

    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion', 
                    'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'propertyNoMoney'

    filename = '3.5_intangible_assets'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_8)

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        save_as_excel(t, filename, full_update)


### Таблиця 9: зведена інформація про банківські рахунки політичної партії (propertyMoney)
def table_4(r_df, full_update):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion', 
                    'party_main_name','party_main_EDRPOU','report_id']
    main_var = 'propertyMoney'
    filename = '4_bank_accounts'

    table = subset_table(r_df, main_var, cols_to_select)

    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_9)

        t['account_number'] = clean_bank_account(t['account_number'])

        # check if any are longer than 29
        t['account_number'].apply(lambda x: len(x)).value_counts()

        # t[t.account_number.apply(lambda x: len(x)>29)]

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        save_as_excel(t, filename, full_update)

### Таблиця 10: зведена інформація про приватні грошові внески на рахунки політичної партії (contributionConMoney)  
def table_5(r_df, full_update, party_list, party_region_list):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'party_main_name','party_main_EDRPOU',
                    'partyLocationRegion','report_id']

    main_var = 'contributionConMoney'

    filename = '5_private_contributions'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_10)

        # delete empty
        t = t[(t.donor_name.notna())&(t.bank_edrpou.notna())]

        # clean bank_account
        t['bank_account'] = clean_bank_account(t['bank_account'])

        # Якщо серед донорів зустрічаються партійні осередки чи центральний офіс політичної партії 'donor_type' замінити на “партійний осередок” (визначила по 'donor_edrpou')
        t = check_edrpou_for_party(t, 'donor_edrpou', 'donor_type', party_list, party_region_list)

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # sort columns
        t = t[list(renamer_10.values())+['party_main_name','party_main_EDRPOU','report_id']]

        # clean t['donor_name']
        var_to_clean = 'donor_name'

        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)



### Таблиця 11: зведена інформація про негрошові внески на користь політичної партії (contributionOtherCon)
def table_6(r_df, full_update, party_list, party_region_list):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'party_main_name','party_main_EDRPOU','partyLocationRegion','report_id']

    main_var = 'contributionOtherCon'

    filename = '6_in_kind_donations'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_11)

        # delete empty
        t = t[(t.donor_name.notna())&(t.donor_type.notna())]

        # Якщо серед донорів зустрічаються партійні осередки чи центральний офіс політичної партії 'donor_type' замінила на “партійний осередок” (визначила по 'donor_edrpou')

        t.loc[(t.donor_edrpou.isin(party_list.code.tolist()))|
            (t.donor_edrpou.isin(party_region_list.local_org_EDRPOU.tolist())), 
            'donor_type'] = 'Партійний осередок'

        # donor_edrpou delete *
        t.loc[t.donor_edrpou.str.contains('\*',na=False),'donor_edrpou'] = None
        t.loc[t.donor_birth_date.str.contains('\*',na=False),'donor_birth_date'] = None
        t.loc[t.object_registration_number.str.contains('\*',na=False),'object_registration_number'] = None

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”

        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # clean t['donor_name']
        var_to_clean = 'donor_name'

        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)



### Таблиця 12: зведена інформація про кошти державного фінансування виплачені політичним партіям (contributionCosts)
def table_7(r_df, full_update):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'contributionCosts'

    filename = '7_state_funding_transactions'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_12)

        # convert refund_sum to numeric
        t['transaction_sum'] = t['transaction_sum'].astype(int)

        t['bank_account'] = clean_bank_account(t['bank_account'])

        # sort columns
        t = t[list(renamer_12.values()) + ['party_main_name','party_main_EDRPOU','report_id']]

        save_as_excel(t, filename, full_update)



### Таблиця 13: зведена інформація про інші грошові надходження на рахунки політичних партій (contributionOtherCosts)
def table_8(r_df, full_update):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion',
                    'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'contributionOtherCosts'

    filename = '8_other_income'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_13)
        t['bank_account'] = clean_bank_account(t['bank_account'])

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # clean t['sender_name']
        var_to_clean = 'sender_name'

        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)



### Таблиця 14: зведена інформація про витрати державного фінансування політичних партій (paymentGov)
def table_9_1(r_df, full_update, party_list, party_region_list):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'party_main_name','party_main_EDRPOU','partyLocationRegion','report_id']

    main_var = 'paymentGov'

    filename = '9.1_expenditures_public_funding'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_14)

        t['account_number'] = clean_bank_account(t['account_number'])

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # Якщо отримувач коштів є партійною структурою (recipient_EDRPOU), у recipient_type замінити позначку “Юридична особа/Фізична особа” на Партійний осередок.
        t = check_edrpou_for_party(t, 'recipient_EDRPOU', 'recipient_type', party_list, party_region_list)

        # clean t['recipient_name']
        var_to_clean = 'recipient_name'
        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)



### Таблиця 15: зведена інформація про витрати з рахунків з приватним фінансуванням (paymentOther)
def table_9_2(r_df, full_update, party_list, party_region_list):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'party_main_name','party_main_EDRPOU','partyLocationRegion','report_id']

    main_var = 'paymentOther'

    filename = '9.2_expenditures_private_funds'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_15)

        t['account_number'] = clean_bank_account(t['account_number'])

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # Якщо отримувач коштів є партійною структурою (recipient_EDRPOU), у recipient_type замінити позначку “Юридична особа/Фізична особа” на Партійний осередок.
        t = check_edrpou_for_party(t, 'recipient_EDRPOU', 'recipient_type', party_list, party_region_list)

        # delete empty
        t = t[(t.bank_EDRPOU.notna())&(t.account_number.notna())]

        # clean t['recipient_name']
        var_to_clean = 'recipient_name'
        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)


### Таблиця 16: зведена інформація про отримання помилкових надходжень (paymentCostsPaymentReceive)
def table_9_3(r_df, full_update):

    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion',
                    'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'paymentCostsPaymentReceive'

    filename = '9.3_false_donations_info'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:

        t = list_to_rows(table, main_var, cols_to_select, renamer_16)

        t['account_number'] = clean_bank_account(t['account_number'])

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # clean t['donor_name']
        var_to_clean = 'donor_name'
        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)



### НЕ ІСНУЄ Таблиця 17: інформація про повернення внесків та помилкових надходжень (paymentCostsPaymentReturn)
# cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'party_main_name','party_main_EDRPOU','report_id']
# main_var = 'paymentCostsPaymentReturn'
# filename = '9.4_false_donations_returning'
# main_var in r_df.columns


### Таблиця 18: інформація про отримання помилкових інших (негрошових) внесків (paymentOtherCostsPaymentReceive)
def table_9_5(r_df, full_update):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion', 'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'paymentOtherCostsPaymentReceive'

    filename = '9.5_false_in_kind_donations_info'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_18)

        if len(t) == 0:
            # create columns if t is empty
            t = pd.DataFrame(columns=list(renamer_18.values())+['party_main_name','party_main_EDRPOU','report_id'])
            save_as_excel(t, filename, full_update)

        else:
            # sort columns
            sorter = [x for x in list(renamer_18.values())+['party_main_name','party_main_EDRPOU','report_id']  if x in t.columns]
            t = t[sorter]

            # Для центральних офісів політичних партій legal_entity_region –> “Україна”
            t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

            # clean t['recipient_name']

            var_to_clean = 'onor_name'

            t[var_to_clean] = t[var_to_clean].str.upper()
            t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

            # уніфікувати ФОП, ТОВ і ПАТ
            org_name_clean(t, var_to_clean)

            # уніфікувати апострофи в іменах
            t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

            # replace ocassional english letters
            t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

            # save
            save_as_excel(t, filename, full_update)

    else:
        # create t
        t = pd.DataFrame(columns=list(renamer_18.values())+['party_main_name','party_main_EDRPOU','report_id'])
        save_as_excel(t, filename, full_update)
        


### НЕ ІСНУЄ Таблиця 19: інформація про отримання помилкових інших (негрошових) внесків (paymentOtherCostsPaymentReturn)
# cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'party_main_name','party_main_EDRPOU','report_id']
# main_var = 'paymentOtherCostsPaymentReturn'
# filename = '9.5_false_in_kind_donations_returning'
# main_var in r_df.columns



### Таблиця 20: інформація про фінансові зобов'язання (obligate)
def table_10(r_df, full_update):
    cols_to_select = ['officeType','types','period', 'year','partyName','partyCode', 'partyLocationRegion',
                    'party_main_name','party_main_EDRPOU','report_id']

    main_var = 'obligate'

    filename = '10_liabilities'
    table = subset_table(r_df, main_var, cols_to_select)
    if len(table) > 0:
        t = list_to_rows(table, main_var, cols_to_select, renamer_20)

        # Для центральних офісів політичних партій legal_entity_region –> “Україна”
        t.loc[t.officeType == 'Центральний офіс', 'legal_entity_region'] = 'Україна'

        # delete empty
        t = t[t.name.notnull()]

        # clean t['name']
        var_to_clean = 'name'
        t[var_to_clean] = t[var_to_clean].str.upper()
        t[var_to_clean] = t[var_to_clean].str.replace('\s+',' ', regex=True).str.strip()

        # уніфікувати ФОП, ТОВ і ПАТ
        org_name_clean(t, var_to_clean)

        # уніфікувати апострофи в іменах
        t[var_to_clean] = t[var_to_clean].replace(r"(?<=\w)[’\"\`](?=\w)", "\'", regex=True)

        # replace ocassional english letters
        t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean] = t.loc[t[var_to_clean].str.contains('[А-Я]', na=False),var_to_clean].str.replace('C','С').str.replace('I','І')

        save_as_excel(t, filename, full_update)




## Перевірити в яких файлах є які партії
def files_where_to_look_for_local_parties():
    # завантажити основу таблиці
    df = pd.read_excel('data/excel_tables/1_legal_entity_report_info.xlsx')
    df = df[['legal_entity_name','legal_entity_edrpou','officeType','party_main_name','party_main_EDRPOU']]
    df = df.drop_duplicates()
    
    # створити список файлів які перевіряти
    excel_files = os.listdir('data/excel_tables')
    excel_files = [x for x in excel_files 
                if x.endswith('.xlsx') and x != '2.2_other_party_orgs_info.xlsx' 
                and not x.startswith('0_') and not x.startswith('1')]
    excel_files = sorted(excel_files)

    excel_files.append('10_liabilities.xlsx')
    
    # перевірити кожен файл
    for filename in excel_files:
        d = pd.read_excel('data/excel_tables/' + filename)

        if 'legal_entity_edrpou' in d.columns:
            df[filename] = df['legal_entity_edrpou'].isin(d['legal_entity_edrpou']).astype(int)
        elif 'local_org_EDRPOU' in d.columns:
            df[filename] = df['legal_entity_edrpou'].isin(d['local_org_EDRPOU']).astype(int)
            
    df.to_excel('data/excel_tables/0_files_where_to_look_for_local_parties.xlsx', index=False, engine='xlsxwriter')