import pandas as pd


def subset_table(df, main_var, cols_to_select):
    # subset
    table = df[[main_var] + cols_to_select]
    
    # видалити пусті
    table = table[table[main_var].apply(lambda x: len(x)>0)]
    
    return table


# замінити *** (деперсоналізовані дані) на None
def replace_stars(cell):
    if isinstance(cell, str) and (set(cell) == {'*'} or set(cell) == {'*','_'}):
        return None
    else:
        return cell
    

# основна функція розпаковки змінної зі списком в індивідуальні рядки
def list_to_rows(table, main_var, cols_to_select, renamer):

    t = pd.DataFrame()

    for i in table.index:
        new_df = pd.DataFrame(table.loc[i, main_var])
        for n in cols_to_select:
            new_df[n] = table.loc[i,n]
        t = pd.concat([t, new_df], axis=0)

    # перейменувати
    t = t.rename(renamer, axis=1)
    t = t.reset_index(drop=True)
    
    # замінити ***
    t = t.transform(replace_stars)

    return t


# зберегти як excel
def save_as_excel(table, filename, full_update):
    path = f'data/excel_tables/{filename}.xlsx'
    
    if full_update:
        table.to_excel(path, index=False, engine='xlsxwriter')
    
    else:
        # open and save in excel_df
        excel_df = pd.read_excel(path)
        if len(table) > 0:
            new_table = pd.concat([excel_df, table], axis=0, ignore_index=True)
            new_table.to_excel(path, index=False, engine='xlsxwriter')
        else:
            excel_df.to_excel(path, index=False, engine='xlsxwriter')



#### Data cleaning ####

# почистити IBAN
def clean_bank_account(bank_account_column):
    clean_bank_account_column = (bank_account_column.str.replace('№','', regex=True)
                                .str.replace(' ','', regex=True)
                                .str.replace(':','', regex=True)
                                .str.replace('\n','', regex=True)
                                .str.strip())
    return clean_bank_account_column



org_names_to_rename = {
 'ТОВ ПІДПРИЄМСТВО КИЇВ': 'ТОВ ПІДПРИЄМСТВО "КИЇВ"',
 'ТОВ "ПІДПРИЄМСТВО "КИЇВ"': 'ТОВ ПІДПРИЄМСТВО "КИЇВ"',
 'ПІДПРИЄМСТВО "КИЇВ" ТОВ': 'ТОВ ПІДПРИЄМСТВО "КИЇВ"',
 'ТОВ "ПІДПРИЄМСТВО КИЇВ"': 'ТОВ ПІДПРИЄМСТВО "КИЇВ"',
 'ПІДПРИЄМСТВО "КИЇВ"': 'ТОВ ПІДПРИЄМСТВО "КИЇВ"',
 'АКЦІОНЕРНЕ ТОВАРИСТВО "РАЙФФАЙЗЕН БАНК"': 'АКЦІОНЕРНЕ ТОВАРИСТВО "РАЙФФАЙЗЕН БАНК АВАЛЬ"'}



def org_name_clean(t, var_name):

    # manual replacers
    t[var_name] = t[var_name].replace(org_names_to_rename)

    # replace 'ФІЗИЧНА ОСОБА ПІДПРИЄМЕЦЬ' with 'ФОП' inside each cell

    fop_replacers = ['ФІЗИЧНА ОСОБА ПІДРИЄМЕЦЬ', 'ФІЗИЧНА ОСОБА ПІДПРИЄМЕЦЬ','ФІЗИЧНА ОСОБА ПІДПРИЄМЕЦЬ',
                    'ФІЗИЧНА ОСОБА- ПІДПРИЄМЕЦЬ','ФІЗИЧНА ОСОБА-ПІДПРИЄМЕЦЬ','ФІЗИЧНА ОСОБА-ПІДРИЄМЕЦЬ','ФІЗИЧНА ОСОБА - ПІДПРИЄМЕЦЬ',
                    'ФІЗИЧНА ОСОБА -ПІДПРИЄМЕЦЬ','ФІЗИЧНА ОБОБА-ПІДПРИЄМЕЦЬ','ФІЗИЧНА ОСОБА-ПІДПРИМЕЦЬ']
    for replacer in fop_replacers:
        t[var_name] = t[var_name].str.replace(replacer,'ФОП', regex=True)

    # Long to short form of ТОВ
    tov_replacers = ["ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ","ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПРОВІДАЛЬНІСТЮ"]
    for replacer in tov_replacers:
        t[var_name] = t[var_name].str.replace(replacer,'ТОВ', regex=True)

    t[var_name] = t[var_name].str.replace("ПРИВАТНЕ АКЦІОНЕРНЕ ТОВАРИСТВО",'ПрАТ', regex=True)
    
    for replacer in ["ПУБЛІЧНЕ АКЦІОНЕРНЕ ТОВАРИСТВО", 'ПУБЛІЧНЕ АКЦІОНЕРНЕ ТОВАРИСТВО']:
        t[var_name] = t[var_name].str.replace(replacer,'ПАТ', regex=True)

    for replacer in ["ДЕРЖАВНОЇ ПОДАТКОВОЇ СЛУЖБИ", 'ДЕРЖАВНА ПОДАТКОВА СЛУЖБА']:
        t[var_name] = t[var_name].str.replace(replacer,'ДПС', regex=True)
    

    t[var_name] = t[var_name].str.replace('ПОЛІТИЧНА ПАРТІЯ','ПП', regex=True)

    for replacer in ["АКЦІОНЕРНЕ ТОВАРИВСТВО",'АКЦІОНЕРНЕ ТОВАРИСТВО']:
        t[var_name] = t[var_name].str.replace(replacer,'АТ', regex=True)    

    # delete 'ФОП' at the end of the string and put at the start
    for i in t.index:
        if type(t.loc[i,var_name]) == str: 
            if t.loc[i,var_name].startswith('ФОП'):
                t.loc[i,var_name] = t.loc[i,var_name].replace('ФОП ','').strip() + ' ФОП'
