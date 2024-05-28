import pandas as pd, requests
from time import sleep
from tqdm import tqdm

# вивантажити інформацію про партії з АРІ
def download_party_info():

    # таблиця зі всіма партіями (api/getpartylistmain)
    party_list = requests.get('https://politdata.nazk.gov.ua/api/getpartylistmain').json()
    party_list = pd.DataFrame(party_list['data'])
    party_list = party_list.drop(['locationFact','politPartyUnitId','locations'], axis=1)


    # таблиця зі всіма регіональними представництвами (api/getpartylistregion)
    party_region_list = requests.get('https://politdata.nazk.gov.ua/api/getpartylistregion').json()
    party_region_list = pd.DataFrame(party_region_list['data'])
    party_region_list = party_region_list.drop(['locationFact','locations'], axis=1) 

    # додати інформацію про центральні партії до регіональних філій
    party_region_list = party_region_list.merge(party_list[['name','unitId','code']]
                                                .rename({'name':'party_main_name', 'code':'party_main_EDRPOU','unitId':'party_main_unitId'}, 
                                                        axis=1), 
                                                right_on='party_main_unitId', 
                                                left_on='politPartyUnitId', 
                                                how='left')

    # перейменувати
    party_region_list = party_region_list.rename({'name':'local_org_name', 'code':'local_org_EDRPOU'},axis=1)

    return party_list, party_region_list


# завантажити всі звіти
def download_all_reports(full_update):
    
    # завантажити список всіх звітів, які є в системі
    report_list = requests.get('https://politdata.nazk.gov.ua/api/getreportslist').json()
    report_list = [X['id'] for X in report_list]

    # завантажити список всіх звітів, які вже були завантажені
    with open('data/data_for_downloader/downloaded_report_ids.txt', 'r') as f:
        downloaded_report_ids = f.read().splitlines()

    # у залежності від типу оновлення (повний чи частковий) визначити список звітів, які потрібно завантажити
    if full_update:
        reports_to_download = report_list
    else:
        reports_to_download = [x for x in report_list if x not in downloaded_report_ids]

    if len(reports_to_download) > 0:

        # download
        r_df = pd.DataFrame()
        for i in tqdm(reports_to_download):
            one_report = requests.get(f'https://politdata.nazk.gov.ua/api/getreport/{i}').json()
            
            one_report_df = pd.DataFrame(one_report)
            one_report_df['report_id'] = i
            r_df = pd.concat([r_df, one_report_df], axis=0, ignore_index=True)

            sleep(0.2)

        # Файл зі всіма завантаженими звітами
        if full_update:
            # створити новий файл зі всіма завантаженими звітами
            with open('data/data_for_downloader/downloaded_report_ids.txt', 'w') as f:
                for report_id in r_df['report_id'].unique():
                    f.write(str(report_id) + '\n')

        else:
            # додати підвантажені звіти до txt файлу
            with open('data/data_for_downloader/downloaded_report_ids.txt', 'a') as f:
                for report_id in r_df['report_id'].unique():
                    f.write(str(report_id) + '\n')

        # додати інфу про центральний офіс (який був визначений в кожному звіті)
        party_main_finder = r_df.loc[r_df.officeType=="Центральний офіс",['report_id','partyCode','partyName']].rename({'partyCode':'party_main_EDRPOU','partyName':'party_main_name'}, axis=1)

        # обʼєднати r_df та party_main_finder
        r_df = r_df.merge(party_main_finder, on='report_id', how='left')

    else:
        r_df = pd.DataFrame(columns=['date','year','types','period','website','obligate','tablets1','tablets2','isWebsite','partyCode','partyName','documentId','officeType','paymentGov','headLastName','paymentOther','reportNumber','headFirstName',
                                     'propertyMoney','quantityFirst','quantityThird','headMiddleName','propertyPapers','quantitySecond','signerFullName','paymentOtherSum','propertyNoMoney','propertyObjects','headLocationCity','propertyMovables',
                                     'contributionCosts','partyLocationCity','propertyTransport','headLocationRegion','partyLocationIndex','headLocationCountry','partyIsLocationSame','partyLocationRegion','contributionConMoney','contributionOtherCon',
                                     'partyLocationCountry','partyLocationBuilding','partyLocationDistrict','contributionOtherCosts','partyLocationCountryFact','paymentCostsPaymentReceive','paymentOtherCostsPaymentReceive','id','report_id','headLocationApt',
                                     'partyLocationApt','partyLocationAptFact','partyLocationIndexFact','partyLocationKorpusFact','partyLocationRegionFact','partyLocationBuildingFact','partyLocationDistrictFact','partyLocationStreet','headIdNumber',
                                     'headIdIssurer','isAddedToCentralOffice','partyLocationCityFact','partyLocationStreetFact','headProxyDoc','headLocationKorpus','partyLocationKorpus','headLocationAptFact','headLocationIndexFact','headLocationRegionFact',
                                     'headLocationStreetFact','headLocationCountryFact','headLocationBuildingFact','headLocationDistrictFact','party_main_EDRPOU','party_main_name'])

    return r_df




# Чистка назв партій
def party_name_cleaner(r_df, party_variable):

    r_df[party_variable] = r_df[party_variable].str.upper()

    to_delete = ['^ПОЛІТИЧНА ПАРТІЯ', '^ВСЕУКРАЇНСЬКЕ ОБ\'ЄДНАННЯ', 'ВСЕУКРАЇНСЬКЕ ПОЛІТИЧНЕ ОБ\'ЄДНАННЯ', 'ПОЛІТИЧНОЇ ПАРТІЇ',
                'ПОЛІТИЧЯНА ПАРТІЯ','СОЦІАЛЬНО-ЕКОЛОГІЧНА ПАРТІЯ', 'ВСЕУКРАЇНСЬКЕ ОБ\'ЄДНАННЯ', 'СОЦІАЛЬНО-ПОЛІТИЧНИЙ СОЮЗ',
                '«', '»', '\"']
    for words in to_delete:
        r_df[party_variable] = r_df[party_variable].str.replace(words, '', regex=True)

    # прибрати зайві пробіли
    r_df[party_variable] = r_df[party_variable].str.replace('\s+', ' ', regex=True).str.strip()

    # ручні правки
    party_renamer = {
        'РІШУЧИХ ДІЙ': 'ПАРТІЯ РІШУЧИХ ДІЙ',
        'КОНКРЕТНИХ СПРАВ':'ПАРТІЯ КОНКРЕТНИХ СПРАВ',
        'БЛОК СВІТЛИЧНОЇ РАЗОМ!':'БЛОК СВІТЛИЧНОЇ «РАЗОМ!»',
        'ПАРТІЯ ВОЛОДИМИРА БУРЯКА ЄДНАННЯ':'ПАРТІЯ ВОЛОДИМИРА БУРЯКА «ЄДНАННЯ»',
        'МИР':'ВСЕУКРАЇНСЬКЕ ОБ\'ЄДНАННЯ «МИР»',
        'ПАРТІЯ ІГОРЯ КОЛИХАЄВА НАМ ТУТ ЖИТИ!':'ПАРТІЯ ІГОРЯ КОЛИХАЄВА «НАМ ТУТ ЖИТИ!»',
        'КОМАНДА МАКСИМА ЄФІМОВА НАШ КРАМАТОРСЬК':'КОМАНДА МАКСИМА ЄФІМОВА «НАШ КРАМАТОРСЬК»',
        'ГРОМАДСЬКО-ПОЛІТИЧНИЙ РУХ ВАЛЕНТИНА НАЛИВАЙЧЕНКА СПРАВЕДЛИВІСТЬ':'ГРОМАДСЬКО-ПОЛІТИЧНИЙ РУХ ВАЛЕНТИНА НАЛИВАЙЧЕНКА «СПРАВЕДЛИВІСТЬ»',
        'МАЛОГО І СЕРЕДНЬОГО БІЗНЕСУ УКРАЇНИ':'ПАРТІЯ МАЛОГО І СЕРЕДНЬОГО БІЗНЕСУ УКРАЇНИ',
        'БЛОК ВІЛКУЛА УКРАЇНСЬКА ПЕРСПЕКТИВА':'БЛОК ВІЛКУЛА «УКРАЇНСЬКА ПЕРСПЕКТИВА»',
        'НАЦІОНАЛЬНО-ДЕМОКРАТИЧНЕ ОБ\'ЄДНАННЯ УКРАЇНА':'НАЦІОНАЛЬНО-ДЕМОКРАТИЧНЕ ОБ\'ЄДНАННЯ «УКРАЇНА»',
        'ГРОМАДЯНСЬКИЙ РУХ СВІДОМІ':'ГРОМАДЯНСЬКИЙ РУХ «СВІДОМІ»',
        'ВО ПЛАТФОРМА ГРОМАД':'ПЛАТФОРМА ГРОМАД',
        'ГРОМАДЯНСЬКАПЛАТФОРМА':'ГРОМАДЯНСЬКА ПЛАТФОРМА'
    }

    r_df[party_variable] = r_df[party_variable].replace(party_renamer)

    return r_df

# уніфікація party_main_name (беремо з останнього звіту за EDRPOU)
def unify_party_main_name(r_df):

    # уніфікувати назву центрального офісу
    party_renamer = r_df[['date','party_main_EDRPOU','party_main_name']]
    party_renamer = party_renamer.sort_values(['party_main_EDRPOU','date'], ascending=True)
    party_renamer = party_renamer.drop_duplicates(['party_main_EDRPOU'], keep = 'last')
    party_renamer = party_renamer.set_index('party_main_EDRPOU').to_dict()['party_main_name']
    r_df['party_main_name'] = r_df['party_main_EDRPOU'].replace(party_renamer)

    # уніфікувати назву регіонального офісу
    party_renamer = r_df[['date','partyCode','partyName']]
    party_renamer = party_renamer.sort_values(['partyCode','date'], ascending=True)
    party_renamer = party_renamer.drop_duplicates(['partyCode'], keep = 'last')
    party_renamer = party_renamer.set_index('partyCode').to_dict()['partyName']
    r_df['partyName'] = r_df['partyCode'].replace(party_renamer)

    return r_df

# Якщо серед донорів зустрічаються партійні осередки чи центральний офіс політичної партії 'donor_type' замінити на “партійний осередок” (визначила по 'donor_edrpou')
def check_edrpou_for_party(table, var_edrpou_to_search, var_to_replace, party_list, party_region_list):
    table.loc[(table[var_edrpou_to_search].isin(party_list.code.tolist()))|
              (table[var_edrpou_to_search].isin(party_region_list.local_org_EDRPOU.tolist())), 
              var_to_replace] = 'Партійний осередок'
    return table



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
    t = t.apply(lambda x: x.apply(replace_stars))

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
            if len(excel_df) > 0:
                new_table = pd.concat([excel_df, table], axis=0, ignore_index=True)
                new_table.to_excel(path, index=False, engine='xlsxwriter')
            else:
                table.to_excel(path, index=False, engine='xlsxwriter')
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
    'АКЦІОНЕРНЕ ТОВАРИСТВО "РАЙФФАЙЗЕН БАНК"': 'АКЦІОНЕРНЕ ТОВАРИСТВО "РАЙФФАЙЗЕН БАНК АВАЛЬ"'
    }


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
